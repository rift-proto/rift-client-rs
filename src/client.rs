use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::SinkExt;
use tokio::sync::{Mutex, RwLock, broadcast, oneshot};
use tokio_tungstenite::tungstenite::Message as WsMessage;

use rifts::ack::AckStatus;
use rifts::frame::{Frame, Priority};
use rifts::message::SubscribeMode;
use rifts::message::command::Reply;
use rifts::transport::frame_codec::encode_frame;

use crate::config::RiftClientConfig;
use crate::connection::ConnectionInner;
use crate::error::{ClientError, Result};
use crate::events::ClientEvent;
use crate::frame_builder::{self, FrameIdCounter};
use crate::subscriber::SubscriptionTracker;

/// Async Rift realtime client.
///
/// Create with [`RiftClient::new`], connect with [`RiftClient::connect`],
/// then use the publish/subscribe methods to interact with topics.
///
/// Obtain a stream of incoming events via [`RiftClient::subscribe_events`].
pub struct RiftClient {
    url: String,
    config: Arc<RwLock<RiftClientConfig>>,
    inner: RwLock<Option<Arc<ConnectionInner>>>,
    event_tx: broadcast::Sender<ClientEvent>,
    subscriptions: Arc<Mutex<SubscriptionTracker>>,
    closed: Arc<AtomicBool>,
    frame_ids: FrameIdCounter,
}

/// Options for [`RiftClient::publish`].
#[derive(Debug, Default, Clone)]
pub struct PublishOpts {
    /// Optional deduplication key.
    pub dedupe_key: Option<String>,
    /// Optional ordering key for partition ordering.
    pub ordering_key: Option<String>,
    /// Time-to-live in milliseconds.
    pub ttl_ms: Option<u32>,
    /// Message priority.
    pub priority: Option<Priority>,
}

/// Options for [`RiftClient::command`].
#[derive(Debug, Default, Clone)]
pub struct CommandOpts {
    /// Command timeout in milliseconds. Defaults to 5000.
    pub timeout_ms: Option<u64>,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
    /// Message priority.
    pub priority: Option<Priority>,
}

/// Options for [`RiftClient::publish_state`].
#[derive(Debug, Default, Clone)]
pub struct StateOpts {
    /// Optional state name.
    pub name: Option<String>,
    /// Time-to-live in milliseconds.
    pub ttl_ms: Option<u32>,
    /// Optional subject identifier.
    pub subject: Option<String>,
}

impl RiftClient {
    /// Create a new client. Does **not** connect yet.
    pub fn new(url: impl Into<String>, config: RiftClientConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            url: url.into(),
            config: Arc::new(RwLock::new(config)),
            inner: RwLock::new(None),
            event_tx,
            subscriptions: Arc::new(Mutex::new(SubscriptionTracker::new())),
            closed: Arc::new(AtomicBool::new(false)),
            frame_ids: FrameIdCounter::new(),
        }
    }

    // ── Connection lifecycle ─────────────────────────────────────────────

    /// Connect to the Rift server, perform the Hello/Welcome/Ready handshake,
    /// and start the heartbeat loop. Returns after the Ready frame is received.
    pub async fn connect(&self) -> Result<()> {
        if self.inner.read().await.is_some() {
            return Err(ClientError::AlreadyConnected);
        }
        self.closed.store(false, Ordering::SeqCst);

        let inner = crate::connection::connect(
            &self.url,
            &self.config,
            &self.event_tx,
            Arc::clone(&self.subscriptions),
            self.frame_ids.clone(),
        )
        .await?;

        *self.inner.write().await = Some(inner);

        // Spawn reconnect monitor — captures the inner Arc so it gets
        // dropped when disconnect_notify fires, releasing the connection.
        let guard = self.inner.read().await;
        let conn = guard.as_ref().ok_or(ClientError::NotConnected)?;
        let disconnect_notify = conn.disconnect_notify.clone();
        let inner_arc = conn.clone();
        drop(guard);
        let event_tx = self.event_tx.clone();
        let config = Arc::clone(&self.config);
        let subscriptions = Arc::clone(&self.subscriptions);
        let closed = Arc::clone(&self.closed);
        let url = self.url.clone();
        let frame_ids = self.frame_ids.clone();

        tokio::spawn(async move {
            disconnect_notify.notified().await;
            // Dropping inner_arc releases the connection resources
            drop(inner_arc);
            // Emit disconnect
            let _ = event_tx.send(ClientEvent::Disconnected {
                code: 1006,
                reason: "connection lost".into(),
            });
            // Auto-reconnect
            if !closed.load(Ordering::SeqCst) {
                let cfg = config.read().await;
                let auto = cfg.auto_reconnect;
                let max_attempts = cfg.max_reconnect_attempts;
                let base_delay = cfg.reconnect_delay;
                drop(cfg);

                if auto {
                    try_reconnect(
                        &url,
                        &config,
                        &event_tx,
                        subscriptions,
                        closed,
                        frame_ids,
                        max_attempts,
                        base_delay,
                    )
                    .await;
                }
            }
        });

        Ok(())
    }

    /// Gracefully close the connection and stop auto-reconnect.
    pub async fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        let mut guard = self.inner.write().await;
        if let Some(inner) = guard.take() {
            let mut writer = inner.writer.lock().await;
            let _ = writer.send(WsMessage::Close(None)).await;
        }
        Ok(())
    }

    /// Returns `true` if connected and the handshake is complete.
    pub async fn is_connected(&self) -> bool {
        self.inner.read().await.is_some()
    }

    /// Returns the current session ID, or `None` if not connected.
    pub async fn session_id(&self) -> Option<String> {
        self.inner
            .read()
            .await
            .as_ref()
            .map(|i| i.session_id.clone())
    }

    /// Returns the current epoch.
    pub async fn epoch(&self) -> u32 {
        self.inner
            .read()
            .await
            .as_ref()
            .map(|i| i.epoch)
            .unwrap_or(1)
    }

    // ── Event subscription ───────────────────────────────────────────────

    /// Obtain a broadcast receiver for [`ClientEvent`]s emitted by this client.
    pub fn subscribe_events(&self) -> broadcast::Receiver<ClientEvent> {
        self.event_tx.subscribe()
    }

    // ── Subscribe / Unsubscribe ──────────────────────────────────────────

    /// Subscribe to a topic. The subscription is tracked for auto-resubscribe
    /// on reconnect.
    pub async fn subscribe(
        &self,
        topic: &str,
        mode: SubscribeMode,
        from_offset: Option<i64>,
    ) -> Result<()> {
        self.require_connected().await?;
        {
            let mut subs = self.subscriptions.lock().await;
            subs.add(topic, mode);
        }
        let frame = frame_builder::subscribe_frame(
            self.frame_ids.next(),
            topic,
            mode_str(mode),
            from_offset,
        );
        self.send_frame(frame).await
    }

    /// Unsubscribe from a topic.
    pub async fn unsubscribe(&self, topic: &str) -> Result<()> {
        self.require_connected().await?;
        {
            let mut subs = self.subscriptions.lock().await;
            subs.remove(topic);
        }
        let frame = frame_builder::unsubscribe_frame(self.frame_ids.next(), topic);
        self.send_frame(frame).await
    }

    // ── Publish ──────────────────────────────────────────────────────────

    /// Publish an event to a topic.
    pub async fn publish(
        &self,
        topic: &str,
        event: &str,
        schema: &str,
        payload: serde_json::Value,
        opts: Option<PublishOpts>,
    ) -> Result<()> {
        self.require_connected().await?;
        let opts = opts.unwrap_or_default();
        let message_id = ulid::Ulid::new().to_string();
        let frame = frame_builder::event_frame(
            self.frame_ids.next(),
            topic,
            event,
            &message_id,
            schema,
            payload,
            opts.dedupe_key.as_deref(),
            opts.ordering_key.as_deref(),
            opts.ttl_ms,
            opts.priority,
        );
        self.send_frame(frame).await
    }

    /// Send a command and await the reply. Times out after `timeout_ms`
    /// (default 5 000 ms).
    pub async fn command(
        &self,
        topic: &str,
        cmd: &str,
        schema: &str,
        payload: serde_json::Value,
        opts: Option<CommandOpts>,
    ) -> Result<Reply> {
        self.require_connected().await?;
        let opts = opts.unwrap_or_default();
        let timeout_ms = opts.timeout_ms.unwrap_or(5_000);
        let correlation_id = uuid::Uuid::now_v7().to_string();

        let frame = frame_builder::command_frame(
            self.frame_ids.next(),
            topic,
            cmd,
            &correlation_id,
            timeout_ms,
            schema,
            payload,
            opts.idempotency_key.as_deref(),
            opts.priority,
        );

        // Register pending reply
        let (tx, rx) = oneshot::channel::<Reply>();
        {
            let inner = self.inner.read().await;
            let conn = inner.as_ref().ok_or(ClientError::NotConnected)?;
            let mut pending = conn.pending_replies.lock().await;
            pending.insert(correlation_id.clone(), tx);
        }

        // Send the command frame
        self.send_frame(frame).await?;

        // Await reply or timeout
        match tokio::time::timeout(Duration::from_millis(timeout_ms), rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_)) => {
                // sender dropped — disconnect occurred
                Err(ClientError::NotConnected)
            }
            Err(_) => {
                // Clean up the pending entry
                let inner = self.inner.read().await;
                if let Some(conn) = inner.as_ref() {
                    conn.pending_replies.lock().await.remove(&correlation_id);
                }
                Err(ClientError::CommandTimeout(timeout_ms))
            }
        }
    }

    /// Publish a state message to a topic.
    pub async fn publish_state(
        &self,
        topic: &str,
        state_key: &str,
        value: serde_json::Value,
        opts: Option<StateOpts>,
    ) -> Result<()> {
        self.require_connected().await?;
        let opts = opts.unwrap_or_default();
        let frame = frame_builder::state_frame(
            self.frame_ids.next(),
            topic,
            state_key,
            value,
            opts.name.as_deref(),
            opts.ttl_ms,
            opts.subject.as_deref(),
        );
        self.send_frame(frame).await
    }

    /// Send a high-frequency datagram to a topic.
    pub async fn send_datagram(
        &self,
        topic: &str,
        schema: &str,
        payload: serde_json::Value,
        event: Option<&str>,
    ) -> Result<()> {
        self.require_connected().await?;
        let frame =
            frame_builder::datagram_frame(self.frame_ids.next(), topic, schema, event, payload);
        self.send_frame(frame).await
    }

    /// Send a stream segment to a topic.
    pub async fn send_stream_segment(
        &self,
        topic: &str,
        stream_id: &str,
        seq: u64,
        schema: &str,
        payload: serde_json::Value,
        final_segment: bool,
    ) -> Result<()> {
        self.require_connected().await?;
        let frame = frame_builder::stream_frame(
            self.frame_ids.next(),
            topic,
            stream_id,
            seq,
            schema,
            payload,
            final_segment,
        );
        self.send_frame(frame).await
    }

    // ── Ack ──────────────────────────────────────────────────────────────

    /// Send an acknowledgement for a received message.
    pub async fn ack(&self, message_id: &str, status: AckStatus) -> Result<()> {
        self.require_connected().await?;
        let frame = frame_builder::ack_frame(self.frame_ids.next(), message_id, status.as_str());
        self.send_frame(frame).await
    }

    // ── Internal helpers ─────────────────────────────────────────────────

    async fn require_connected(&self) -> Result<()> {
        if self.inner.read().await.is_none() {
            return Err(ClientError::NotConnected);
        }
        Ok(())
    }

    async fn send_frame(&self, frame: Frame) -> Result<()> {
        let inner = self.inner.read().await;
        let conn = inner.as_ref().ok_or(ClientError::NotConnected)?;
        let bytes = encode_frame(&frame)?;
        conn.writer
            .lock()
            .await
            .send(WsMessage::Binary(bytes.to_vec()))
            .await?;
        Ok(())
    }
}

fn mode_str(mode: SubscribeMode) -> &'static str {
    match mode {
        SubscribeMode::Live => "live",
        SubscribeMode::Replay => "replay",
        SubscribeMode::SnapshotThenLive => "snapshot_then_live",
        SubscribeMode::Latest => "latest",
        SubscribeMode::Passive => "passive",
        SubscribeMode::Ephemeral => "ephemeral",
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_reconnect(
    url: &str,
    config: &Arc<RwLock<RiftClientConfig>>,
    event_tx: &broadcast::Sender<ClientEvent>,
    subscriptions: Arc<Mutex<SubscriptionTracker>>,
    closed: Arc<AtomicBool>,
    frame_ids: FrameIdCounter,
    max_attempts: u32,
    base_delay: Duration,
) {
    for attempt in 1..=max_attempts {
        if closed.load(Ordering::SeqCst) {
            return;
        }
        let _ = event_tx.send(ClientEvent::Reconnecting { attempt });
        // Exponential backoff capped at 5x base
        let delay = base_delay * attempt.min(5);
        tokio::time::sleep(delay).await;
        if closed.load(Ordering::SeqCst) {
            return;
        }
        // Bump epoch
        {
            let mut cfg = config.write().await;
            cfg.epoch += 1;
        }
        match crate::connection::connect(
            url,
            config.as_ref(),
            event_tx,
            Arc::clone(&subscriptions),
            frame_ids.clone(),
        )
        .await
        {
            Ok(_inner) => {
                tracing::info!(attempt, "reconnected");
                return;
            }
            Err(e) => {
                tracing::warn!(attempt, "reconnect failed: {e}");
            }
        }
    }
    let _ = event_tx.send(ClientEvent::Error(format!(
        "max reconnect attempts ({max_attempts}) exceeded"
    )));
}
