use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use futures_util::SinkExt;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use rifts::protocol::hello::Ready;
use rifts::transport::frame_codec::encode_frame;

use crate::frame_builder::{FrameIdCounter, ping_frame};

type WsWriter =
    futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, WsMessage>;

/// Shared state between the heartbeat task and the reader task.
pub struct HeartbeatState {
    pub missed_pongs: AtomicU32,
}

impl HeartbeatState {
    pub fn new() -> Self {
        Self {
            missed_pongs: AtomicU32::new(0),
        }
    }

    /// Called by the reader task when a Pong frame arrives.
    pub fn reset_missed(&self) {
        self.missed_pongs.store(0, Ordering::SeqCst);
    }

    pub fn missed(&self) -> u32 {
        self.missed_pongs.load(Ordering::SeqCst)
    }
}

/// Spawn the heartbeat loop.
pub fn spawn_heartbeat(
    writer: Arc<Mutex<WsWriter>>,
    ready: &Ready,
    frame_ids: FrameIdCounter,
) -> (JoinHandle<()>, Arc<HeartbeatState>) {
    let state = Arc::new(HeartbeatState::new());
    let ping_interval = Duration::from_millis(ready.ping_interval_ms as u64);
    let pong_timeout = Duration::from_millis(ready.pong_timeout_ms as u64);
    let max_missed = ready.max_missed_pongs;
    let jitter_ms = ready.jitter_ms;

    let state_clone = Arc::clone(&state);
    let handle = tokio::spawn(async move {
        heartbeat_loop(
            writer,
            state_clone,
            frame_ids,
            ping_interval,
            pong_timeout,
            max_missed,
            jitter_ms,
        )
        .await;
    });

    (handle, state)
}

async fn heartbeat_loop(
    writer: Arc<Mutex<WsWriter>>,
    state: Arc<HeartbeatState>,
    frame_ids: FrameIdCounter,
    ping_interval: Duration,
    pong_timeout: Duration,
    max_missed: u32,
    jitter_ms: u32,
) {
    let mut rng = StdRng::from_os_rng();
    loop {
        // Add random jitter: [0, 2*jitter_ms]
        let jitter = if jitter_ms > 0 {
            let j: u64 = rng.random_range(0..=jitter_ms as u64 * 2);
            Duration::from_millis(j.saturating_sub(jitter_ms as u64))
        } else {
            Duration::ZERO
        };
        let wait = ping_interval.saturating_add(jitter);
        tokio::time::sleep(wait).await;

        // Send ping
        let id = frame_ids.next();
        let ping = ping_frame(id);
        let encoded = match encode_frame(&ping) {
            Ok(b) => b,
            Err(_) => continue,
        };
        {
            let mut w = writer.lock().await;
            if w.send(WsMessage::Binary(encoded.to_vec())).await.is_err() {
                break;
            }
        }

        // Wait for pong
        tokio::time::sleep(pong_timeout).await;
        if state.missed() == 0 {
            continue; // pong was received
        }

        state.missed_pongs.fetch_add(1, Ordering::SeqCst);
        let current = state.missed();
        if current >= max_missed {
            tracing::warn!(missed = current, "heartbeat timeout — closing connection");
            let mut w = writer.lock().await;
            // Close without frame details (tungstenite will use default close)
            let _ = w.send(WsMessage::Close(None)).await;
            break;
        }
    }
}
