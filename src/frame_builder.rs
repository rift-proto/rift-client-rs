use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rifts::frame::{Codec, Frame, FrameFlags, FrameType, Priority};
use serde_json::Value as JsonValue;

/// Shared, atomically incremented frame ID counter.
#[derive(Debug, Clone)]
pub struct FrameIdCounter(Arc<AtomicU64>);

impl FrameIdCounter {
    pub fn new() -> Self {
        Self(Arc::new(AtomicU64::new(0)))
    }

    pub fn next(&self) -> u64 {
        self.0.fetch_add(1, Ordering::SeqCst) + 1
    }
}

impl Default for FrameIdCounter {
    fn default() -> Self {
        Self::new()
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ── Control frames ────────────────────────────────────────────────────────

/// Build a Control frame carrying a JSON payload.
pub fn control_frame(id: u64, payload: JsonValue) -> Frame {
    let json = serde_json::to_vec(&payload).unwrap_or_default();
    Frame {
        version: 0x0100,
        frame_id: id,
        frame_type: FrameType::Control,
        flags: FrameFlags::empty(),
        codec: Codec::Json,
        timestamp: now_ms(),
        payload: Some(Bytes::from(json)),
        ..Default::default()
    }
}

/// Build a Hello control frame.
#[allow(clippy::too_many_arguments)]
pub fn hello_frame(
    id: u64,
    client_id: &str,
    session_id: Option<&str>,
    epoch: u32,
    codecs: &[Codec],
    token: &str,
    last_offsets: &std::collections::BTreeMap<String, i64>,
    features: &[String],
) -> Frame {
    let codec_strs: Vec<&str> = codecs.iter().map(|c| c.name()).collect();
    let sdk = serde_json::json!({"name": "rift-client", "version": env!("CARGO_PKG_VERSION")});
    let payload = serde_json::json!({
        "protocol": "rift",
        "version": 0x0100_u32,
        "client_id": client_id,
        "session_id": session_id,
        "epoch": epoch,
        "codecs": codec_strs,
        "auth_modes": ["bearer"],
        "token": token,
        "last_offsets": last_offsets,
        "sdk": sdk,
        "features": features,
    });
    control_frame(id, payload)
}

// ── Data frames ───────────────────────────────────────────────────────────

/// Build a Data frame carrying an event publish.
#[allow(clippy::too_many_arguments)]
pub fn event_frame(
    id: u64,
    topic: &str,
    event: &str,
    message_id: &str,
    schema: &str,
    payload: JsonValue,
    dedupe_key: Option<&str>,
    ordering_key: Option<&str>,
    ttl_ms: Option<u32>,
    priority: Option<Priority>,
) -> Frame {
    let body = serde_json::json!({
        "class": "event",
        "event": event,
        "message_id": message_id,
        "schema": schema,
        "payload": payload,
        "dedupe_key": dedupe_key,
        "ordering_key": ordering_key,
        "ttl_ms": ttl_ms,
    });
    let mut frame = data_frame(id, topic, Some(event), Some(message_id), None, body);
    frame.priority = priority;
    frame.ttl_ms = ttl_ms;
    frame
}

/// Build a Data frame carrying a command.
#[allow(clippy::too_many_arguments)]
pub fn command_frame(
    id: u64,
    topic: &str,
    command: &str,
    correlation_id: &str,
    timeout_ms: u64,
    schema: &str,
    payload: JsonValue,
    idempotency_key: Option<&str>,
    priority: Option<Priority>,
) -> Frame {
    let body = serde_json::json!({
        "class": "command",
        "command": command,
        "correlation_id": correlation_id,
        "timeout_ms": timeout_ms,
        "idempotency_key": idempotency_key,
        "schema": schema,
        "payload": payload,
    });
    let mut frame = data_frame(id, topic, None, None, Some(correlation_id), body);
    frame.priority = priority;
    frame
}

/// Build a Data frame carrying a state update.
pub fn state_frame(
    id: u64,
    topic: &str,
    state_key: &str,
    value: JsonValue,
    name: Option<&str>,
    ttl_ms: Option<u32>,
    subject: Option<&str>,
) -> Frame {
    let body = serde_json::json!({
        "class": "state",
        "state_key": state_key,
        "name": name,
        "value": value,
        "ttl_ms": ttl_ms,
        "subject": subject,
        "updated_at": now_ms(),
    });
    data_frame(id, topic, None, None, None, body)
}

/// Build a Data frame carrying a datagram.
pub fn datagram_frame(
    id: u64,
    topic: &str,
    schema: &str,
    event: Option<&str>,
    payload: JsonValue,
) -> Frame {
    let body = serde_json::json!({
        "class": "datagram",
        "schema": schema,
        "event": event,
        "payload": payload,
    });
    data_frame(id, topic, None, None, None, body)
}

/// Build a Data frame carrying a stream segment.
pub fn stream_frame(
    id: u64,
    topic: &str,
    stream_id: &str,
    seq: u64,
    schema: &str,
    payload: JsonValue,
    final_segment: bool,
) -> Frame {
    let body = serde_json::json!({
        "class": "stream",
        "stream_id": stream_id,
        "seq": seq,
        "final_segment": final_segment,
        "schema": schema,
        "payload": payload,
    });
    data_frame(id, topic, None, None, None, body)
}

// ── Subscribe / Unsubscribe ───────────────────────────────────────────────

/// Build a subscribe control frame.
pub fn subscribe_frame(id: u64, topic: &str, mode: &str, from_offset: Option<i64>) -> Frame {
    control_frame(
        id,
        serde_json::json!({
            "type": "subscribe",
            "topic": topic,
            "mode": mode,
            "from_offset": from_offset,
            "filter": null,
        }),
    )
}

/// Build an unsubscribe control frame.
pub fn unsubscribe_frame(id: u64, topic: &str) -> Frame {
    control_frame(
        id,
        serde_json::json!({
            "type": "unsubscribe",
            "topic": topic,
        }),
    )
}

// ── Ack ───────────────────────────────────────────────────────────────────

/// Build an Ack frame.
pub fn ack_frame(id: u64, message_id: &str, status: &str) -> Frame {
    let body = serde_json::json!({
        "message_id": message_id,
        "status": status,
    });
    let json = serde_json::to_vec(&body).unwrap_or_default();
    Frame {
        version: 0x0100,
        frame_id: id,
        frame_type: FrameType::Ack,
        flags: FrameFlags::empty(),
        codec: Codec::Json,
        timestamp: now_ms(),
        payload: Some(Bytes::from(json)),
        ..Default::default()
    }
}

// ── Flow (ping/pong) ─────────────────────────────────────────────────────

/// Build a Ping flow frame.
pub fn ping_frame(id: u64) -> Frame {
    let body = serde_json::json!({"type": "ping", "timestamp": now_ms()});
    let json = serde_json::to_vec(&body).unwrap_or_default();
    Frame {
        version: 0x0100,
        frame_id: id,
        frame_type: FrameType::Flow,
        flags: FrameFlags::empty(),
        codec: Codec::Json,
        timestamp: now_ms(),
        payload: Some(Bytes::from(json)),
        ..Default::default()
    }
}

// ── Internal helpers ──────────────────────────────────────────────────────

fn data_frame(
    id: u64,
    topic: &str,
    event: Option<&str>,
    message_id: Option<&str>,
    correlation_id: Option<&str>,
    body: JsonValue,
) -> Frame {
    let json = serde_json::to_vec(&body).unwrap_or_default();
    Frame {
        version: 0x0100,
        frame_id: id,
        frame_type: FrameType::Data,
        flags: FrameFlags::empty(),
        codec: Codec::Json,
        timestamp: now_ms(),
        topic: Some(topic.to_string()),
        event: event.map(String::from),
        message_id: message_id.map(String::from),
        correlation_id: correlation_id.map(String::from),
        payload: Some(Bytes::from(json)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_id_counter_is_monotonic() {
        let counter = FrameIdCounter::new();
        let a = counter.next();
        let b = counter.next();
        assert!(b > a);
    }

    #[test]
    fn hello_frame_has_protocol_and_version() {
        let offsets = std::collections::BTreeMap::new();
        let f = hello_frame(1, "app", None, 1, &[Codec::Json], "tok", &offsets, &[]);
        assert_eq!(f.frame_type, FrameType::Control);
        let payload: JsonValue = serde_json::from_slice(f.payload.as_ref().unwrap()).unwrap();
        assert_eq!(payload["protocol"], "rift");
        assert_eq!(payload["version"], 0x0100);
    }

    #[test]
    fn event_frame_has_class_and_topic() {
        let f = event_frame(
            1,
            "room/1",
            "chat.msg",
            "msg-1",
            "chat.msg@1.0",
            serde_json::json!({"text": "hi"}),
            None,
            None,
            None,
            None,
        );
        assert_eq!(f.frame_type, FrameType::Data);
        assert_eq!(f.topic.as_deref(), Some("room/1"));
    }

    #[test]
    fn ping_frame_is_flow_type() {
        let f = ping_frame(1);
        assert_eq!(f.frame_type, FrameType::Flow);
    }
}
