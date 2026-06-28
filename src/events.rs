use rifts::ack::AckStatus;
use rifts::message::command::Reply;
use rifts::message::datagram::Datagram;
use rifts::message::event::Event;
use rifts::message::snapshot::Snapshot;
use rifts::message::state::State;
use rifts::message::stream::StreamSegment;

/// Events emitted by [`RiftClient`](crate::RiftClient) via its broadcast channel.
///
/// Obtain a receiver with [`RiftClient::subscribe_events`](crate::RiftClient::subscribe_events).
#[derive(Debug, Clone)]
pub enum ClientEvent {
    /// Connection established — Ready frame received.
    Connected {
        /// Server-assigned session ID.
        session_id: String,
        /// Session epoch.
        epoch: u32,
    },

    /// Connection lost.
    Disconnected {
        /// WebSocket close code.
        code: u16,
        /// Human-readable close reason.
        reason: String,
    },

    /// Business event on a subscribed topic.
    EventReceived {
        /// The topic the event was published to.
        topic: String,
        /// The decoded event payload.
        event: Event,
    },

    /// Command reply received.
    ReplyReceived {
        /// The decoded reply.
        reply: Reply,
    },

    /// State message received.
    StateReceived {
        /// The topic the state update targets.
        topic: String,
        /// The decoded state payload.
        state: State,
    },

    /// Datagram received.
    DatagramReceived {
        /// The topic the datagram was sent to.
        topic: String,
        /// The decoded datagram payload.
        datagram: Datagram,
    },

    /// Stream segment received.
    StreamReceived {
        /// The topic the stream belongs to.
        topic: String,
        /// The decoded stream segment.
        segment: StreamSegment,
    },

    /// Snapshot received.
    SnapshotReceived {
        /// The topic the snapshot belongs to.
        topic: String,
        /// The decoded snapshot payload.
        snapshot: Snapshot,
    },

    /// System message received.
    System {
        /// The system event name.
        event_name: String,
        /// The event payload.
        payload: serde_json::Value,
    },

    /// Server acknowledgement received.
    AckReceived {
        /// The message ID that was acknowledged.
        message_id: String,
        /// The acknowledgement status.
        status: AckStatus,
    },

    /// Pong received from server.
    Pong {
        /// Server-reported timestamp (milliseconds since epoch).
        timestamp: i64,
    },

    /// Protocol or transport error.
    Error(String),

    /// Client is about to attempt a reconnect.
    Reconnecting {
        /// The current reconnect attempt number (1-indexed).
        attempt: u32,
    },
}
