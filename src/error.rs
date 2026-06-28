use thiserror::Error;

/// Errors produced by [`RiftClient`](crate::RiftClient).
#[derive(Debug, Error)]
pub enum ClientError {
    /// The client is not connected.
    #[error("connection not open")]
    NotConnected,

    /// [`RiftClient::connect`](crate::RiftClient::connect) was called while already connected.
    #[error("already connected")]
    AlreadyConnected,

    /// Underlying WebSocket error.
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    /// Protocol-level error from the `rifts` codec.
    #[error("rifts protocol error: {0}")]
    Protocol(#[from] rifts::error::RiftError),

    /// The Hello/Welcome/Ready handshake failed.
    #[error("handshake failed: {0}")]
    Handshake(String),

    /// Too many consecutive pongs were missed.
    #[error("heartbeat timeout — {0} consecutive pongs missed")]
    HeartbeatTimeout(u32),

    /// All reconnect attempts have been exhausted.
    #[error("max reconnect attempts ({0}) exceeded")]
    MaxReconnect(u32),

    /// A command did not receive a reply within its timeout.
    #[error("command timed out after {0}ms")]
    CommandTimeout(u64),

    /// JSON serialization / deserialization error.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Catch-all for other errors.
    #[error("{0}")]
    Other(String),
}

/// Convenience alias for `Result<T, ClientError>`.
pub type Result<T> = std::result::Result<T, ClientError>;
