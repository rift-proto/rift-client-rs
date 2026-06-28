use std::time::Duration;

use rifts::frame::Codec;

/// Configuration for [`RiftClient`].
///
/// Create one via `Default` and then override fields as needed.
#[derive(Debug, Clone)]
pub struct RiftClientConfig {
    /// Unique client identifier (e.g. user ID or app instance).
    pub client_id: String,

    /// Authentication token (JWT or opaque).
    pub token: String,

    /// Session ID for resume; auto-generated if `None`.
    pub session_id: Option<String>,

    /// Session epoch counter, defaults to 1.
    pub epoch: u32,

    /// Preferred codecs in priority order.
    pub codecs: Vec<Codec>,

    /// Protocol feature flags to advertise in Hello.
    pub features: Vec<String>,

    /// Per-topic last-seen offsets for session resume.
    pub last_offsets: std::collections::BTreeMap<String, i64>,

    /// Automatically reconnect on disconnect.
    pub auto_reconnect: bool,

    /// Base delay before the first reconnect attempt.
    pub reconnect_delay: Duration,

    /// Maximum number of consecutive reconnect attempts before giving up.
    pub max_reconnect_attempts: u32,
}

impl Default for RiftClientConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            token: String::new(),
            session_id: None,
            epoch: 1,
            codecs: vec![Codec::Cbor, Codec::Json],
            features: vec!["resume".into()],
            last_offsets: std::collections::BTreeMap::new(),
            auto_reconnect: true,
            reconnect_delay: Duration::from_secs(1),
            max_reconnect_attempts: 10,
        }
    }
}
