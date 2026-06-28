#![deny(missing_docs)]
#![doc = "**DEPRECATED** — use `rifts` crate with feature `client` instead."]

//! # rift-client — Rift Realtime Protocol / 1.0 Async Rust Client SDK
//!
//! > **DEPRECATED**: This crate has been merged into [`rifts`](https://crates.io/crates/rifts).
//! > Use `rifts` with feature `client` instead.
//!
//! Connect to a [`riftrust`](https://docs.rs/rifts) server over WebSocket,
//! perform the Hello/Welcome/Ready handshake, and interact with topics
//! through a typed, broadcast-based event system.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use rift_client::{RiftClient, RiftClientConfig, ClientEvent};
//! use rifts::message::SubscribeMode;
//!
//! # async fn run() -> rift_client::Result<()> {
//! let client = RiftClient::new(
//!     "ws://localhost:9000",
//!     RiftClientConfig {
//!         client_id: "my-app".into(),
//!         token:    "my-jwt".into(),
//!         ..Default::default()
//!     },
//! );
//!
//! let mut events = client.subscribe_events();
//! client.connect().await?;
//!
//! client.subscribe("room/1", SubscribeMode::Live, None).await?;
//! client.publish(
//!     "room/1", "chat.message", "chat.message@1.0",
//!     serde_json::json!({"text": "hello"}),
//!     None,
//! ).await?;
//!
//! while let Ok(evt) = events.recv().await {
//!     match evt {
//!         ClientEvent::EventReceived { topic, event } => {
//!             println!("[{topic}] {}: {:?}", event.event, event.payload);
//!         }
//!         ClientEvent::Disconnected { .. } => break,
//!         _ => {}
//!     }
//! }
//! # Ok(())
//! # }
//! ```

mod client;
mod config;
mod connection;
mod error;
mod events;
mod frame_builder;
mod heartbeat;
mod subscriber;

pub use client::{CommandOpts, PublishOpts, RiftClient, StateOpts};
pub use config::RiftClientConfig;
pub use error::{ClientError, Result};
pub use events::ClientEvent;

// Re-export commonly used rifts types for convenience.
pub use rifts::frame::{Codec, Frame, FrameFlags, FrameType, Priority};
pub use rifts::message::SubscribeMode;
pub use rifts::message::command::Reply;
pub use rifts::protocol::close::CloseCode;
