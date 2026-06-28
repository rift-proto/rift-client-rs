# rift-client

Async Rust client SDK for the [Rift Realtime Protocol / 1.0](https://rift.dev) — a WebSocket-based bidirectional messaging protocol built for AI-era realtime use cases.

[![CI](https://github.com/rift-proto/rift-client-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/rift-proto/rift-client-rs/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/rift-client)](https://crates.io/crates/rift-client)
[![Documentation](https://docs.rs/rift-client/badge.svg)](https://docs.rs/rift-client)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)](#license)

## Features

- **Publish / Subscribe** — typed event delivery with `Live`, `Replay`, `SnapshotThenLive`, `Latest`, `Passive`, and `Ephemeral` modes
- **Command / Reply** — request-response over topics with correlation IDs and timeouts
- **State** — lightweight key-value state per topic
- **Datagrams** — high-frequency, fire-and-forget messages
- **Streams** — ordered, segmented streaming (e.g. LLM token output)
- **Snapshots** — full state snapshots on subscribe
- **Session resume** — automatic session/epoch tracking for seamless reconnect
- **Auto-reconnect** — exponential backoff with configurable limits
- **Heartbeat** — server-driven ping/pong with jitter

## Quick Start

```rust
use rift_client::{RiftClient, RiftClientConfig, ClientEvent};
use rifts::message::SubscribeMode;

#[tokio::main]
async fn main() -> rift_client::Result<()> {
    let client = RiftClient::new(
        "ws://localhost:9000",
        RiftClientConfig {
            client_id: "my-app".into(),
            token:    "my-jwt".into(),
            ..Default::default()
        },
    );

    let mut events = client.subscribe_events();
    client.connect().await?;

    client.subscribe("room/1", SubscribeMode::Live, None).await?;

    client.publish(
        "room/1",
        "chat.message",
        "chat.message@1.0",
        serde_json::json!({"text": "hello"}),
        None,
    ).await?;

    while let Ok(evt) = events.recv().await {
        match evt {
            ClientEvent::EventReceived { topic, event } => {
                println!("[{topic}] {}: {:?}", event.event, event.payload);
            }
            ClientEvent::Disconnected { .. } => break,
            _ => {}
        }
    }

    Ok(())
}
```

## API Overview

### Connection

```rust
let client = RiftClient::new(url, config);
client.connect().await?;
client.is_connected().await;     // bool
client.session_id().await;       // Option<String>
client.close().await?;
```

### Pub/Sub

```rust
client.subscribe(topic, mode, from_offset).await?;
client.unsubscribe(topic).await?;

client.publish(topic, event, schema, payload, opts).await?;
client.publish_state(topic, state_key, value, opts).await?;
client.send_datagram(topic, schema, payload, event).await?;
client.send_stream_segment(topic, stream_id, seq, schema, payload, final_segment).await?;
```

### Command / Reply

```rust
let reply = client.command(topic, cmd, schema, payload, opts).await?;
```

### Events

```rust
let mut rx = client.subscribe_events();

while let Ok(evt) = rx.recv().await {
    match evt {
        ClientEvent::Connected { session_id, epoch } => { /* ... */ }
        ClientEvent::Disconnected { code, reason }     => { /* ... */ }
        ClientEvent::EventReceived { topic, event }    => { /* ... */ }
        ClientEvent::ReplyReceived { reply }           => { /* ... */ }
        ClientEvent::StateReceived { topic, state }    => { /* ... */ }
        ClientEvent::DatagramReceived { topic, datagram } => { /* ... */ }
        ClientEvent::StreamReceived { topic, segment }    => { /* ... */ }
        ClientEvent::SnapshotReceived { topic, snapshot } => { /* ... */ }
        ClientEvent::System { event_name, payload }    => { /* ... */ }
        ClientEvent::AckReceived { message_id, status } => { /* ... */ }
        ClientEvent::Pong { timestamp }                => { /* ... */ }
        ClientEvent::Error(msg)                        => { /* ... */ }
        ClientEvent::Reconnecting { attempt }          => { /* ... */ }
    }
}
```

### Configuration

```rust
RiftClientConfig {
    client_id:                String,                    // required
    token:                    String,                    // auth token (JWT or opaque)
    session_id:               Option<String>,            // for resume; auto-generated
    epoch:                    u32,                       // default: 1
    codecs:                   Vec<Codec>,                // default: [Cbor, Json]
    features:                 Vec<String>,               // default: ["resume"]
    last_offsets:             BTreeMap<String, i64>,     // per-topic resume offsets
    auto_reconnect:           bool,                      // default: true
    reconnect_delay:          Duration,                  // default: 1s
    max_reconnect_attempts:   u32,                       // default: 10
}
```

## Minimum Supported Rust Version

Rust **1.85** (edition 2024).

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
