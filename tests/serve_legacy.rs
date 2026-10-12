use std::time::Duration;

use ferry::config::{Config, LogLevel, LoggingConfig, RetryConfig, TransportMode};
use ferry::transport::{build_transport, TransportEvent};
use reqwest::header::HeaderMap;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::timeout;

mod common;

use common::FerryProcess;

const PROTOCOL_VERSION: &str = "2025-11-25";

#[tokio::test]
async fn serve_legacy_stdio_works_with_sse_transport() -> Result<(), Box<dyn std::error::Error>> {
    let mut ferry = FerryProcess::spawn_stdio().await?;
    let (sender, mut receiver) = mpsc::channel(32);
    let transport = build_transport(
        legacy_transport_config(&format!("{}/sse", ferry.base_url))?,
        sender,
    )?;

    transport.connect().await?;
    assert!(transport
        .send(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION
            }
        }))
        .await?
        .is_none());

    let initialize = next_message(&mut receiver).await?;
    assert_eq!(
        initialize["result"]["protocolVersion"],
        Value::String(PROTOCOL_VERSION.to_string())
    );

    let ready = next_message(&mut receiver).await?;
    assert_eq!(ready["method"], Value::String("test/ready".to_string()));

    assert!(transport
        .send(json!({
            "jsonrpc": "2.0",
            "method": "notify_client",
            "params": {
                "value": "legacy"
            }
        }))
        .await?
        .is_none());
    let notification = next_message(&mut receiver).await?;
    assert_eq!(
        notification["method"],
        Value::String("test/notification".to_string())
    );
    assert_eq!(
        notification["params"]["value"],
        Value::String("legacy".to_string())
    );

    assert!(transport
        .send(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "delayed_echo",
            "params": {
                "value": "first",
                "delayMs": 200
            }
        }))
        .await?
        .is_none());

    let duplicate_error = transport
        .send(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "delayed_echo",
            "params": {
                "value": "duplicate",
                "delayMs": 10
            }
        }))
        .await
        .expect_err("duplicate legacy id should fail");
    assert!(duplicate_error.to_string().contains("HTTP 409"));

    let delayed = next_message(&mut receiver).await?;
    assert_eq!(
        delayed["result"]["echo"],
        Value::String("first".to_string())
    );

    transport.shutdown().await?;
    ferry.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn serve_legacy_relay_tunnels_to_upstream_ferry() -> Result<(), Box<dyn std::error::Error>> {
    let mut upstream = FerryProcess::spawn_stdio().await?;
    let mut relay =
        FerryProcess::spawn_relay(&format!("{}/mcp", upstream.base_url), "streamable-http").await?;
    let (sender, mut receiver) = mpsc::channel(32);
    let transport = build_transport(
        legacy_transport_config(&format!("{}/sse", relay.base_url))?,
        sender,
    )?;

    transport.connect().await?;
    assert!(transport
        .send(json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": PROTOCOL_VERSION
            }
        }))
        .await?
        .is_none());

    let initialize = next_message(&mut receiver).await?;
    assert_eq!(
        initialize["result"]["protocolVersion"],
        Value::String(PROTOCOL_VERSION.to_string())
    );

    let ready = next_message(&mut receiver).await?;
    assert_eq!(ready["method"], Value::String("test/ready".to_string()));

    assert!(transport
        .send(json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "delayed_echo",
            "params": {
                "value": "through-legacy-relay",
                "delayMs": 25
            }
        }))
        .await?
        .is_none());

    let echo = next_message(&mut receiver).await?;
    assert_eq!(
        echo["result"]["echo"],
        Value::String("through-legacy-relay".to_string())
    );

    transport.shutdown().await?;
    relay.shutdown().await?;
    upstream.shutdown().await?;
    Ok(())
}

fn legacy_transport_config(url: &str) -> Result<Config, Box<dyn std::error::Error>> {
    Ok(Config {
        url: url.parse()?,
        transport: TransportMode::Sse,
        headers: HeaderMap::new(),
        timeout: Duration::from_secs(3),
        socks5: None,
        retry: RetryConfig {
            count: 1,
            base_delay: Duration::from_millis(25),
        },
        logging: LoggingConfig {
            level: LogLevel::Warn,
            file: None,
        },
    })
}

async fn next_message(
    receiver: &mut mpsc::Receiver<TransportEvent>,
) -> Result<Value, Box<dyn std::error::Error>> {
    match timeout(Duration::from_secs(3), receiver.recv()).await? {
        Some(TransportEvent::Message(message)) => Ok(message),
        Some(TransportEvent::Fatal(error)) => Err(Box::new(error)),
        None => Err("transport event channel closed unexpectedly".into()),
    }
}
