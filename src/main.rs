use std::sync::Arc;

use clap::Parser;
use ferry::cli::{Cli, Command};
use ferry::config::Config;
use ferry::error::{Error, Result};
use ferry::logging;
use ferry::server::{self, ServeConfig};
use ferry::stdio::StdioCodec;
use ferry::transport::{build_transport, TransportEvent};
use serde_json::{json, Value};
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const EVENT_CHANNEL_CAPACITY: usize = 256;
const JSONRPC_INTERNAL_ERROR: i64 = -32603;

#[tokio::main]
async fn main() {
    if let Err(error) = try_main().await {
        eprintln!("ferry: {error}");
        std::process::exit(1);
    }
}

async fn try_main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Serve(serve_cli)) => {
            let config = ServeConfig::load(serve_cli)?;
            logging::init(&config.logging)?;
            server::run(config, shutdown_signal()).await
        }
        None => {
            let config = Config::load(cli.proxy)?;
            logging::init(&config.logging)?;
            run_proxy(config).await
        }
    }
}

async fn run_proxy(config: Config) -> Result<()> {
    let mut stdio = StdioCodec::new(tokio::io::stdin(), tokio::io::stdout());
    let (events_tx, mut events) = mpsc::channel(EVENT_CHANNEL_CAPACITY);
    let transport = Arc::new(build_transport(config, events_tx.clone())?);
    transport.connect().await?;

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!("shutdown signal received");
                break;
            }
            line = stdio.read_message() => {
                match line {
                    Ok(Some(message)) => {
                        // Requests can run for a long time, so they are sent concurrently to
                        // keep stdin and server-initiated traffic flowing. `initialize`,
                        // notifications, and responses go inline to preserve their ordering.
                        if is_request(&message) && !is_initialize(&message) {
                            let transport = Arc::clone(&transport);
                            let events_tx = events_tx.clone();
                            tokio::spawn(async move {
                                let id = message.get("id").cloned();
                                let result = transport.send(message).await;
                                if let Some(event) = send_outcome(id, result) {
                                    let _ = events_tx.send(event).await;
                                }
                            });
                        } else {
                            let id = is_request(&message).then(|| message.get("id").cloned()).flatten();
                            let result = transport.send(message).await;
                            match send_outcome(id, result) {
                                Some(TransportEvent::Message(response)) => stdio.write_message(&response).await?,
                                Some(TransportEvent::Fatal(error)) => {
                                    error!(error = %error, "transport reported fatal error");
                                    return Err(error);
                                }
                                None => {}
                            }
                        }
                    }
                    Ok(None) => {
                        info!("stdin closed, shutting down");
                        break;
                    }
                    Err(error) => {
                        warn!(error = %error, "invalid message on stdin, skipping");
                    }
                }
            }
            event = events.recv() => {
                match event {
                    Some(TransportEvent::Message(message)) => stdio.write_message(&message).await?,
                    Some(TransportEvent::Fatal(error)) => {
                        error!(error = %error, "transport reported fatal error");
                        return Err(error);
                    }
                    None => {
                        warn!("transport event channel closed");
                        break;
                    }
                }
            }
        }
    }

    transport.shutdown().await
}

/// Turns the result of sending one stdin message into what, if anything, goes back to the
/// client. A failed request is answered with a JSON-RPC error so the client does not hang.
fn send_outcome(id: Option<Value>, result: Result<Option<Value>>) -> Option<TransportEvent> {
    match result {
        Ok(response) => response.map(TransportEvent::Message),
        Err(Error::SessionExpired) => Some(TransportEvent::Fatal(Error::SessionExpired)),
        Err(error) => {
            error!(error = %error, "failed to send message to remote");
            id.map(|id| {
                TransportEvent::Message(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "error": {
                        "code": JSONRPC_INTERNAL_ERROR,
                        "message": format!("ferry: {error}"),
                    },
                }))
            })
        }
    }
}

fn is_request(message: &Value) -> bool {
    message.get("method").is_some() && message.get("id").is_some()
}

fn is_initialize(message: &Value) -> bool {
    message.get("method").and_then(Value::as_str) == Some("initialize")
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = signal::unix::signal(signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(_) => {
                let _ = signal::ctrl_c().await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = signal::ctrl_c().await;
    }
}
