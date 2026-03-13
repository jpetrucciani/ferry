use clap::Parser;
use ferry::cli::Cli;
use ferry::config::Config;
use ferry::error::{Error, Result};
use ferry::logging;
use ferry::stdio::StdioCodec;
use ferry::transport::{build_transport, TransportEvent};
use tokio::signal;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() {
    if let Err(error) = try_main().await {
        eprintln!("ferry: {error}");
        std::process::exit(1);
    }
}

async fn try_main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(cli)?;
    logging::init(&config.logging)?;
    run(config).await
}

async fn run(config: Config) -> Result<()> {
    let mut stdio = StdioCodec::new(tokio::io::stdin(), tokio::io::stdout());
    let (mut transport, mut events) = build_transport(config)?;
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
                        match transport.send(message).await {
                            Ok(Some(response)) => stdio.write_message(&response).await?,
                            Ok(None) => {}
                            Err(Error::SessionExpired) => {
                                error!("remote MCP session expired");
                                return Err(Error::SessionExpired);
                            }
                            Err(error) => {
                                error!(error = %error, "failed to send message to remote");
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
                    Some(TransportEvent::Fatal(Error::SessionExpired)) => {
                        error!("remote MCP session expired");
                        return Err(Error::SessionExpired);
                    }
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
