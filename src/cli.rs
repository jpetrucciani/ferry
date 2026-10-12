use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, Parser)]
#[command(
    name = "ferry",
    version,
    about = "Transparent MCP stdio-to-remote transport proxy"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub proxy: ProxyCli,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    Serve(ServeCli),
}

#[derive(Debug, Clone, Args, Default)]
pub struct ProxyCli {
    #[arg(short = 'u', long)]
    pub url: Option<String>,

    #[arg(short = 't', long, value_enum)]
    pub transport: Option<TransportArg>,

    #[arg(long)]
    pub socks5: Option<String>,

    #[arg(long = "socks5-auth")]
    pub socks5_auth: Option<String>,

    #[arg(short = 'H', long = "header")]
    pub headers: Vec<String>,

    #[arg(long)]
    pub timeout: Option<u64>,

    #[arg(long)]
    pub retry: Option<usize>,

    #[arg(long = "retry-delay")]
    pub retry_delay: Option<u64>,

    #[arg(long = "log-level", value_enum)]
    pub log_level: Option<LogLevelArg>,

    #[arg(long = "log-file")]
    pub log_file: Option<PathBuf>,

    #[arg(short = 'c', long)]
    pub config: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct ServeCli {
    #[arg(long)]
    pub listen: Option<SocketAddr>,

    #[arg(long)]
    pub path: Option<String>,

    #[arg(long = "legacy-sse-path")]
    pub legacy_sse_path: Option<String>,

    #[arg(long = "legacy-messages-path")]
    pub legacy_messages_path: Option<String>,

    #[arg(long = "health-path")]
    pub health_path: Option<String>,

    #[arg(long = "auth-bearer-token")]
    pub auth_bearer_token: Option<String>,

    #[arg(long = "auth-header")]
    pub auth_headers: Vec<String>,

    #[arg(long = "allow-origin")]
    pub allow_origins: Vec<String>,

    /// Seconds a session may sit with no requests and no open stream before it is shut down.
    #[arg(long = "idle-timeout")]
    pub idle_timeout: Option<u64>,

    #[arg(long = "log-level", value_enum)]
    pub log_level: Option<LogLevelArg>,

    #[arg(long = "log-file")]
    pub log_file: Option<PathBuf>,

    #[arg(short = 'c', long)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub backend: Option<ServeBackendCli>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum ServeBackendCli {
    Stdio(ServeStdioCli),
    Relay(ServeRelayCli),
}

#[derive(Debug, Clone, Args)]
pub struct ServeStdioCli {
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Args)]
pub struct ServeRelayCli {
    #[arg(short = 'u', long)]
    pub url: String,

    #[arg(short = 't', long, value_enum)]
    pub transport: Option<TransportArg>,

    #[arg(short = 'H', long = "header")]
    pub headers: Vec<String>,

    #[arg(long)]
    pub timeout: Option<u64>,

    #[arg(long)]
    pub retry: Option<usize>,

    #[arg(long = "retry-delay")]
    pub retry_delay: Option<u64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum TransportArg {
    Auto,
    StreamableHttp,
    Sse,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum LogLevelArg {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::Parser;

    use crate::cli::{Cli, Command, ServeBackendCli};

    #[test]
    fn parses_serve_stdio_subcommand() {
        let cli = Cli::parse_from([
            "ferry",
            "serve",
            "--listen",
            "127.0.0.1:9000",
            "--config",
            "/tmp/ferry.toml",
            "stdio",
            "--",
            "example-server",
            "--stdio",
        ]);

        match cli.command {
            Some(Command::Serve(serve)) => match serve.backend {
                Some(ServeBackendCli::Stdio(stdio)) => {
                    assert_eq!(
                        serve.listen.map(|value| value.to_string()),
                        Some("127.0.0.1:9000".to_string())
                    );
                    assert_eq!(serve.config, Some(PathBuf::from("/tmp/ferry.toml")));
                    assert_eq!(stdio.command, vec!["example-server", "--stdio"]);
                }
                Some(ServeBackendCli::Relay(_)) => panic!("expected stdio backend"),
                None => panic!("expected stdio backend"),
            },
            None => panic!("expected serve subcommand"),
        }
    }
}
