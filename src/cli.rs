use std::path::PathBuf;

use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, Parser)]
#[command(
    name = "ferry",
    version,
    about = "Transparent MCP stdio-to-remote transport proxy"
)]
pub struct Cli {
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
