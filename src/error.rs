use std::fmt::{Display, Formatter};
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use reqwest::header::{InvalidHeaderName, InvalidHeaderValue};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Json(serde_json::Error),
    Toml(toml::de::Error),
    Http(reqwest::Error),
    HeaderName(InvalidHeaderName),
    HeaderValue(InvalidHeaderValue),
    MissingUrl,
    InvalidConfigPath(PathBuf),
    InvalidSocks5Auth(String),
    InvalidTimeout(u64),
    InvalidRetryDelay(u64),
    InvalidRetryCount(usize),
    InvalidTransport(String),
    InvalidLogLevel(String),
    InvalidHeader(String),
    InvalidEnvInterpolation(String),
    InvalidContentType(String),
    InvalidResponse(String),
    CompatibilityFallback(reqwest::StatusCode),
    SessionExpired,
    Shutdown,
    Message(String),
    RequestTimeout(Duration),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "io error: {error}"),
            Self::Json(error) => write!(formatter, "json error: {error}"),
            Self::Toml(error) => write!(formatter, "toml error: {error}"),
            Self::Http(error) => write!(formatter, "http error: {error}"),
            Self::HeaderName(error) => write!(formatter, "invalid header name: {error}"),
            Self::HeaderValue(error) => write!(formatter, "invalid header value: {error}"),
            Self::MissingUrl => formatter.write_str("missing remote URL"),
            Self::InvalidConfigPath(path) => {
                write!(formatter, "config file does not exist: {}", path.display())
            }
            Self::InvalidSocks5Auth(value) => {
                write!(
                    formatter,
                    "invalid SOCKS5 auth, expected USER:PASS: {value}"
                )
            }
            Self::InvalidTimeout(value) => {
                write!(formatter, "timeout must be at least 1 second, got {value}")
            }
            Self::InvalidRetryDelay(value) => {
                write!(formatter, "retry delay must be at least 1 ms, got {value}")
            }
            Self::InvalidRetryCount(value) => {
                write!(
                    formatter,
                    "retry count must be finite and reasonable, got {value}"
                )
            }
            Self::InvalidTransport(value) => write!(formatter, "invalid transport: {value}"),
            Self::InvalidLogLevel(value) => write!(formatter, "invalid log level: {value}"),
            Self::InvalidHeader(value) => write!(formatter, "invalid header: {value}"),
            Self::InvalidEnvInterpolation(value) => {
                write!(
                    formatter,
                    "missing environment variable for interpolation: {value}"
                )
            }
            Self::InvalidContentType(value) => {
                write!(formatter, "unsupported content type: {value}")
            }
            Self::InvalidResponse(value) => write!(formatter, "invalid response: {value}"),
            Self::CompatibilityFallback(status) => {
                write!(
                    formatter,
                    "compatibility fallback triggered by HTTP status {status}"
                )
            }
            Self::SessionExpired => formatter.write_str("remote MCP session expired"),
            Self::Shutdown => formatter.write_str("shutdown requested"),
            Self::Message(value) => formatter.write_str(value),
            Self::RequestTimeout(duration) => {
                write!(formatter, "request timed out after {}s", duration.as_secs())
            }
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<toml::de::Error> for Error {
    fn from(value: toml::de::Error) -> Self {
        Self::Toml(value)
    }
}

impl From<reqwest::Error> for Error {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value)
    }
}

impl From<InvalidHeaderName> for Error {
    fn from(value: InvalidHeaderName) -> Self {
        Self::HeaderName(value)
    }
}

impl From<InvalidHeaderValue> for Error {
    fn from(value: InvalidHeaderValue) -> Self {
        Self::HeaderValue(value)
    }
}
