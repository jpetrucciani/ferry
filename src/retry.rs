use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;

use crate::config::RetryConfig;

const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Statuses that mean a POST was refused before the server acted on it.
/// 502 and 504 are excluded because the upstream may already have processed the message.
pub fn is_retryable_post_status(status: StatusCode) -> bool {
    status == StatusCode::SERVICE_UNAVAILABLE
}

pub fn compatibility_fallback_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
    )
}

pub fn unknown_session_status(status: StatusCode) -> bool {
    matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND)
}

/// Errors worth retrying for idempotent requests.
pub fn is_retryable_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

/// Errors where the POST body was never delivered, so resending cannot duplicate it.
pub fn is_undelivered_error(error: &reqwest::Error) -> bool {
    error.is_connect()
}

/// A server-provided SSE `retry:` hint is honored as-is; otherwise the delay grows
/// exponentially from the configured base, capped at `MAX_BACKOFF`, plus jitter.
pub fn backoff_delay(
    config: &RetryConfig,
    attempt: usize,
    retry_hint: Option<Duration>,
) -> Duration {
    if let Some(hint) = retry_hint {
        return hint;
    }

    let multiplier = 1_u32 << attempt.min(31);
    let scaled = config
        .base_delay
        .saturating_mul(multiplier)
        .min(MAX_BACKOFF);
    scaled.saturating_add(jitter(config.base_delay))
}

fn jitter(base: Duration) -> Duration {
    let jitter_window_ms = base.as_millis().min(u128::from(u64::MAX)) as u64;
    if jitter_window_ms == 0 {
        return Duration::from_millis(0);
    }

    let nanos = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => u64::from(duration.subsec_nanos()),
        Err(_) => 0,
    };
    Duration::from_millis(nanos % jitter_window_ms)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use reqwest::StatusCode;

    use crate::config::RetryConfig;
    use crate::retry::{backoff_delay, compatibility_fallback_status, is_retryable_post_status};

    #[test]
    fn only_retries_posts_the_server_refused() {
        assert!(is_retryable_post_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_post_status(StatusCode::BAD_GATEWAY));
        assert!(!is_retryable_post_status(StatusCode::GATEWAY_TIMEOUT));
    }

    #[test]
    fn classifies_fallback_statuses() {
        assert!(compatibility_fallback_status(StatusCode::BAD_REQUEST));
        assert!(compatibility_fallback_status(StatusCode::NOT_FOUND));
        assert!(!compatibility_fallback_status(StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn honors_retry_hint_without_scaling() {
        let config = RetryConfig {
            count: 3,
            base_delay: Duration::from_millis(100),
        };

        let delay = backoff_delay(&config, 4, Some(Duration::from_millis(250)));
        assert_eq!(delay, Duration::from_millis(250));
    }

    #[test]
    fn caps_exponential_backoff() {
        let config = RetryConfig {
            count: 1_000,
            base_delay: Duration::from_secs(1),
        };

        let delay = backoff_delay(&config, 999, None);
        assert!(delay < Duration::from_secs(32), "delay was {delay:?}");
    }
}
