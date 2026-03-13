use std::time::{Duration, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;

use crate::config::RetryConfig;

pub fn is_retryable_http_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
    )
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

pub fn is_retryable_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || error.is_connect() || error.is_request()
}

pub fn backoff_delay(
    config: &RetryConfig,
    attempt: usize,
    retry_hint: Option<Duration>,
) -> Duration {
    let base = retry_hint.unwrap_or(config.base_delay);
    let multiplier = 1_u32 << attempt.min(31);
    let scaled = base.saturating_mul(multiplier);
    scaled.saturating_add(jitter(base))
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
    use crate::retry::{backoff_delay, compatibility_fallback_status, is_retryable_http_status};

    #[test]
    fn classifies_retryable_http_statuses() {
        assert!(is_retryable_http_status(StatusCode::BAD_GATEWAY));
        assert!(is_retryable_http_status(StatusCode::SERVICE_UNAVAILABLE));
        assert!(!is_retryable_http_status(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn classifies_fallback_statuses() {
        assert!(compatibility_fallback_status(StatusCode::BAD_REQUEST));
        assert!(compatibility_fallback_status(StatusCode::NOT_FOUND));
        assert!(!compatibility_fallback_status(StatusCode::UNAUTHORIZED));
    }

    #[test]
    fn respects_retry_hint() {
        let config = RetryConfig {
            count: 3,
            base_delay: Duration::from_millis(100),
        };

        let delay = backoff_delay(&config, 0, Some(Duration::from_millis(250)));
        assert!(delay >= Duration::from_millis(250));
    }
}
