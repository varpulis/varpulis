//! Rate limiting module for Varpulis CLI
//!
//! Provides token bucket rate limiting for WebSocket connections.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use warp::Filter;

// =============================================================================
// Configuration
// =============================================================================

/// Rate limiting configuration
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Whether rate limiting is enabled
    pub enabled: bool,
    /// Maximum requests per second per client
    pub requests_per_second: u32,
    /// Burst capacity (max tokens in bucket)
    pub burst_size: u32,
    /// Maximum number of tracked IP addresses (prevents memory exhaustion)
    pub max_tracked_ips: usize,
}

impl RateLimitConfig {
    /// Default maximum number of tracked IP addresses.
    const DEFAULT_MAX_TRACKED_IPS: usize = 10_000;

    /// Create a disabled rate limit config
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            requests_per_second: 0,
            burst_size: 0,
            max_tracked_ips: Self::DEFAULT_MAX_TRACKED_IPS,
        }
    }

    /// Create a rate limit config with the given rate
    pub fn new(requests_per_second: u32) -> Self {
        Self {
            enabled: true,
            requests_per_second,
            // Default burst size is 2x the rate
            burst_size: requests_per_second.saturating_mul(2),
            max_tracked_ips: Self::DEFAULT_MAX_TRACKED_IPS,
        }
    }

    /// Create a rate limit config with custom burst size
    pub fn with_burst(requests_per_second: u32, burst_size: u32) -> Self {
        Self {
            enabled: true,
            requests_per_second,
            burst_size,
            max_tracked_ips: Self::DEFAULT_MAX_TRACKED_IPS,
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

// =============================================================================
// Token Bucket
// =============================================================================

/// Token bucket state for a single client
#[derive(Debug, Clone)]
struct TokenBucket {
    /// Current number of tokens
    tokens: f64,
    /// Last time tokens were added
    last_update: Instant,
    /// Maximum tokens (burst capacity)
    max_tokens: f64,
    /// Tokens added per second
    refill_rate: f64,
}

impl TokenBucket {
    fn new(max_tokens: u32, refill_rate: u32) -> Self {
        Self {
            tokens: max_tokens as f64,
            last_update: Instant::now(),
            max_tokens: max_tokens as f64,
            refill_rate: refill_rate as f64,
        }
    }

    /// Try to consume a token, returning true if successful
    fn try_consume(&mut self) -> bool {
        self.refill();

        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_update);
        let new_tokens = elapsed.as_secs_f64() * self.refill_rate;

        self.tokens = (self.tokens + new_tokens).min(self.max_tokens);
        self.last_update = now;
    }

    /// Get remaining tokens (for header)
    fn remaining(&self) -> u32 {
        self.tokens as u32
    }

    /// Get time until next token is available (for header)
    fn reset_after(&self) -> Duration {
        if self.tokens >= 1.0 {
            Duration::ZERO
        } else {
            let tokens_needed = 1.0 - self.tokens;
            let seconds = tokens_needed / self.refill_rate;
            Duration::from_secs_f64(seconds)
        }
    }
}

// =============================================================================
// Rate Limiter
// =============================================================================

/// Rate limiter with per-IP tracking
#[derive(Debug)]
pub struct RateLimiter {
    config: RateLimitConfig,
    buckets: RwLock<HashMap<IpAddr, TokenBucket>>,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            buckets: RwLock::new(HashMap::new()),
        }
    }

    /// Check if a request from the given IP should be allowed
    pub async fn check(&self, ip: IpAddr) -> RateLimitResult {
        if !self.config.enabled {
            return RateLimitResult::Allowed {
                remaining: u32::MAX,
                reset_after: Duration::ZERO,
            };
        }

        let mut buckets = self.buckets.write().await;

        // Evict oldest entry if at capacity and this is a new IP
        if !buckets.contains_key(&ip) && buckets.len() >= self.config.max_tracked_ips {
            let oldest_ip = buckets
                .iter()
                .min_by_key(|(_, b)| b.last_update)
                .map(|(ip, _)| *ip);
            if let Some(ip_to_evict) = oldest_ip {
                buckets.remove(&ip_to_evict);
            }
        }

        let bucket = buckets.entry(ip).or_insert_with(|| {
            TokenBucket::new(self.config.burst_size, self.config.requests_per_second)
        });

        if bucket.try_consume() {
            RateLimitResult::Allowed {
                remaining: bucket.remaining(),
                reset_after: bucket.reset_after(),
            }
        } else {
            RateLimitResult::Limited {
                retry_after: bucket.reset_after(),
            }
        }
    }

    /// Clean up old buckets (call periodically)
    pub async fn cleanup(&self, max_age: Duration) {
        let now = Instant::now();
        let mut buckets = self.buckets.write().await;
        buckets.retain(|_, bucket| now.duration_since(bucket.last_update) < max_age);
    }

    /// Get number of tracked clients
    pub async fn client_count(&self) -> usize {
        self.buckets.read().await.len()
    }
}

/// Result of a rate limit check
#[derive(Debug, Clone)]
pub enum RateLimitResult {
    /// Request is allowed
    Allowed {
        /// Remaining requests in current window
        remaining: u32,
        /// Time until bucket is full
        reset_after: Duration,
    },
    /// Request is rate limited
    Limited {
        /// Time until next request is allowed
        retry_after: Duration,
    },
}

// =============================================================================
// Warp Filter
// =============================================================================

/// Warp rejection type for rate limiting
#[derive(Debug)]
pub struct RateLimitRejection {
    pub retry_after_secs: u64,
}

impl warp::reject::Reject for RateLimitRejection {}

/// Create a warp filter that applies rate limiting
pub fn with_rate_limit(
    limiter: Arc<RateLimiter>,
) -> impl warp::Filter<Extract = (RateLimitHeaders,), Error = warp::Rejection> + Clone {
    warp::addr::remote().and_then(move |addr: Option<std::net::SocketAddr>| {
        let limiter = limiter.clone();
        async move {
            // Get client IP (use localhost if not available)
            let ip = addr
                .map(|a| a.ip())
                .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));

            match limiter.check(ip).await {
                RateLimitResult::Allowed {
                    remaining,
                    reset_after,
                } => Ok(RateLimitHeaders {
                    remaining,
                    reset_after_secs: reset_after.as_secs(),
                }),
                RateLimitResult::Limited { retry_after } => {
                    Err(warp::reject::custom(RateLimitRejection {
                        retry_after_secs: retry_after.as_secs().max(1),
                    }))
                }
            }
        }
    })
}

/// Headers to include in response
#[derive(Debug, Clone)]
pub struct RateLimitHeaders {
    pub remaining: u32,
    pub reset_after_secs: u64,
}

/// Handle rate limit rejection in recovery
pub fn handle_rate_limit_rejection(
    rejection: &warp::Rejection,
) -> Option<warp::reply::WithStatus<warp::reply::Json>> {
    if let Some(r) = rejection.find::<RateLimitRejection>() {
        let json = warp::reply::json(&serde_json::json!({
            "error": "rate_limited",
            "message": "Too many requests",
            "retry_after_seconds": r.retry_after_secs,
        }));
        Some(warp::reply::with_status(
            json,
            warp::http::StatusCode::TOO_MANY_REQUESTS,
        ))
    } else {
        None
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_disabled() {
        let config = RateLimitConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_config_new() {
        let config = RateLimitConfig::new(100);
        assert!(config.enabled);
        assert_eq!(config.requests_per_second, 100);
        assert_eq!(config.burst_size, 200);
    }

    #[test]
    fn test_config_with_burst() {
        let config = RateLimitConfig::with_burst(100, 50);
        assert!(config.enabled);
        assert_eq!(config.requests_per_second, 100);
        assert_eq!(config.burst_size, 50);
    }

    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10, 10);
        assert_eq!(bucket.remaining(), 10);

        // Consume all tokens
        for _ in 0..10 {
            assert!(bucket.try_consume());
        }

        // Should be rate limited
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10, 100); // 100/sec refill

        // Consume all tokens
        for _ in 0..10 {
            bucket.try_consume();
        }

        // Manually set last_update to past
        bucket.last_update = Instant::now() - Duration::from_millis(100);

        // After 100ms at 100/sec, should have ~10 tokens
        bucket.refill();
        assert!(bucket.remaining() >= 9); // Allow some margin
    }

    #[tokio::test]
    async fn test_rate_limiter_disabled() {
        let config = RateLimitConfig::disabled();
        let limiter = RateLimiter::new(config);

        let ip = "127.0.0.1".parse().unwrap();
        match limiter.check(ip).await {
            RateLimitResult::Allowed { remaining, .. } => {
                assert_eq!(remaining, u32::MAX);
            }
            RateLimitResult::Limited { .. } => panic!("Should not be limited"),
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_allows_burst() {
        let config = RateLimitConfig::with_burst(10, 5); // 5 burst, 10/sec refill
        let limiter = RateLimiter::new(config);

        let ip = "127.0.0.1".parse().unwrap();

        // Should allow burst of 5
        for i in 0..5 {
            match limiter.check(ip).await {
                RateLimitResult::Allowed { remaining, .. } => {
                    assert_eq!(remaining, 4 - i);
                }
                RateLimitResult::Limited { .. } => panic!("Should not be limited at request {}", i),
            }
        }

        // 6th request should be limited
        match limiter.check(ip).await {
            RateLimitResult::Allowed { .. } => panic!("Should be limited"),
            RateLimitResult::Limited { retry_after } => {
                assert!(retry_after.as_millis() <= 100);
            }
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_per_ip() {
        let config = RateLimitConfig::with_burst(10, 2);
        let limiter = RateLimiter::new(config);

        let ip1: IpAddr = "127.0.0.1".parse().unwrap();
        let ip2: IpAddr = "127.0.0.2".parse().unwrap();

        // Exhaust ip1
        for _ in 0..2 {
            limiter.check(ip1).await;
        }

        // ip1 should be limited
        match limiter.check(ip1).await {
            RateLimitResult::Allowed { .. } => panic!("ip1 should be limited"),
            RateLimitResult::Limited { .. } => {}
        }

        // ip2 should still be allowed
        match limiter.check(ip2).await {
            RateLimitResult::Allowed { .. } => {}
            RateLimitResult::Limited { .. } => panic!("ip2 should not be limited"),
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_cleanup() {
        let config = RateLimitConfig::new(10);
        let limiter = RateLimiter::new(config);

        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        limiter.check(ip).await;

        assert_eq!(limiter.client_count().await, 1);

        // Cleanup with very short max age should remove the bucket
        limiter.cleanup(Duration::from_nanos(1)).await;
        assert_eq!(limiter.client_count().await, 0);
    }

    #[tokio::test]
    async fn test_rate_limiter_bounded() {
        let mut config = RateLimitConfig::new(10);
        config.max_tracked_ips = 3;
        let limiter = RateLimiter::new(config);

        // Add 3 IPs (at capacity)
        for i in 1..=3u8 {
            let ip: IpAddr = format!("10.0.0.{}", i).parse().unwrap();
            limiter.check(ip).await;
        }
        assert_eq!(limiter.client_count().await, 3);

        // Adding a 4th IP should evict the oldest
        let ip4: IpAddr = "10.0.0.4".parse().unwrap();
        limiter.check(ip4).await;
        assert_eq!(limiter.client_count().await, 3);
    }

    #[test]
    fn test_reset_after_calculation() {
        let mut bucket = TokenBucket::new(10, 10); // 10/sec

        // When full, reset_after should be 0
        assert_eq!(bucket.reset_after(), Duration::ZERO);

        // Exhaust tokens
        for _ in 0..10 {
            bucket.try_consume();
        }

        // reset_after should be ~100ms (need 1 token at 10/sec)
        let reset = bucket.reset_after();
        assert!(reset.as_millis() >= 90 && reset.as_millis() <= 110);
    }
}
