use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hyper::header::{HeaderValue, RETRY_AFTER};

use super::{HttpError, IntoMiddleware, Middleware, Next, Request, Response};

mod compression;
pub(crate) mod conditional;

pub use compression::{compression, compression_with_min_size, gzip};
pub use conditional::etag;
pub(crate) use conditional::{PreconditionResult, evaluate_preconditions};

static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);
const MAX_REQUEST_ID_BYTES: usize = 128;
const DEFAULT_RATE_LIMIT_CLIENTS: usize = 10_000;
const MAX_RATE_LIMIT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

pub fn cors() -> Middleware {
    Arc::new(|req: Request, next: Next| {
        Box::pin(async move {
            let mut res = next(req).await;
            res = res
                .header("access-control-allow-origin", "*")
                .header(
                    "access-control-allow-methods",
                    "GET,POST,PUT,PATCH,DELETE,OPTIONS,HEAD",
                )
                .header(
                    "access-control-allow-headers",
                    "content-type,authorization,x-request-id",
                );
            res
        })
    })
}

pub fn request_id() -> Middleware {
    Arc::new(|mut req: Request, next: Next| {
        Box::pin(async move {
            let supplied = req.headers_all("x-request-id");
            let id = match supplied.as_slice() {
                [value] if valid_request_id(value) => (*value).to_string(),
                [] => req
                    .header("x-request-id")
                    .filter(|value| valid_request_id(value))
                    .map(str::to_string)
                    .unwrap_or_else(generate_request_id),
                _ => generate_request_id(),
            };
            // The generated value is always a visible ASCII token.
            req.set_header("x-request-id", &id)
                .expect("the generated request id is a valid header value");
            let mut res = next(req).await;
            res = res.header("x-request-id", &id);
            res
        })
    })
}

fn valid_request_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REQUEST_ID_BYTES
        && value.bytes().all(|byte| matches!(byte, b'!'..=b'~'))
}

pub fn tracing() -> Middleware {
    Arc::new(|req: Request, next: Next| {
        Box::pin(async move {
            let method = req.method.clone();
            let path = req.path.clone();
            println!("--> {} {}", method, path);
            let res = next(req).await;
            println!("<-- {} {} ({})", method, path, res.status);
            res
        })
    })
}

/// Structured logging via the `tracing` crate (requires the `tracing`
/// feature): wraps each request in an info span (method + path) and emits an
/// event with the status and latency when it completes.
#[cfg(feature = "tracing")]
pub fn trace() -> Middleware {
    use tracing::Instrument;

    Arc::new(|req: Request, next: Next| {
        let span = tracing::info_span!("request", method = %req.method, path = %req.path);
        Box::pin(
            async move {
                let start = std::time::Instant::now();
                let res = next(req).await;
                tracing::info!(
                    status = res.status,
                    latency_ms = start.elapsed().as_millis() as u64,
                    "request served"
                );
                res
            }
            .instrument(span),
        )
    })
}

/// Cuts off everything it wraps (handler plus inner middleware) after
/// `duration`, answering `408 Request Timeout` (formatted by the app's error
/// handler when one is registered). Scope it per route or per router to give
/// slow endpoints their own budget alongside the global `request_timeout`.
pub fn timeout(duration: Duration) -> Middleware {
    Arc::new(move |req: Request, next: Next| {
        Box::pin(async move {
            match tokio::time::timeout(duration, next(req)).await {
                Ok(res) => res,
                Err(_) => Response::from_error(HttpError::request_timeout("Request Timeout")),
            }
        })
    })
}

/// Fixed-window rate limiting with the default bound of 10,000 tracked client
/// IPs. Use [`RateLimit`] when a different storage bound is required.
pub fn rate_limit(max_requests: u32, window: Duration) -> Middleware {
    RateLimit::new(max_requests, window).into_middleware()
}

#[derive(Clone, Copy)]
struct RateBucket {
    start: Instant,
    count: u32,
}

struct RateState {
    buckets: HashMap<Option<IpAddr>, RateBucket>,
    overflow: Option<RateBucket>,
    next_cleanup: Instant,
    #[cfg(test)]
    cleanup_sweeps: usize,
}

/// Bounded fixed-window, per-client-IP rate limiter.
///
/// Unknown clients share one overflow bucket after `max_clients` distinct
/// clients are tracked. Expired buckets are swept periodically; between
/// sweeps, new identities use the overflow bucket without scanning the map.
pub struct RateLimit {
    max_requests: u32,
    window: Duration,
    max_clients: usize,
}

impl RateLimit {
    pub fn new(max_requests: u32, window: Duration) -> Self {
        assert!(!window.is_zero(), "rate-limit window must be positive");
        Self {
            max_requests,
            window,
            max_clients: DEFAULT_RATE_LIMIT_CLIENTS,
        }
    }

    pub fn max_clients(mut self, max_clients: usize) -> Self {
        assert!(
            max_clients > 0,
            "rate-limit client capacity must be greater than zero"
        );
        self.max_clients = max_clients;
        self
    }
}

impl IntoMiddleware for RateLimit {
    fn into_middleware(self) -> Middleware {
        let cleanup_interval = self.window.min(MAX_RATE_LIMIT_CLEANUP_INTERVAL);
        let now = Instant::now();
        let state = Arc::new(Mutex::new(RateState {
            buckets: HashMap::new(),
            overflow: None,
            next_cleanup: now.checked_add(cleanup_interval).unwrap_or(now),
            #[cfg(test)]
            cleanup_sweeps: 0,
        }));
        let max_requests = self.max_requests;
        let window = self.window;
        let max_clients = self.max_clients;

        Arc::new(move |req: Request, next: Next| {
            let state = Arc::clone(&state);
            Box::pin(async move {
                let key = req.remote_addr().map(|addr| addr.ip());
                let now = Instant::now();
                let over_limit = {
                    let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
                    check_rate_limit_state(
                        &mut state,
                        key,
                        now,
                        cleanup_interval,
                        window,
                        max_clients,
                        max_requests,
                    )
                };

                match over_limit {
                    Some(remaining) => {
                        let retry_after = retry_after_seconds(remaining).to_string();
                        let retry_after =
                            HeaderValue::from_str(&retry_after).expect("retry seconds are valid");
                        Response::from_error(
                            HttpError::too_many_requests("Too Many Requests")
                                .header(RETRY_AFTER, retry_after),
                        )
                    }
                    None => next(req).await,
                }
            })
        })
    }
}

fn check_rate_limit_state(
    state: &mut RateState,
    key: Option<IpAddr>,
    now: Instant,
    cleanup_interval: Duration,
    window: Duration,
    max_clients: usize,
    max_requests: u32,
) -> Option<Duration> {
    if now >= state.next_cleanup {
        remove_expired_rate_buckets(state, now, window);
        state.next_cleanup = now.checked_add(cleanup_interval).unwrap_or(now);
    }

    let has_capacity = state.buckets.len() < max_clients;
    let bucket = match state.buckets.entry(key) {
        Entry::Occupied(entry) => entry.into_mut(),
        Entry::Vacant(entry) if has_capacity => entry.insert(RateBucket {
            start: now,
            count: 0,
        }),
        Entry::Vacant(_) => state.overflow.get_or_insert(RateBucket {
            start: now,
            count: 0,
        }),
    };
    check_rate_bucket(bucket, now, window, max_requests)
}

fn remove_expired_rate_buckets(state: &mut RateState, now: Instant, window: Duration) {
    #[cfg(test)]
    {
        state.cleanup_sweeps += 1;
    }
    state
        .buckets
        .retain(|_, bucket| now.duration_since(bucket.start) < window);
    if state
        .overflow
        .is_some_and(|bucket| now.duration_since(bucket.start) >= window)
    {
        state.overflow = None;
    }
}

fn check_rate_bucket(
    bucket: &mut RateBucket,
    now: Instant,
    window: Duration,
    max_requests: u32,
) -> Option<Duration> {
    let elapsed = now.duration_since(bucket.start);
    if elapsed >= window {
        bucket.start = now;
        bucket.count = 0;
    }
    bucket.count = bucket.count.saturating_add(1);
    (bucket.count > max_requests).then(|| {
        let elapsed = now.duration_since(bucket.start);
        window.saturating_sub(elapsed)
    })
}

fn retry_after_seconds(remaining: Duration) -> u64 {
    remaining
        .as_secs()
        .saturating_add(u64::from(remaining.subsec_nanos() != 0))
        .max(1)
}

#[cfg(test)]
mod rate_limit_tests {
    use std::net::{IpAddr, Ipv4Addr};

    use super::*;

    #[test]
    fn capacity_churn_only_sweeps_on_the_cleanup_interval() {
        let start = Instant::now();
        let window = Duration::from_secs(60);
        let cleanup_interval = window;
        let mut state = RateState {
            buckets: HashMap::new(),
            overflow: None,
            next_cleanup: start + cleanup_interval,
            cleanup_sweeps: 0,
        };

        let tracked = Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)));
        assert_eq!(
            check_rate_limit_state(
                &mut state,
                tracked,
                start,
                cleanup_interval,
                window,
                1,
                u32::MAX,
            ),
            None
        );

        // An attacker can present arbitrarily many identities, but between
        // cleanup ticks each lookup remains bounded and shares one overflow
        // bucket instead of sweeping every tracked client.
        for identity in 2..=10_000_u32 {
            let key = Some(IpAddr::V4(Ipv4Addr::from(identity)));
            assert_eq!(
                check_rate_limit_state(
                    &mut state,
                    key,
                    start + Duration::from_secs(1),
                    cleanup_interval,
                    window,
                    1,
                    u32::MAX,
                ),
                None
            );
        }
        assert_eq!(state.buckets.len(), 1);
        assert!(state.overflow.is_some());
        assert_eq!(state.cleanup_sweeps, 0);

        // The scheduled pass reclaims both expired tracked and overflow
        // buckets, after which the triggering identity can be tracked.
        let replacement = Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)));
        assert_eq!(
            check_rate_limit_state(
                &mut state,
                replacement,
                start + window + Duration::from_secs(1),
                cleanup_interval,
                window,
                1,
                u32::MAX,
            ),
            None
        );
        assert_eq!(state.cleanup_sweeps, 1);
        assert_eq!(state.buckets.len(), 1);
        assert!(state.buckets.contains_key(&replacement));
        assert!(state.overflow.is_none());
    }
}

/// Configurable CORS: origin allowlist (or any origin), credentials, methods,
/// headers, and max-age, with automatic preflight handling. Register with
/// `app.layer(Cors::new().allow_origin("https://app.example.com"))`.
///
/// Requests without an `Origin` header pass through untouched. Preflights
/// (`OPTIONS` + `Access-Control-Request-Method`) are answered directly with
/// `204` and never reach the router.
pub struct Cors {
    any_origin: bool,
    origins: Vec<String>,
    methods: String,
    headers: Option<String>,
    credentials: bool,
    max_age_secs: Option<u64>,
}

impl Cors {
    pub fn new() -> Self {
        Self {
            any_origin: false,
            origins: Vec::new(),
            methods: "GET, POST, PUT, PATCH, DELETE, OPTIONS, HEAD".to_string(),
            headers: None,
            credentials: false,
            max_age_secs: None,
        }
    }

    /// Allows any origin. With credentials enabled the request origin is
    /// echoed back (the spec forbids `*` together with credentials).
    pub fn allow_any_origin(mut self) -> Self {
        self.any_origin = true;
        self
    }

    /// Adds an origin to the allowlist (repeatable).
    pub fn allow_origin(mut self, origin: &str) -> Self {
        self.origins.push(origin.to_string());
        self
    }

    pub fn allow_methods(mut self, methods: &[&str]) -> Self {
        self.methods = methods.join(", ");
        self
    }

    /// Sets the allowed request headers. When not set, preflights echo the
    /// headers the client asked for.
    pub fn allow_headers(mut self, headers: &[&str]) -> Self {
        self.headers = Some(headers.join(", "));
        self
    }

    pub fn allow_credentials(mut self, allow: bool) -> Self {
        self.credentials = allow;
        self
    }

    pub fn max_age_secs(mut self, seconds: u64) -> Self {
        self.max_age_secs = Some(seconds);
        self
    }

    /// Returns the `Access-Control-Allow-Origin` value to grant, if any.
    fn grant_for(&self, origin: &str) -> Option<String> {
        if self.any_origin {
            if self.credentials {
                Some(origin.to_string())
            } else {
                Some("*".to_string())
            }
        } else if self.origins.iter().any(|allowed| allowed == origin) {
            Some(origin.to_string())
        } else {
            None
        }
    }

    fn apply_grant(&self, res: Response, allowed: &str) -> Response {
        let mut res = res.header("access-control-allow-origin", allowed);
        if self.credentials {
            res = res.header("access-control-allow-credentials", "true");
        }
        res
    }

    fn varies_by_origin(&self) -> bool {
        !self.any_origin || self.credentials
    }
}

impl Default for Cors {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoMiddleware for Cors {
    fn into_middleware(self) -> Middleware {
        let cors = Arc::new(self);
        Arc::new(move |req: Request, next: Next| {
            let cors = Arc::clone(&cors);
            Box::pin(async move {
                let origin = match req.singleton_header("origin") {
                    Ok(Some(origin)) => origin.to_string(),
                    Ok(None) => return next(req).await,
                    Err(error) => return Response::from_error(error),
                };
                let requested_method = match req.singleton_header("access-control-request-method") {
                    Ok(method) => method,
                    Err(error) => return Response::from_error(error),
                };
                let grant = cors.grant_for(&origin);

                let is_preflight = req.method == "OPTIONS" && requested_method.is_some();
                if is_preflight {
                    let requested_headers = {
                        let values = req.headers_all("access-control-request-headers");
                        if values.is_empty() {
                            req.header("access-control-request-headers")
                                .map(str::to_string)
                        } else {
                            Some(values.join(", "))
                        }
                    };
                    // Preflight output depends on the origin and requested
                    // method/headers, including when an origin is denied.
                    let mut res = Response::send("").status(204).header(
                        "vary",
                        "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
                    );
                    if let Some(allowed) = &grant {
                        let headers = cors.headers.clone().or(requested_headers);
                        res = res.header("access-control-allow-methods", &cors.methods);
                        if let Some(headers) = headers {
                            res = res.header("access-control-allow-headers", &headers);
                        }
                        if let Some(age) = cors.max_age_secs {
                            res = res.header("access-control-max-age", &age.to_string());
                        }
                        res = cors.apply_grant(res, allowed);
                    }
                    return res;
                };

                let mut res = next(req).await;
                if cors.varies_by_origin() {
                    res = res.append_header("vary", "Origin");
                }
                match grant {
                    Some(allowed) => cors.apply_grant(res, &allowed),
                    None => res,
                }
            })
        })
    }
}

fn generate_request_id() -> String {
    let count = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("req-{}-{}", nanos, count)
}

#[cfg(test)]
mod tests {
    use super::retry_after_seconds;
    use std::time::Duration;

    #[test]
    fn retry_after_rounds_fractional_seconds_up() {
        assert_eq!(retry_after_seconds(Duration::from_millis(1)), 1);
        assert_eq!(retry_after_seconds(Duration::from_millis(1_001)), 2);
        assert_eq!(retry_after_seconds(Duration::from_secs(2)), 2);
    }
}
