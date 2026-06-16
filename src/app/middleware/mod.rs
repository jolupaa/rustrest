use std::collections::HashMap;
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
            let id = req
                .header("x-request-id")
                .map(str::to_string)
                .unwrap_or_else(generate_request_id);
            req.headers.insert("x-request-id".to_string(), id.clone());
            let mut res = next(req).await;
            res = res.header("x-request-id", &id);
            res
        })
    })
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

/// Fixed-window, per-client-IP rate limiting: at most `max_requests` per
/// `window` from one IP (requests without a peer address — e.g. from the test
/// client — share a single bucket). Over the limit the middleware
/// short-circuits with `429 Too Many Requests` and a `Retry-After` header in
/// seconds. The structured error still flows through a registered global
/// error handler, while its `Retry-After` header is preserved.
pub fn rate_limit(max_requests: u32, window: Duration) -> Middleware {
    /// Per-client window state: window start and requests seen in it. The
    /// `None` key holds clients with no known peer address.
    type RateBuckets = HashMap<Option<IpAddr>, (Instant, u32)>;
    let buckets: Arc<Mutex<RateBuckets>> = Arc::new(Mutex::new(HashMap::new()));
    Arc::new(move |req: Request, next: Next| {
        let buckets = Arc::clone(&buckets);
        Box::pin(async move {
            let key = req.remote_addr().map(|addr| addr.ip());
            let now = Instant::now();
            let over_limit = {
                let mut buckets = buckets.lock().expect("rate limit lock");
                // Expired windows are dropped wholesale so the map only ever
                // holds clients seen within the current window.
                buckets.retain(|_, (start, _)| now.duration_since(*start) < window);
                let (start, count) = buckets.entry(key).or_insert((now, 0));
                *count += 1;
                (*count > max_requests).then(|| window.saturating_sub(now.duration_since(*start)))
            };
            match over_limit {
                Some(remaining) => {
                    let retry_after = remaining.as_secs().max(1).to_string();
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
        if allowed != "*" {
            res = res.append_header("vary", "Origin");
        }
        res
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
                let Some(origin) = req.header("origin").map(str::to_string) else {
                    return next(req).await;
                };
                let grant = cors.grant_for(&origin);

                let is_preflight = req.method == "OPTIONS"
                    && req.header("access-control-request-method").is_some();
                if is_preflight {
                    let mut res = Response::send("").status(204);
                    if let Some(allowed) = &grant {
                        let headers = cors.headers.clone().or_else(|| {
                            req.header("access-control-request-headers")
                                .map(str::to_string)
                        });
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
                }

                let res = next(req).await;
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
