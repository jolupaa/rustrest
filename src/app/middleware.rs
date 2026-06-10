use std::collections::HashMap;
use std::io::Write;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use hyper::header::{CONTENT_ENCODING, VARY};

use super::{HttpError, IntoMiddleware, Middleware, Next, Request, Response};

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

pub fn gzip() -> Middleware {
    Arc::new(|req: Request, next: Next| {
        Box::pin(async move {
            let accepts_gzip = req
                .header("accept-encoding")
                .is_some_and(|value| value.split(',').any(|part| part.trim() == "gzip"));
            let mut res = next(req).await;

            if !accepts_gzip
                || res.status == 101
                || res.body_bytes().is_none_or(<[u8]>::is_empty)
                || res.headers.contains_key(CONTENT_ENCODING)
            {
                return res;
            }

            if res
                .map_body_bytes(|body| {
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(body).map_err(|err| {
                        HttpError::internal_server_error(format!(
                            "Could not compress response: {}",
                            err
                        ))
                    })?;
                    encoder.finish().map_err(|err| {
                        HttpError::internal_server_error(format!(
                            "Could not finish gzip encoding: {}",
                            err
                        ))
                    })
                })
                .is_ok()
            {
                res = res
                    .header(CONTENT_ENCODING.as_str(), "gzip")
                    .append_header(VARY.as_str(), "Accept-Encoding");
            }
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

/// Bodies smaller than this are not worth compressing.
const COMPRESSION_MIN_BYTES: usize = 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Encoding {
    #[cfg(feature = "brotli")]
    Brotli,
    Gzip,
    Deflate,
}

impl Encoding {
    fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "brotli")]
            Encoding::Brotli => "br",
            Encoding::Gzip => "gzip",
            Encoding::Deflate => "deflate",
        }
    }

    fn encode(self, body: &[u8]) -> Result<Vec<u8>, HttpError> {
        let failed =
            |err: std::io::Error| HttpError::internal_server_error(format!("Compression: {err}"));
        match self {
            #[cfg(feature = "brotli")]
            Encoding::Brotli => {
                let mut writer = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
                writer.write_all(body).map_err(failed)?;
                writer.flush().map_err(failed)?;
                Ok(writer.into_inner())
            }
            Encoding::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body).map_err(failed)?;
                encoder.finish().map_err(failed)
            }
            Encoding::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body).map_err(failed)?;
                encoder.finish().map_err(failed)
            }
        }
    }
}

/// Picks the best supported encoding from an `Accept-Encoding` header
/// (brotli, when the feature is on, then gzip, then deflate). `q=0` refuses.
fn choose_encoding(accept_encoding: &str) -> Option<Encoding> {
    #[cfg(feature = "brotli")]
    let mut brotli_ok = false;
    let mut gzip_ok = false;
    let mut deflate_ok = false;

    for part in accept_encoding.split(',') {
        let token = part.trim();
        let (name, params) = token.split_once(';').unwrap_or((token, ""));
        if params.replace(' ', "").eq_ignore_ascii_case("q=0") {
            continue;
        }
        match name.trim().to_ascii_lowercase().as_str() {
            #[cfg(feature = "brotli")]
            "br" => brotli_ok = true,
            "gzip" => gzip_ok = true,
            "deflate" => deflate_ok = true,
            _ => {}
        }
    }

    #[cfg(feature = "brotli")]
    if brotli_ok {
        return Some(Encoding::Brotli);
    }
    if gzip_ok {
        Some(Encoding::Gzip)
    } else if deflate_ok {
        Some(Encoding::Deflate)
    } else {
        None
    }
}

/// Content negotiation for response compression with a 1 KB minimum size.
/// Supports gzip and deflate (plus brotli with the `brotli` feature).
pub fn compression() -> Middleware {
    compression_with_min_size(COMPRESSION_MIN_BYTES)
}

/// Like [`compression`], with a custom minimum body size.
pub fn compression_with_min_size(min_size: usize) -> Middleware {
    Arc::new(move |req: Request, next: Next| {
        let encoding = req.header("accept-encoding").and_then(choose_encoding);
        Box::pin(async move {
            let mut res = next(req).await;
            let Some(encoding) = encoding else {
                return res;
            };
            if res.status == 101
                || res.headers.contains_key(CONTENT_ENCODING)
                || res.body_bytes().is_none_or(|body| body.len() < min_size)
            {
                return res;
            }
            if res.map_body_bytes(|body| encoding.encode(body)).is_ok() {
                res = res
                    .header(CONTENT_ENCODING.as_str(), encoding.name())
                    .append_header(VARY.as_str(), "Accept-Encoding");
            }
            res
        })
    })
}

/// Strong-ETag validation for buffered 200 responses: hashes the body
/// (SHA-1), sets `ETag` when the handler did not set one, and answers a
/// matching `If-None-Match` on GET/HEAD with `304 Not Modified` (headers
/// kept, body dropped). Streaming bodies and non-200 responses pass through
/// untouched.
pub fn etag() -> Middleware {
    Arc::new(|req: Request, next: Next| {
        let if_none_match = matches!(req.method.as_str(), "GET" | "HEAD")
            .then(|| req.header("if-none-match").map(str::to_string))
            .flatten();
        Box::pin(async move {
            let mut res = next(req).await;
            if res.status != 200 {
                return res;
            }
            let tag = match res
                .headers
                .get("etag")
                .and_then(|value| value.to_str().ok())
            {
                Some(existing) => existing.to_string(),
                None => {
                    let Some(body) = res.body_bytes() else {
                        return res;
                    };
                    if body.is_empty() {
                        return res;
                    }
                    let tag = format!("\"{}\"", sha1_hex(body));
                    res = res.header("etag", &tag);
                    tag
                }
            };
            let revalidated = if_none_match.is_some_and(|raw| {
                raw.split(',').any(|candidate| {
                    let candidate = candidate.trim().trim_start_matches("W/");
                    candidate == "*" || candidate == tag
                })
            });
            if revalidated {
                res.clear_body();
                res = res.status(304);
            }
            res
        })
    })
}

fn sha1_hex(data: &[u8]) -> String {
    use std::fmt::Write as _;

    use sha1::{Digest, Sha1};
    let digest = Sha1::digest(data);
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        let _ = write!(out, "{:02x}", byte);
    }
    out
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
                Err(_) => Response::from_error(HttpError::new(408, "Request Timeout")),
            }
        })
    })
}

/// Fixed-window, per-client-IP rate limiting: at most `max_requests` per
/// `window` from one IP (requests without a peer address — e.g. from the test
/// client — share a single bucket). Over the limit the middleware
/// short-circuits with `429 Too Many Requests` and a `Retry-After` header in
/// seconds. The 429 is returned directly rather than through the error
/// handler so `Retry-After` is always preserved.
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
                Some(remaining) => Response::send("Too Many Requests")
                    .status(429)
                    .header("retry-after", &remaining.as_secs().max(1).to_string()),
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
