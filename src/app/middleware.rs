use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
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
