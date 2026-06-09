use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use hyper::header::{CONTENT_ENCODING, VARY};

use super::{HttpError, Middleware, Next, Request};

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

            if !accepts_gzip || res.headers.contains_key(CONTENT_ENCODING) {
                return res;
            }

            if res
                .map_body_bytes(|body| {
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                    encoder.write_all(body).map_err(|err| {
                        HttpError::internal_server_error(format!(
                            "No se pudo comprimir la respuesta: {}",
                            err
                        ))
                    })?;
                    encoder.finish().map_err(|err| {
                        HttpError::internal_server_error(format!(
                            "No se pudo finalizar gzip: {}",
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

fn generate_request_id() -> String {
    let count = REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("req-{}-{}", nanos, count)
}
