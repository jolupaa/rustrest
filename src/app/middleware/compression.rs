use std::io::Write;
use std::sync::{Arc, OnceLock};

use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use hyper::StatusCode;
use hyper::body::Bytes;
use hyper::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE,
    ETAG, HeaderValue, VARY,
};
use tokio::sync::Semaphore;

use crate::app::{HttpError, Middleware, Next, Request, Response};

/// Bodies smaller than this are not worth compressing.
const COMPRESSION_MIN_BYTES: usize = 1024;
const BLOCKING_COMPRESSION_THRESHOLD: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Encoding {
    Gzip,
    #[cfg(feature = "brotli")]
    Brotli,
    Deflate,
}

impl Encoding {
    fn name(self) -> &'static str {
        match self {
            Encoding::Gzip => "gzip",
            #[cfg(feature = "brotli")]
            Encoding::Brotli => "br",
            Encoding::Deflate => "deflate",
        }
    }

    fn server_preference(self) -> usize {
        match self {
            Encoding::Gzip => 0,
            #[cfg(feature = "brotli")]
            Encoding::Brotli => 1,
            Encoding::Deflate => 2,
        }
    }

    fn encode(self, body: &[u8]) -> Result<Vec<u8>, HttpError> {
        let failed = |err: std::io::Error| {
            HttpError::internal_server_error("No se pudo comprimir la respuesta").with_source(err)
        };
        match self {
            Encoding::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body).map_err(failed)?;
                encoder.finish().map_err(failed)
            }
            #[cfg(feature = "brotli")]
            Encoding::Brotli => {
                let mut writer = brotli::CompressorWriter::new(Vec::new(), 4096, 5, 22);
                writer.write_all(body).map_err(failed)?;
                writer.flush().map_err(failed)?;
                Ok(writer.into_inner())
            }
            Encoding::Deflate => {
                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body).map_err(failed)?;
                encoder.finish().map_err(failed)
            }
        }
    }
}

#[derive(Clone, Copy)]
struct CodingPreference {
    coding: Encoding,
    quality: u16,
    client_order: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Negotiation {
    Encode {
        encoding: Encoding,
        identity_acceptable: bool,
    },
    Identity,
    NotAcceptable,
}

/// Legacy gzip-only middleware. Prefer [`compression`] for full negotiation.
pub fn gzip() -> Middleware {
    compression_with_min_size_and_supported(0, &[Encoding::Gzip])
}

/// Content negotiation for response compression with a 1 KB minimum size.
/// Supports gzip and deflate (plus brotli with the `brotli` feature).
pub fn compression() -> Middleware {
    compression_with_min_size(COMPRESSION_MIN_BYTES)
}

/// Like [`compression`], with a custom minimum body size.
pub fn compression_with_min_size(min_size: usize) -> Middleware {
    compression_with_min_size_and_supported(min_size, supported_encodings())
}

fn compression_with_min_size_and_supported(
    min_size: usize,
    supported: &'static [Encoding],
) -> Middleware {
    Arc::new(move |req: Request, next: Next| {
        // Accept-Encoding is a list-valued field, and HTTP permits a sender
        // or intermediary to split that list across repeated field lines.
        let accept_encoding = combined_request_header(&req, "accept-encoding");
        let negotiation = accept_encoding
            .as_deref()
            .map(|header| negotiate_encoding(header, supported));
        Box::pin(async move {
            let mut res = next(req).await;

            // These statuses never carry a representation to negotiate.
            if status_has_no_representation(res.status) {
                return res;
            }

            // A handler that already selected a content coding owns that
            // representation. It still has to be acceptable to this client,
            // and the result must be keyed by Accept-Encoding for caches.
            if let Some(content_encoding) = combined_content_encoding(&res) {
                ensure_accept_encoding_vary(&mut res);
                if !preencoded_is_acceptable(
                    accept_encoding.as_deref(),
                    content_encoding.as_deref(),
                ) {
                    return not_acceptable_response();
                }
                return res;
            }

            let transform_skipped = should_skip_transform(&res, min_size);
            ensure_accept_encoding_vary(&mut res);

            match negotiation {
                Some(Negotiation::Encode {
                    encoding,
                    identity_acceptable,
                }) => {
                    if transform_skipped {
                        return if identity_acceptable {
                            res
                        } else {
                            not_acceptable_response()
                        };
                    }

                    match encode_response_body(&mut res, encoding).await {
                        Ok(body) => {
                            let replaced = res.replace_body_bytes(Bytes::from(body));
                            debug_assert!(replaced, "only buffered bodies reach compression");
                            remove_stale_representation_headers(&mut res);
                            res = res.header(CONTENT_ENCODING.as_str(), encoding.name());
                            res
                        }
                        Err(error) => Response::from_error(error),
                    }
                }
                Some(Negotiation::NotAcceptable) => not_acceptable_response(),
                Some(Negotiation::Identity) | None => res,
            }
        })
    })
}

async fn encode_response_body(
    res: &mut Response,
    encoding: Encoding,
) -> Result<Vec<u8>, HttpError> {
    let body_len = res
        .body_bytes()
        .expect("only buffered bodies reach compression")
        .len();
    if body_len < BLOCKING_COMPRESSION_THRESHOLD {
        return encoding.encode(
            res.body_bytes()
                .expect("only buffered bodies reach compression"),
        );
    }

    // Compression is CPU-bound. Bound the amount of work admitted to Tokio's
    // blocking pool so a burst of large responses cannot create hundreds of
    // concurrent encoders and their associated buffers.
    let permit = Arc::clone(compression_permits())
        .acquire_owned()
        .await
        .map_err(|error| {
            HttpError::internal_server_error("No se pudo programar la compresion")
                .with_source(error)
        })?;
    let body = res
        .body_bytes()
        .expect("only buffered bodies reach compression")
        .to_vec();
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        encoding.encode(&body)
    })
    .await
    .map_err(|error| {
        HttpError::internal_server_error("No se pudo completar la compresion").with_source(error)
    })?
}

fn compression_permits() -> &'static Arc<Semaphore> {
    static PERMITS: OnceLock<Arc<Semaphore>> = OnceLock::new();
    PERMITS.get_or_init(|| {
        let parallelism = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1);
        Arc::new(Semaphore::new(parallelism.saturating_mul(2).max(1)))
    })
}

fn preencoded_is_acceptable(accept_encoding: Option<&str>, content_encoding: Option<&str>) -> bool {
    let Some(content_encoding) = content_encoding else {
        return false;
    };
    if !content_encoding
        .split(',')
        .map(str::trim)
        .any(|coding| !coding.is_empty())
    {
        return false;
    }
    let Some(accept_encoding) = accept_encoding else {
        return true;
    };
    let tokens: Vec<_> = accept_encoding
        .split(',')
        .enumerate()
        .filter_map(|(index, token)| parse_accept_encoding_token(index, token))
        .collect();
    let wildcard_quality = tokens
        .iter()
        .find(|token| token.name == "*")
        .map(|token| token.quality);

    let mut saw_coding = false;
    for coding in content_encoding
        .split(',')
        .map(str::trim)
        .filter(|coding| !coding.is_empty())
    {
        saw_coding = true;
        let acceptable = if coding.eq_ignore_ascii_case("identity") {
            tokens
                .iter()
                .find(|token| token.name == "identity")
                .map(|token| token.quality)
                .unwrap_or_else(|| if wildcard_quality == Some(0) { 0 } else { 1000 })
                > 0
        } else {
            tokens
                .iter()
                .find(|token| token.name.eq_ignore_ascii_case(coding))
                .map(|token| token.quality)
                .or(wildcard_quality)
                .unwrap_or(0)
                > 0
        };
        if !acceptable {
            return false;
        }
    }
    saw_coding
}

/// Combines every Content-Encoding field line in wire order. `None` means the
/// field is absent; `Some(None)` means at least one value cannot be represented
/// by the framework's string-based negotiation logic and must fail closed.
fn combined_content_encoding(res: &Response) -> Option<Option<String>> {
    let mut combined = String::new();
    let mut present = false;
    for value in res.headers.get_all(CONTENT_ENCODING).iter() {
        present = true;
        let Ok(value) = value.to_str() else {
            return Some(None);
        };
        if !combined.is_empty() {
            combined.push(',');
        }
        combined.push_str(value);
    }
    present.then_some(Some(combined))
}

fn remove_stale_representation_headers(res: &mut Response) {
    for name in [
        CONTENT_LENGTH.as_str(),
        ACCEPT_RANGES.as_str(),
        CONTENT_RANGE.as_str(),
        ETAG.as_str(),
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
    ] {
        res.headers.remove(name);
    }
}

fn supported_encodings() -> &'static [Encoding] {
    &[
        Encoding::Gzip,
        #[cfg(feature = "brotli")]
        Encoding::Brotli,
        Encoding::Deflate,
    ]
}

fn negotiate_encoding(header: &str, supported: &'static [Encoding]) -> Negotiation {
    let tokens: Vec<_> = header
        .split(',')
        .enumerate()
        .filter_map(|(index, token)| parse_accept_encoding_token(index, token))
        .collect();

    let wildcard_quality = tokens
        .iter()
        .find(|token| token.name == "*")
        .map(|token| token.quality);
    let explicit_identity_quality = tokens
        .iter()
        .find(|token| token.name == "identity")
        .map(|token| token.quality);
    let identity_quality = explicit_identity_quality.unwrap_or_else(|| {
        // `*` only excludes identity when its quality is zero. Otherwise,
        // an uncoded representation remains acceptable by default, but
        // that default does not express a preference over listed codings.
        if wildcard_quality == Some(0) { 0 } else { 1000 }
    });

    let mut preferences = Vec::new();
    for &coding in supported {
        let name = coding.name();
        let explicit = tokens.iter().find(|token| token.name == name);
        let quality = explicit
            .map(|token| token.quality)
            .or(wildcard_quality)
            .unwrap_or(0);
        let client_order = explicit
            .map(|token| token.client_order)
            .or_else(|| {
                wildcard_quality.and_then(|_| {
                    tokens
                        .iter()
                        .find(|token| token.name == "*")
                        .map(|token| token.client_order)
                })
            })
            .unwrap_or(usize::MAX);
        if quality > 0 {
            preferences.push(CodingPreference {
                coding,
                quality,
                client_order,
            });
        }
    }

    let Some(best) = preferences.into_iter().max_by(|left, right| {
        left.quality
            .cmp(&right.quality)
            .then_with(|| {
                right
                    .coding
                    .server_preference()
                    .cmp(&left.coding.server_preference())
            })
            .then_with(|| right.client_order.cmp(&left.client_order))
    }) else {
        return if identity_quality == 0 {
            Negotiation::NotAcceptable
        } else {
            Negotiation::Identity
        };
    };

    if explicit_identity_quality.is_some_and(|quality| quality > best.quality) {
        Negotiation::Identity
    } else {
        Negotiation::Encode {
            encoding: best.coding,
            identity_acceptable: identity_quality > 0,
        }
    }
}

struct AcceptEncodingToken {
    name: String,
    quality: u16,
    client_order: usize,
}

fn parse_accept_encoding_token(client_order: usize, raw: &str) -> Option<AcceptEncodingToken> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }
    let mut parts = raw.split(';');
    let name = parts.next()?.trim().to_ascii_lowercase();
    if name.is_empty() {
        return None;
    }
    let mut quality = 1000;
    for param in parts {
        let Some((key, value)) = param.trim().split_once('=') else {
            continue;
        };
        if key.trim().eq_ignore_ascii_case("q") {
            quality = parse_quality(value.trim()).unwrap_or(0);
        }
    }
    Some(AcceptEncodingToken {
        name,
        quality,
        client_order,
    })
}

fn parse_quality(raw: &str) -> Option<u16> {
    let (whole, fraction) = raw.split_once('.').unwrap_or((raw, ""));
    match whole {
        "0" => {
            if fraction.len() > 3 || !fraction.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            let mut padded = fraction.to_string();
            while padded.len() < 3 {
                padded.push('0');
            }
            padded.parse().ok()
        }
        "1" => {
            if fraction.bytes().any(|byte| byte != b'0') || fraction.len() > 3 {
                None
            } else {
                Some(1000)
            }
        }
        _ => None,
    }
}

fn status_has_no_representation(status: u16) -> bool {
    (100..200).contains(&status) || matches!(status, 204 | 205 | 304)
}

fn should_skip_transform(res: &Response, min_size: usize) -> bool {
    res.status == 206
        // Transforming the bytes while retaining application trailers would
        // leave integrity metadata describing a different representation.
        || res.has_trailers()
        || res
            .headers
            .get_all(CACHE_CONTROL)
            .iter()
            .any(|value| value.to_str().map(has_no_transform).unwrap_or(true))
        || res
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .or(Some(res.content_type.as_str()))
            .is_some_and(|value| value.to_ascii_lowercase().starts_with("image/"))
        || res.body_bytes().is_none_or(|body| body.len() < min_size)
}

fn ensure_accept_encoding_vary(res: &mut Response) {
    let mut fields = Vec::<String>::new();
    let mut saw_wildcard = false;
    let existing: Vec<_> = res.headers.get_all(VARY).iter().cloned().collect();

    for value in existing {
        let Ok(value) = value.to_str() else {
            // Keeping the malformed field and appending another value would
            // leave downstream caches with an ambiguous key. `*` safely
            // prevents reuse when the original value cannot be interpreted.
            res.headers.remove(VARY);
            res.headers.insert(VARY, HeaderValue::from_static("*"));
            return;
        };
        for field in value
            .split(',')
            .map(str::trim)
            .filter(|field| !field.is_empty())
        {
            if field == "*" {
                saw_wildcard = true;
                continue;
            }
            let canonical = if field.eq_ignore_ascii_case("accept-encoding") {
                "Accept-Encoding"
            } else {
                field
            };
            if !fields
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(canonical))
            {
                fields.push(canonical.to_string());
            }
        }
    }

    if saw_wildcard {
        res.headers.insert(VARY, HeaderValue::from_static("*"));
        return;
    }
    if !fields
        .iter()
        .any(|field| field.eq_ignore_ascii_case("accept-encoding"))
    {
        fields.push("Accept-Encoding".to_string());
    }

    match HeaderValue::from_str(&fields.join(", ")) {
        Ok(combined) => {
            res.headers.insert(VARY, combined);
        }
        Err(_) => {
            // A malformed handler-provided Vary value must never panic the
            // server or produce an unsafe cache key.
            res.headers.insert(VARY, HeaderValue::from_static("*"));
        }
    }
}

fn not_acceptable_response() -> Response {
    Response::from_error(
        HttpError::new(
            StatusCode::NOT_ACCEPTABLE,
            "not_acceptable",
            "No hay una codificacion de respuesta aceptable",
        )
        .header(VARY, HeaderValue::from_static("Accept-Encoding")),
    )
}

fn has_no_transform(cache_control: &str) -> bool {
    cache_control
        .split(',')
        .any(|token| token.trim().eq_ignore_ascii_case("no-transform"))
}

fn combined_request_header(req: &Request, name: &str) -> Option<String> {
    let values = req.headers_all(name);
    if values.is_empty() {
        req.header(name).map(str::to_string)
    } else {
        Some(values.join(","))
    }
}
