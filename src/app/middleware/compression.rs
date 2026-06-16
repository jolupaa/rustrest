use std::io::Write;
use std::sync::Arc;

use flate2::Compression;
use flate2::write::{GzEncoder, ZlibEncoder};
use hyper::StatusCode;
use hyper::header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, VARY};

use crate::app::{HttpError, Middleware, Next, Request, Response};

/// Bodies smaller than this are not worth compressing.
const COMPRESSION_MIN_BYTES: usize = 1024;

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
            HttpError::internal_server_error("Could not compress response").with_source(err)
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
    Encode(Encoding),
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
        let negotiation = req
            .header("accept-encoding")
            .map(|header| negotiate_encoding(header, supported));
        Box::pin(async move {
            let mut res = next(req).await;
            match negotiation {
                Some(Negotiation::Encode(encoding)) => {
                    if should_skip(&res, min_size) {
                        return res;
                    }
                    if res.map_body_bytes(|body| encoding.encode(body)).is_ok() {
                        res = res
                            .header(CONTENT_ENCODING.as_str(), encoding.name())
                            .append_header(VARY.as_str(), "Accept-Encoding");
                    }
                    res
                }
                Some(Negotiation::NotAcceptable) => Response::from_error(HttpError::new(
                    StatusCode::NOT_ACCEPTABLE,
                    "not_acceptable",
                    "No hay una codificacion de respuesta aceptable",
                )),
                Some(Negotiation::Identity) | None => res,
            }
        })
    })
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
    let identity_quality = tokens
        .iter()
        .find(|token| token.name == "identity")
        .map(|token| token.quality)
        .or(wildcard_quality)
        .unwrap_or(1000);

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

    Negotiation::Encode(best.coding)
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

fn should_skip(res: &Response, min_size: usize) -> bool {
    matches!(res.status, 101 | 204 | 206 | 304)
        || res.headers.contains_key(CONTENT_ENCODING)
        || res
            .headers
            .get(CACHE_CONTROL)
            .and_then(|value| value.to_str().ok())
            .is_some_and(has_no_transform)
        || res
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .or(Some(res.content_type.as_str()))
            .is_some_and(|value| value.to_ascii_lowercase().starts_with("image/"))
        || res.body_bytes().is_none_or(|body| body.len() < min_size)
}

fn has_no_transform(cache_control: &str) -> bool {
    cache_control
        .split(',')
        .any(|token| token.trim().eq_ignore_ascii_case("no-transform"))
}
