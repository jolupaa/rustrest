use std::convert::Infallible;
use std::fs;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::read::{GzDecoder, ZlibDecoder};
use futures_util::stream;
use hyper::body::Bytes;
use hyper::header::{
    ACCEPT_RANGES, CACHE_CONTROL, CONTENT_ENCODING, CONTENT_LENGTH, ETAG, HeaderMap, HeaderValue,
    VARY,
};
use rustrest::{App, Request, Response, TestClient, middleware};

fn semantics_temp_dir(name: &str) -> std::path::PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    std::env::temp_dir().join(format!(
        "rustrest-{name}-{}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn compression_client() -> TestClient {
    let mut app = App::new();
    app.layer(middleware::compression_with_min_size(0));
    let body = "x".repeat(2048);
    let _ = app.get("/data", move |_req: Request| Response::send(&body));
    TestClient::new(app)
}

fn decode_body(encoding: &str, body: &[u8]) -> String {
    let mut decoded = String::new();
    match encoding {
        "gzip" => GzDecoder::new(body).read_to_string(&mut decoded).unwrap(),
        "deflate" => ZlibDecoder::new(body).read_to_string(&mut decoded).unwrap(),
        other => panic!("unexpected encoding {other}"),
    };
    decoded
}

#[tokio::test]
async fn compression_negotiates_weighted_accept_encoding() {
    let cases = [
        ("gzip;q=0.4, deflate;q=0.8", Some("deflate"), 200),
        ("br;q=0, gzip;q=1", Some("gzip"), 200),
        ("identity;q=0, *;q=0", None, 406),
        ("*;q=0.5", Some("gzip"), 200),
        ("gzip;q=0.5", Some("gzip"), 200),
        ("gzip;q=0.4, identity;q=0.9", None, 200),
        (
            "deflate;q=0.8, identity;q=0.8, gzip;q=0.8",
            Some("gzip"),
            200,
        ),
    ];

    for (header, expected_encoding, expected_status) in cases {
        let client = compression_client();
        let response = client
            .get("/data")
            .header("accept-encoding", header)
            .send()
            .await;

        assert_eq!(response.status, expected_status, "header={header}");
        match expected_encoding {
            Some(encoding) => {
                assert_eq!(
                    response
                        .headers
                        .get(CONTENT_ENCODING)
                        .unwrap_or_else(|| {
                            panic!(
                                "missing Content-Encoding for header={header}; headers={:?}",
                                response.headers
                            )
                        })
                        .to_str()
                        .unwrap(),
                    encoding,
                    "header={header}"
                );
                assert_eq!(
                    decode_body(encoding, response.body_bytes().unwrap()).len(),
                    2048
                );
            }
            None => assert!(
                response.headers.get(CONTENT_ENCODING).is_none(),
                "header={header}"
            ),
        }
    }
}

#[tokio::test]
async fn compression_combines_repeated_accept_encoding_fields() {
    let response = compression_client()
        .get("/data")
        .header("accept-encoding", "gzip;q=0")
        .header("accept-encoding", "deflate;q=1")
        .send()
        .await;

    assert_eq!(response.status, 200);
    assert_eq!(response.headers.get(CONTENT_ENCODING).unwrap(), "deflate");
    assert_eq!(
        decode_body("deflate", response.body_bytes().unwrap()).len(),
        2048
    );
}

#[tokio::test]
async fn large_buffered_compression_preserves_the_representation() {
    let mut app = App::new();
    app.layer(middleware::compression_with_min_size(0));
    let body = "respuesta grande ".repeat(10_000);
    let expected = body.clone();
    let _ = app.get("/large", move |_req: Request| Response::send(&body));
    let client = TestClient::new(app);

    let response = client
        .get("/large")
        .header("accept-encoding", "gzip")
        .send()
        .await;

    assert_eq!(response.status, 200);
    assert_eq!(response.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
    assert_eq!(
        decode_body("gzip", response.body_bytes().unwrap()),
        expected
    );
}

#[tokio::test]
async fn compression_rewrites_representation_headers_and_combines_vary() {
    let mut app = App::new();
    app.layer(middleware::compression_with_min_size(0));
    let body = "x".repeat(2048);
    let _ = app.get("/data", move |_req: Request| {
        Response::send(&body)
            .header(CONTENT_LENGTH.as_str(), "2048")
            .header(ACCEPT_RANGES.as_str(), "bytes")
            .append_header(VARY.as_str(), "Origin")
            .append_header(VARY.as_str(), "accept-encoding, ORIGIN")
    });
    let client = TestClient::new(app);

    let compressed = client
        .get("/data")
        .header("accept-encoding", "gzip")
        .send()
        .await;
    assert_eq!(compressed.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
    assert_eq!(
        compressed.headers.get(CONTENT_LENGTH).unwrap(),
        compressed.body_bytes().unwrap().len().to_string().as_str()
    );
    assert_ne!(compressed.headers.get(CONTENT_LENGTH).unwrap(), "2048");
    assert!(compressed.headers.get(ACCEPT_RANGES).is_none());
    assert_eq!(
        compressed.headers.get(VARY).unwrap(),
        "Origin, Accept-Encoding"
    );
    assert_eq!(compressed.headers.get_all(VARY).iter().count(), 1);

    let identity = client
        .get("/data")
        .header("accept-encoding", "gzip;q=0.4, identity;q=0.9")
        .send()
        .await;
    assert!(identity.headers.get(CONTENT_ENCODING).is_none());
    assert_eq!(identity.headers.get(CONTENT_LENGTH).unwrap(), "2048");
    assert_eq!(identity.headers.get(ACCEPT_RANGES).unwrap(), "bytes");
    assert_eq!(
        identity.headers.get(VARY).unwrap(),
        "Origin, Accept-Encoding"
    );
    assert_eq!(identity.headers.get_all(VARY).iter().count(), 1);
}

#[tokio::test]
async fn compression_varies_identity_but_leaves_preencoded_responses_alone() {
    let mut app = App::new();
    app.layer(middleware::compression());
    let large = "x".repeat(2048);

    let body = large.clone();
    let _ = app.get("/identity", move |_req: Request| {
        Response::send(&body).header(VARY.as_str(), "Origin")
    });
    let _ = app.get("/small", |_req: Request| Response::send("tiny"));
    let body = large;
    let _ = app.get("/encoded", move |_req: Request| {
        Response::send(&body)
            .header(CONTENT_ENCODING.as_str(), "br")
            .header(VARY.as_str(), "Origin")
    });
    let client = TestClient::new(app);

    let preferred_identity = client
        .get("/identity")
        .header("accept-encoding", "gzip;q=0.4, identity;q=0.9")
        .send()
        .await;
    assert!(preferred_identity.headers.get(CONTENT_ENCODING).is_none());
    assert_eq!(
        preferred_identity.headers.get(VARY).unwrap(),
        "Origin, Accept-Encoding"
    );

    let skipped_small = client
        .get("/small")
        .header("accept-encoding", "gzip")
        .send()
        .await;
    assert!(skipped_small.headers.get(CONTENT_ENCODING).is_none());
    assert_eq!(skipped_small.headers.get(VARY).unwrap(), "Accept-Encoding");

    let skipped_small_without_header = client.get("/small").send().await;
    assert_eq!(
        skipped_small_without_header.headers.get(VARY).unwrap(),
        "Accept-Encoding"
    );

    let no_accept_encoding = client.get("/identity").send().await;
    assert!(no_accept_encoding.headers.get(CONTENT_ENCODING).is_none());
    assert_eq!(
        no_accept_encoding.headers.get(VARY).unwrap(),
        "Origin, Accept-Encoding"
    );

    let preencoded = client
        .get("/encoded")
        .header("accept-encoding", "br")
        .send()
        .await;
    assert_eq!(preencoded.headers.get(CONTENT_ENCODING).unwrap(), "br");
    assert_eq!(
        preencoded.headers.get(VARY).unwrap(),
        "Origin, Accept-Encoding"
    );

    let rejected = client
        .get("/encoded")
        .header("accept-encoding", "gzip, br;q=0")
        .send()
        .await;
    assert_eq!(rejected.status, 406);
    assert_eq!(rejected.headers.get(VARY).unwrap(), "Accept-Encoding");
}

#[tokio::test]
async fn compression_validates_every_preencoded_content_encoding_field_line() {
    let mut app = App::new();
    app.layer(middleware::compression());
    let body = "x".repeat(2048);
    let _ = app.get("/encoded", move |_req: Request| {
        Response::send(&body)
            .header(CONTENT_ENCODING.as_str(), "gzip")
            .append_header(CONTENT_ENCODING.as_str(), "br")
    });
    let client = TestClient::new(app);

    let rejected = client
        .get("/encoded")
        .header("accept-encoding", "gzip")
        .send()
        .await;
    assert_eq!(rejected.status, 406);

    let accepted = client
        .get("/encoded")
        .header("accept-encoding", "gzip, br")
        .send()
        .await;
    assert_eq!(accepted.status, 200);
    assert_eq!(
        accepted
            .headers
            .get_all(CONTENT_ENCODING)
            .iter()
            .collect::<Vec<_>>(),
        vec!["gzip", "br"]
    );
}

#[tokio::test]
async fn compression_removes_validators_for_the_unencoded_representation() {
    let mut app = App::new();
    app.layer(middleware::compression_with_min_size(0));
    let body = "x".repeat(2048);
    let _ = app.get("/data", move |_req: Request| {
        Response::send(&body)
            .header(ETAG.as_str(), "\"unencoded\"")
            .header("digest", "sha-256=:AAAA:")
            .header("content-digest", "sha-256=:AAAA:")
            .header("repr-digest", "sha-256=:AAAA:")
            .header("content-md5", "AAAA")
    });

    let response = TestClient::new(app)
        .get("/data")
        .header("accept-encoding", "gzip")
        .send()
        .await;

    assert_eq!(response.status, 200);
    assert_eq!(response.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
    for name in [
        ETAG.as_str(),
        "digest",
        "content-digest",
        "repr-digest",
        "content-md5",
    ] {
        assert!(
            response.headers.get(name).is_none(),
            "stale representation header {name}"
        );
    }
}

#[tokio::test]
async fn compression_preserves_trailers_only_with_the_identity_representation() {
    let mut app = App::new();
    app.layer(middleware::compression_with_min_size(0));
    let _ = app.get("/trailers", |_req: Request| {
        let mut trailers = HeaderMap::new();
        trailers.insert(ETAG, HeaderValue::from_static("\"late\""));
        Response::send(&"x".repeat(2048)).with_trailers(trailers)
    });
    let client = TestClient::new(app);

    let identity = client
        .get("/trailers")
        .header("accept-encoding", "gzip")
        .send()
        .await;
    assert_eq!(identity.status, 200);
    assert!(identity.headers.get(CONTENT_ENCODING).is_none());
    assert_eq!(identity.headers.get("trailer").unwrap(), "etag");

    let rejected = client
        .get("/trailers")
        .header("accept-encoding", "gzip, identity;q=0")
        .send()
        .await;
    assert_eq!(rejected.status, 406);
}

#[tokio::test]
async fn compression_returns_406_when_identity_is_forbidden_and_transform_is_skipped() {
    let mut app = App::new();
    app.layer(middleware::compression());
    let large = "x".repeat(2048);

    let _ = app.get("/small", |_req: Request| Response::send("tiny"));
    let _ = app.get("/stream", |_req: Request| {
        Response::stream(stream::iter(vec![Ok::<_, Infallible>(Bytes::from_static(
            b"stream",
        ))]))
    });
    let body = large.clone();
    let _ = app.get("/image", move |_req: Request| {
        Response::bytes(body.clone().into(), "image/png")
    });
    let body = large.clone();
    let _ = app.get("/no-transform", move |_req: Request| {
        Response::send(&body).header(CACHE_CONTROL.as_str(), "no-transform")
    });
    let body = large;
    let _ = app.get("/partial", move |_req: Request| {
        Response::send(&body).status(206)
    });
    let _ = app.get("/early", |_req: Request| {
        Response::send("ignored").status(103)
    });
    let _ = app.get("/no-content", |_req: Request| {
        Response::send("ignored").status(204)
    });
    let _ = app.get("/reset-content", |_req: Request| {
        Response::send("ignored").status(205)
    });
    let _ = app.get("/not-modified", |_req: Request| {
        Response::send("ignored").status(304)
    });
    let client = TestClient::new(app);

    for path in ["/small", "/stream", "/image", "/no-transform", "/partial"] {
        let response = client
            .get(path)
            .header("accept-encoding", "gzip, identity;q=0")
            .send()
            .await;
        assert_eq!(response.status, 406, "path={path}");
        assert_eq!(
            response.headers.get(VARY).unwrap(),
            "Accept-Encoding",
            "path={path}"
        );
    }

    for (path, status) in [
        // A lone informational response is invalid as the final response and
        // is normalized to an internal error during finalization.
        ("/early", 500),
        ("/no-content", 204),
        ("/reset-content", 205),
        ("/not-modified", 304),
    ] {
        let response = client
            .get(path)
            .header("accept-encoding", "identity;q=0, *;q=0")
            .send()
            .await;
        assert_eq!(response.status, status, "path={path}");
        assert!(response.headers.get(VARY).is_none(), "path={path}");
    }
}

#[tokio::test]
async fn compression_skips_statuses_and_untransformable_responses() {
    let mut app = App::new();
    app.layer(middleware::compression_with_min_size(0));
    let large = "x".repeat(2048);

    let body = large.clone();
    let _ = app.get("/204", move |_req: Request| {
        Response::send(&body).status(204)
    });
    let body = large.clone();
    let _ = app.get("/205", move |_req: Request| {
        Response::send(&body).status(205)
    });
    let body = large.clone();
    let _ = app.get("/304", move |_req: Request| {
        Response::send(&body).status(304)
    });
    let body = large.clone();
    let _ = app.get("/206", move |_req: Request| {
        Response::send(&body).status(206)
    });
    let body = large.clone();
    let _ = app.get("/png", move |_req: Request| {
        Response::bytes(body.clone().into(), "image/png")
    });
    let body = large.clone();
    let _ = app.get("/no-transform", move |_req: Request| {
        Response::send(&body).header(CACHE_CONTROL.as_str(), "public, no-transform")
    });
    let body = large.clone();
    let _ = app.get("/split-no-transform", move |_req: Request| {
        Response::send(&body)
            .header(CACHE_CONTROL.as_str(), "public")
            .append_header(CACHE_CONTROL.as_str(), "no-transform")
    });
    let body = large;
    let _ = app.get("/encoded", move |_req: Request| {
        Response::send(&body).header(CONTENT_ENCODING.as_str(), "identity")
    });

    let client = TestClient::new(app);
    for path in [
        "/204",
        "/205",
        "/304",
        "/206",
        "/png",
        "/no-transform",
        "/split-no-transform",
        "/encoded",
    ] {
        let response = client
            .get(path)
            .header("accept-encoding", "gzip")
            .send()
            .await;
        assert!(
            response.headers.get(CONTENT_ENCODING).is_none()
                || path == "/encoded"
                    && response.headers.get(CONTENT_ENCODING).unwrap() == "identity",
            "{path} was unexpectedly transformed: {:?}",
            response.headers
        );
    }
}

#[tokio::test]
async fn buffered_preconditions_follow_rfc_order() {
    let modified = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let before = httpdate::fmt_http_date(modified - Duration::from_secs(60));
    let after = httpdate::fmt_http_date(modified + Duration::from_secs(60));
    let modified = httpdate::fmt_http_date(modified);

    let mut app = App::new();
    app.layer(middleware::etag());
    let updates = Arc::new(AtomicUsize::new(0));
    let _ = app.get("/doc", move |_req: Request| {
        Response::send("contenido")
            .header("last-modified", &modified)
            .header("etag", "\"v1\"")
    });
    let handler_updates = Arc::clone(&updates);
    let _ = app.post("/doc", move |_req: Request| {
        handler_updates.fetch_add(1, Ordering::SeqCst);
        Response::send("updated").header("etag", "\"v1\"")
    });
    let client = TestClient::new(app);

    let failed_first = client
        .get("/doc")
        .header("if-match", "\"missing\"")
        .header("if-none-match", "\"v1\"")
        .send()
        .await;
    assert_eq!(failed_first.status, 412);

    let if_match_ok = client.get("/doc").header("if-match", "\"v1\"").send().await;
    assert_eq!(if_match_ok.status, 200);

    let not_modified = client
        .get("/doc")
        .header("if-none-match", "W/\"v1\"")
        .send()
        .await;
    assert_eq!(not_modified.status, 304);
    assert_eq!(not_modified.body_text(), "");

    let unsafe_match = client
        .post("/doc")
        .header("if-none-match", "\"v1\"")
        .send()
        .await;
    assert_eq!(unsafe_match.status, 200);
    assert_eq!(unsafe_match.body_text(), "updated");
    assert_eq!(updates.load(Ordering::SeqCst), 1);

    let split_if_match = client
        .get("/doc")
        .header("if-match", "\"missing\"")
        .header("if-match", "\"v1\"")
        .send()
        .await;
    assert_eq!(split_if_match.status, 200);

    let split_if_none_match = client
        .get("/doc")
        .header("if-none-match", "\"missing\"")
        .header("if-none-match", "W/\"v1\"")
        .send()
        .await;
    assert_eq!(split_if_none_match.status, 304);

    let unmodified_failed = client
        .get("/doc")
        .header("if-unmodified-since", &before)
        .send()
        .await;
    assert_eq!(unmodified_failed.status, 412);

    let modified_since = client
        .get("/doc")
        .header("if-modified-since", &after)
        .send()
        .await;
    assert_eq!(modified_since.status, 304);
}

#[tokio::test]
async fn buffered_preconditions_ignore_ambiguous_response_validators() {
    let modified = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    let before = httpdate::fmt_http_date(modified - Duration::from_secs(60));
    let after = httpdate::fmt_http_date(modified + Duration::from_secs(60));
    let modified = httpdate::fmt_http_date(modified);

    let mut app = App::new();
    app.layer(middleware::etag());
    let _ = app.get("/duplicate-etag", |_req: Request| {
        Response::send("contenido con ETag ambiguo")
            .header(ETAG.as_str(), "\"v1\"")
            .append_header(ETAG.as_str(), "\"v2\"")
    });
    let _ = app.get("/duplicate-last-modified", move |_req: Request| {
        Response::send("contenido con fecha ambigua")
            .header("last-modified", &modified)
            .append_header("last-modified", &modified)
            .header(ETAG.as_str(), "\"fecha-v1\"")
    });
    let client = TestClient::new(app);

    let duplicate_etag_does_not_revalidate = client
        .get("/duplicate-etag")
        .header("if-none-match", "\"v1\"")
        .send()
        .await;
    assert_eq!(duplicate_etag_does_not_revalidate.status, 200);
    assert_eq!(
        duplicate_etag_does_not_revalidate.body_text(),
        "contenido con ETag ambiguo"
    );
    assert_eq!(
        duplicate_etag_does_not_revalidate
            .headers
            .get_all(ETAG)
            .iter()
            .count(),
        2
    );

    let duplicate_etag_does_not_fail = client
        .get("/duplicate-etag")
        .header("if-match", "\"missing\"")
        .send()
        .await;
    assert_eq!(duplicate_etag_does_not_fail.status, 200);
    assert_eq!(
        duplicate_etag_does_not_fail.body_text(),
        "contenido con ETag ambiguo"
    );

    let wildcard_still_revalidates = client
        .get("/duplicate-etag")
        .header("if-none-match", "*")
        .send()
        .await;
    assert_eq!(wildcard_still_revalidates.status, 304);
    assert!(wildcard_still_revalidates.headers.get(ETAG).is_none());

    let duplicate_date_does_not_revalidate = client
        .get("/duplicate-last-modified")
        .header("if-modified-since", &after)
        .send()
        .await;
    assert_eq!(duplicate_date_does_not_revalidate.status, 200);
    assert_eq!(
        duplicate_date_does_not_revalidate.body_text(),
        "contenido con fecha ambigua"
    );

    let duplicate_date_does_not_fail = client
        .get("/duplicate-last-modified")
        .header("if-unmodified-since", &before)
        .send()
        .await;
    assert_eq!(duplicate_date_does_not_fail.status, 200);
    assert_eq!(
        duplicate_date_does_not_fail.body_text(),
        "contenido con fecha ambigua"
    );

    let etag_revalidation_drops_the_ambiguous_date = client
        .get("/duplicate-last-modified")
        .header("if-none-match", "\"fecha-v1\"")
        .send()
        .await;
    assert_eq!(etag_revalidation_drops_the_ambiguous_date.status, 304);
    assert!(
        etag_revalidation_drops_the_ambiguous_date
            .headers
            .get("last-modified")
            .is_none()
    );
}

#[tokio::test]
async fn failed_preconditions_remove_inner_compression_metadata() {
    let mut app = App::new();
    // ETag is outermost on the response path, so it evaluates the compressed
    // representation selected by the inner middleware.
    app.layer(middleware::etag());
    app.layer(middleware::compression_with_min_size(0));
    let _ = app.get("/doc", |_req: Request| {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("late"));
        Response::send("contenido comprimible")
            .header("digest", "sha-256=:AAAA:")
            .header("content-range", "bytes 0-19/20")
            .with_trailers(trailers)
    });

    let response = TestClient::new(app)
        .get("/doc")
        .header("accept-encoding", "gzip")
        .header("if-match", "\"missing\"")
        .send()
        .await;

    assert_eq!(response.status, 412);
    assert_eq!(response.body_text(), "");
    assert!(response.headers.get("trailer").is_none());
    for name in [
        CONTENT_ENCODING.as_str(),
        CONTENT_LENGTH.as_str(),
        ACCEPT_RANGES.as_str(),
        "content-range",
        "content-md5",
        "digest",
        "content-digest",
        "repr-digest",
    ] {
        assert!(
            response.headers.get(name).is_none(),
            "stale representation header {name}"
        );
    }
}

#[tokio::test]
async fn etag_handles_empty_bodies_quoted_commas_and_failed_framing() {
    let mut app = App::new();
    app.layer(middleware::etag());
    let _ = app.get("/empty", |_req: Request| Response::send(""));
    let _ = app.get("/comma", |_req: Request| {
        Response::send("contenido").header("etag", "\"release,1\"")
    });
    let _ = app.get("/length", |_req: Request| {
        Response::send("contenido").header("content-length", "9")
    });
    let client = TestClient::new(app);

    let empty = client.get("/empty").send().await;
    let empty_etag = empty
        .headers
        .get(ETAG)
        .expect("empty representations still have validators")
        .to_str()
        .unwrap()
        .to_string();
    let empty_revalidated = client
        .get("/empty")
        .header("if-none-match", &empty_etag)
        .send()
        .await;
    assert_eq!(empty_revalidated.status, 304);

    let comma_revalidated = client
        .get("/comma")
        .header("if-none-match", "\"other\", W/\"release,1\"")
        .send()
        .await;
    assert_eq!(comma_revalidated.status, 304);

    let failed = client
        .get("/length")
        .header("if-match", "\"missing\"")
        .send()
        .await;
    assert_eq!(failed.status, 412);
    assert!(failed.headers.get(CONTENT_LENGTH).is_none());
}

#[tokio::test]
async fn static_file_preconditions_and_if_range_control_ranges() {
    let root = semantics_temp_dir("http-semantics-static");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("static.txt"), "0123456789").unwrap();

    let mut app = App::new();
    let _ = app.static_files("/assets", &root);
    let client = TestClient::new(app);

    let full = client.get("/assets/static.txt").send().await;
    let etag = full
        .headers
        .get("etag")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let last_modified = full
        .headers
        .get("last-modified")
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let modified = httpdate::parse_http_date(&last_modified).unwrap();
    let before = httpdate::fmt_http_date(modified - Duration::from_secs(60));
    let after = httpdate::fmt_http_date(modified + Duration::from_secs(60));

    let if_match_failed = client
        .get("/assets/static.txt")
        .header("if-match", "\"missing\"")
        .send()
        .await;
    assert_eq!(if_match_failed.status, 412);

    let weak_not_modified = client
        .get("/assets/static.txt")
        .header("if-none-match", &etag)
        .send()
        .await;
    assert_eq!(weak_not_modified.status, 304);

    let unmodified_failed = client
        .get("/assets/static.txt")
        .header("if-unmodified-since", &before)
        .send()
        .await;
    assert_eq!(unmodified_failed.status, 412);

    let ranged = client
        .get("/assets/static.txt")
        .header("range", "bytes=2-5")
        .header("if-range", &etag)
        .send()
        .await;
    assert_eq!(ranged.status, 200);

    let date_ranged = client
        .get("/assets/static.txt")
        .header("range", "bytes=2-5")
        .header("if-range", &last_modified)
        .send()
        .await;
    assert_eq!(date_ranged.status, 206);

    let ignored_range = client
        .get("/assets/static.txt")
        .header("range", "bytes=2-5")
        .header("if-range", "\"missing\"")
        .send()
        .await;
    assert_eq!(ignored_range.status, 200);

    let ignored_date_range = client
        .get("/assets/static.txt")
        .header("range", "bytes=2-5")
        .header("if-range", &before)
        .send()
        .await;
    assert_eq!(ignored_date_range.status, 200);

    let future_date_does_not_validate_the_current_representation = client
        .get("/assets/static.txt")
        .header("range", "bytes=2-5")
        .header("if-range", &after)
        .send()
        .await;
    assert_eq!(
        future_date_does_not_validate_the_current_representation.status,
        200
    );

    let duplicate_if_range_is_ambiguous = client
        .get("/assets/static.txt")
        .header("range", "bytes=2-5")
        .header("if-range", &last_modified)
        .header("if-range", &last_modified)
        .send()
        .await;
    assert_eq!(duplicate_if_range_is_ambiguous.status, 200);

    fs::remove_dir_all(root).unwrap();
}
