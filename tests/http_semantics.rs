use std::fs;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::read::{GzDecoder, ZlibDecoder};
use hyper::header::{CACHE_CONTROL, CONTENT_ENCODING};
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
                        .unwrap()
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
async fn compression_skips_statuses_and_untransformable_responses() {
    let mut app = App::new();
    app.layer(middleware::compression_with_min_size(0));
    let large = "x".repeat(2048);

    let body = large.clone();
    let _ = app.get("/204", move |_req: Request| {
        Response::send(&body).status(204)
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
    let body = large;
    let _ = app.get("/encoded", move |_req: Request| {
        Response::send(&body).header(CONTENT_ENCODING.as_str(), "identity")
    });

    let client = TestClient::new(app);
    for path in ["/204", "/304", "/206", "/png", "/no-transform", "/encoded"] {
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
    let _ = app.get("/doc", move |_req: Request| {
        Response::send("contenido")
            .header("last-modified", &modified)
            .header("etag", "\"v1\"")
    });
    let _ = app.post("/doc", |_req: Request| {
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
    assert_eq!(unsafe_match.status, 412);

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

    let if_match_failed = client
        .get("/assets/static.txt")
        .header("if-match", "\"missing\"")
        .send()
        .await;
    assert_eq!(if_match_failed.status, 412);

    let weak_not_modified = client
        .get("/assets/static.txt")
        .header("if-none-match", &format!("W/{etag}"))
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
    assert_eq!(ranged.status, 206);

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

    fs::remove_dir_all(root).unwrap();
}
