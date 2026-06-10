use super::router::{match_pattern, parse_pattern, path_segments};
use super::*;
use futures_util::stream;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use hyper::header::{CONTENT_ENCODING, LOCATION, SET_COOKIE};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn dummy_request(body: &str) -> Request {
    Request {
        method: "GET".to_string(),
        path: "/".to_string(),
        raw_query: None,
        query: HashMap::new(),
        headers: HashMap::new(),
        cookies: HashMap::new(),
        body: Bytes::from(body.to_string()),
        params: HashMap::new(),
        state: StateStore::default(),
        upgrade: None,
        remote_addr: None,
        header_pairs: Vec::new(),
    }
}

#[test]
fn websocket_config_rejects_unbounded_or_inconsistent_values() {
    assert!(
        WebSocketConfig::new()
            .outbound_capacity(0)
            .validate()
            .is_err()
    );
    assert!(
        WebSocketConfig::new()
            .inbound_capacity(0)
            .validate()
            .is_err()
    );
    assert!(
        WebSocketConfig::new()
            .write_buffer_size(1024)
            .max_write_buffer_size(1024)
            .validate()
            .is_err()
    );
    assert!(
        WebSocketConfig::new()
            .ping_interval(Duration::from_secs(30))
            .pong_timeout(Duration::from_secs(30))
            .validate()
            .is_err()
    );
}

#[test]
fn websocket_origin_policy_normalizes_default_ports() {
    let policy = OriginPolicy::allow(["https://app.example.com"]);
    assert!(policy.allows(Some("https://app.example.com:443"), "app.example.com"));
    assert!(!policy.allows(Some("https://evil.example"), "app.example.com"));

    let same_host = OriginPolicy::same_host().allow_missing(false);
    assert!(same_host.allows(Some("http://localhost:3000"), "localhost:3000"));
    assert!(!same_host.allows(None, "localhost:3000"));
}

#[test]
fn websocket_origin_policy_rejects_invalid_explicit_ports() {
    let policy = OriginPolicy::any();
    assert!(!policy.allows(
        Some("https://app.example.com:not-a-port"),
        "app.example.com"
    ));

    let config = WebSocketConfig::new()
        .origin_policy(OriginPolicy::allow(["https://app.example.com:not-a-port"]));
    assert!(config.validate().is_err());
}

#[test]
fn existing_websocket_error_remains_exhaustive() {
    fn classify(error: WebSocketError) -> &'static str {
        match error {
            WebSocketError::Protocol(_) => "protocol",
            WebSocketError::Json(_) => "json",
        }
    }
    let _ = classify as fn(WebSocketError) -> &'static str;
}

#[test]
fn typed_extractors_read_json_path_query_and_state() {
    #[derive(Deserialize)]
    struct CreateUser {
        name: String,
    }
    #[derive(Deserialize)]
    struct UserPath {
        id: u32,
    }
    #[derive(Deserialize)]
    struct UserQuery {
        active: bool,
        tag: Vec<String>,
    }
    struct Config {
        app_name: &'static str,
    }

    let mut state = StateStore::default();
    state.insert(Config {
        app_name: "rustrest",
    });
    let mut req = dummy_request(r#"{"name":"Ada"}"#);
    req.raw_query = Some("active=true&tag=rust&tag=http".to_string());
    req.query = parse_query(req.raw_query.as_deref().unwrap());
    req.params.insert("id".to_string(), "42".to_string());
    req.state = state;

    let Json(user) = req.extract::<Json<CreateUser>>().unwrap();
    let Path(path) = req.extract::<Path<UserPath>>().unwrap();
    let Query(query) = req.extract::<Query<UserQuery>>().unwrap();
    let State(config) = req.extract::<State<Config>>().unwrap();

    assert_eq!(user.name, "Ada");
    assert_eq!(path.id, 42);
    assert!(query.active);
    assert_eq!(query.tag, vec!["rust", "http"]);
    assert_eq!(config.app_name, "rustrest");
}

#[test]
fn extra_extractors_cover_scalars_bodies_wrappers_and_maps() {
    #[derive(Deserialize)]
    struct MyCookies {
        sid: String,
    }
    #[derive(Deserialize)]
    struct MyHeaders {
        #[serde(rename = "x-api-key")]
        key: String,
    }

    let mut req = dummy_request("cuerpo");
    req.params.insert("id".to_string(), "42".to_string());
    req.cookies.insert("sid".to_string(), "abc".to_string());
    req.headers
        .insert("x-api-key".to_string(), "k1".to_string());

    // Scalar Path for single-param routes (numbers and strings).
    let Path(id) = req.extract::<Path<u32>>().unwrap();
    assert_eq!(id, 42);
    let Path(raw) = req.extract::<Path<String>>().unwrap();
    assert_eq!(raw, "42");

    // Raw body extractors.
    let bytes = req.extract::<Bytes>().unwrap();
    assert_eq!(&bytes[..], b"cuerpo");
    let text = req.extract::<String>().unwrap();
    assert_eq!(text, "cuerpo");

    // Option/Result wrappers never fail the extraction itself.
    let missing: Option<Json<serde_json::Value>> = req.extract().unwrap();
    assert!(missing.is_none());
    let failed: Result<Json<serde_json::Value>, HttpError> = req.extract().unwrap();
    assert!(failed.is_err());

    // Typed cookie/header maps.
    let Cookies(cookies) = req.extract::<Cookies<MyCookies>>().unwrap();
    assert_eq!(cookies.sid, "abc");
    let Headers(headers) = req.extract::<Headers<MyHeaders>>().unwrap();
    assert_eq!(headers.key, "k1");
}

#[tokio::test]
async fn http_errors_keep_status_and_can_use_global_error_handler() {
    #[derive(Serialize)]
    struct ErrorBody<'a> {
        error: &'a str,
        status: u16,
    }

    let mut app = App::new();
    app.error_handler(|err: HttpError| {
        Response::json(&ErrorBody {
            error: err.message(),
            status: err.status(),
        })
        .status(err.status())
    });
    app.get("/", |_req: Request| -> Result<Response, HttpError> {
        Err(HttpError::bad_request("Invalid name"))
    });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.status, 400);
    assert_eq!(res.body_text(), r#"{"error":"Invalid name","status":400}"#);
}

#[tokio::test]
async fn builtin_middlewares_add_cors_request_id_gzip_and_tracing() {
    let mut app = App::new();
    app.layer(middleware::tracing());
    app.layer(middleware::request_id());
    app.layer(middleware::cors());
    app.layer(middleware::gzip());
    app.get("/", |req: Request| {
        Response::send(req.header("x-request-id").unwrap_or("no-id"))
    });

    let mut req = dummy_request("");
    req.headers
        .insert("accept-encoding".to_string(), "br, gzip".to_string());
    req.headers
        .insert("x-request-id".to_string(), "req-123".to_string());

    let res = app.dispatch(req).await;

    assert_eq!(res.headers.get("access-control-allow-origin").unwrap(), "*");
    assert_eq!(res.headers.get("x-request-id").unwrap(), "req-123");
    assert_eq!(res.headers.get(CONTENT_ENCODING).unwrap(), "gzip");

    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let mut decoder = flate2::read::GzDecoder::new(&body[..]);
    let mut decoded = String::new();
    decoder.read_to_string(&mut decoded).unwrap();
    assert_eq!(decoded, "req-123");
}

#[cfg(feature = "tracing")]
#[tokio::test]
async fn trace_middleware_emits_events_and_passes_response_through() {
    use tracing::instrument::WithSubscriber;

    struct CountingSubscriber(Arc<AtomicUsize>);
    impl tracing::Subscriber for CountingSubscriber {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, _: &tracing::Event<'_>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    let events = Arc::new(AtomicUsize::new(0));
    let subscriber = CountingSubscriber(Arc::clone(&events));

    let mut app = App::new();
    app.layer(middleware::trace());
    app.get("/ping", |_r: Request| Response::send("pong"));
    let client = TestClient::new(app);

    let res = client.get("/ping").send().with_subscriber(subscriber).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "pong");
    assert!(events.load(Ordering::SeqCst) >= 1, "expected trace events");
}

#[tokio::test]
async fn etag_middleware_sets_validator_and_answers_304() {
    let mut app = App::new();
    app.layer(middleware::etag());
    app.get("/doc", |_r: Request| Response::send("contenido estable"));

    let client = TestClient::new(app);

    let first = client.get("/doc").send().await;
    assert_eq!(first.status, 200);
    let tag = first
        .headers
        .get("etag")
        .expect("etag set")
        .to_str()
        .unwrap()
        .to_string();
    assert!(
        tag.starts_with('"') && tag.ends_with('"'),
        "strong quoted etag, got {tag}"
    );

    // A matching validator gets 304 with no body but the ETag preserved.
    let revalidated = client
        .get("/doc")
        .header("if-none-match", &tag)
        .send()
        .await;
    assert_eq!(revalidated.status, 304);
    assert_eq!(revalidated.body_text(), "");
    assert_eq!(
        revalidated.headers.get("etag").unwrap().to_str().unwrap(),
        tag
    );

    // A stale validator gets the full response again.
    let stale = client
        .get("/doc")
        .header("if-none-match", "\"nope\"")
        .send()
        .await;
    assert_eq!(stale.status, 200);
    assert_eq!(stale.body_text(), "contenido estable");
}

#[tokio::test]
async fn timeout_middleware_cuts_off_slow_handlers() {
    let mut app = App::new();
    app.get("/slow", |_r: Request| async {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        Response::send("late")
    })
    .layer(middleware::timeout(std::time::Duration::from_millis(40)));
    app.get("/fast", |_r: Request| Response::send("quick"))
        .layer(middleware::timeout(std::time::Duration::from_millis(40)));

    let slow = app.dispatch(request_with_method("GET", "/slow")).await;
    assert_eq!(slow.status, 408);

    // Fast handlers under the same budget are untouched.
    let fast = app.dispatch(request_with_method("GET", "/fast")).await;
    assert_eq!(fast.status, 200);
    assert_eq!(fast.body_text(), "quick");
}

#[tokio::test]
async fn rate_limit_middleware_throttles_per_ip_and_recovers() {
    fn request_from(addr: &str) -> Request {
        let mut req = dummy_request("");
        req.remote_addr = Some(addr.parse().unwrap());
        req
    }

    let mut app = App::new();
    app.layer(middleware::rate_limit(
        2,
        std::time::Duration::from_millis(80),
    ));
    app.get("/", |_r: Request| Response::send("ok"));

    // Two requests from the same IP pass; the third is throttled. The port
    // must not matter — limiting is per IP.
    assert_eq!(app.dispatch(request_from("1.1.1.1:1000")).await.status, 200);
    assert_eq!(app.dispatch(request_from("1.1.1.1:1001")).await.status, 200);
    let throttled = app.dispatch(request_from("1.1.1.1:1002")).await;
    assert_eq!(throttled.status, 429);
    assert!(throttled.headers.get("retry-after").is_some());

    // A different client is unaffected.
    assert_eq!(app.dispatch(request_from("2.2.2.2:1000")).await.status, 200);

    // Once the window expires the client is admitted again.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(app.dispatch(request_from("1.1.1.1:1003")).await.status, 200);
}

#[tokio::test]
async fn compression_negotiates_encoding_and_skips_small_bodies() {
    let mut app = App::new();
    app.layer(middleware::compression());
    let big = "x".repeat(2048);
    app.get("/big", move |_r: Request| Response::send(&big));
    app.get("/small", |_r: Request| Response::send("tiny"));

    let client = TestClient::new(app);

    // Only deflate accepted -> zlib-encoded body.
    let res = client
        .get("/big")
        .header("accept-encoding", "deflate")
        .send()
        .await;
    assert_eq!(res.headers.get(CONTENT_ENCODING).unwrap(), "deflate");
    let mut decoder = flate2::read::ZlibDecoder::new(res.body_bytes().unwrap());
    let mut decoded = String::new();
    decoder.read_to_string(&mut decoded).unwrap();
    assert_eq!(decoded.len(), 2048);

    // gzip preferred when both are accepted; Vary advertises the negotiation.
    let res = client
        .get("/big")
        .header("accept-encoding", "deflate, gzip;q=0.8")
        .send()
        .await;
    assert_eq!(res.headers.get(CONTENT_ENCODING).unwrap(), "gzip");
    assert_eq!(res.headers.get("vary").unwrap(), "Accept-Encoding");

    // Bodies under the threshold are left alone.
    let res = client
        .get("/small")
        .header("accept-encoding", "gzip")
        .send()
        .await;
    assert!(res.headers.get(CONTENT_ENCODING).is_none());

    // No Accept-Encoding -> untouched.
    let res = client.get("/big").send().await;
    assert!(res.headers.get(CONTENT_ENCODING).is_none());

    // q=0 explicitly refuses an encoding.
    let res = client
        .get("/big")
        .header("accept-encoding", "gzip;q=0, deflate")
        .send()
        .await;
    assert_eq!(res.headers.get(CONTENT_ENCODING).unwrap(), "deflate");
}

#[tokio::test]
async fn cors_builder_handles_preflight_and_origin_allowlist() {
    let mut app = App::new();
    app.layer(
        middleware::Cors::new()
            .allow_origin("https://app.example.com")
            .allow_credentials(true)
            .max_age_secs(600),
    );
    app.get("/data", |_r: Request| Response::send("data"));
    app.post("/data", |_r: Request| Response::send("created"));

    let client = TestClient::new(app);

    // Preflight short-circuits with the CORS grant.
    let res = client
        .options("/data")
        .header("origin", "https://app.example.com")
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "x-custom")
        .send()
        .await;
    assert_eq!(res.status, 204);
    assert_eq!(
        res.headers.get("access-control-allow-origin").unwrap(),
        "https://app.example.com"
    );
    assert_eq!(
        res.headers.get("access-control-allow-credentials").unwrap(),
        "true"
    );
    assert_eq!(res.headers.get("access-control-max-age").unwrap(), "600");
    assert!(res.headers.get("access-control-allow-methods").is_some());
    assert_eq!(
        res.headers.get("access-control-allow-headers").unwrap(),
        "x-custom"
    );

    // Normal request from an allowed origin gets the grant appended.
    let res = client
        .get("/data")
        .header("origin", "https://app.example.com")
        .send()
        .await;
    assert_eq!(res.body_text(), "data");
    assert_eq!(
        res.headers.get("access-control-allow-origin").unwrap(),
        "https://app.example.com"
    );
    assert_eq!(res.headers.get("vary").unwrap(), "Origin");

    // Disallowed origin: no grant emitted.
    let res = client
        .get("/data")
        .header("origin", "https://evil.example")
        .send()
        .await;
    assert!(res.headers.get("access-control-allow-origin").is_none());

    // Same-origin/non-CORS request untouched.
    let res = client.get("/data").send().await;
    assert!(res.headers.get("access-control-allow-origin").is_none());
}

#[tokio::test]
async fn router_guards_block_requests_and_scoped_fallbacks_handle_misses() {
    let mut api = Router::new();
    api.guard(|req: &Request| req.header("x-api-key") == Some("secret"));
    api.get("/private", |_req: Request| Response::send("private"));
    api.fallback(|_req: Request| Response::send("fallback api").status(404));

    let mut app = App::new();
    app.mount("/api", api);

    let blocked = app
        .dispatch(request_with_method("GET", "/api/private"))
        .await;
    assert_eq!(blocked.status, 403);

    let mut allowed_req = request_with_method("GET", "/api/private");
    allowed_req
        .headers
        .insert("x-api-key".to_string(), "secret".to_string());
    let allowed = app.dispatch(allowed_req).await;
    assert_eq!(allowed.body_text(), "private");

    let mut fallback_req = request_with_method("GET", "/api/not-found");
    fallback_req
        .headers
        .insert("x-api-key".to_string(), "secret".to_string());
    let fallback = app.dispatch(fallback_req).await;
    assert_eq!(fallback.status, 404);
    assert_eq!(fallback.body_text(), "fallback api");
}

#[tokio::test]
async fn error_handler_formats_404_and_405() {
    #[derive(Serialize)]
    struct ErrorBody {
        error: String,
        status: u16,
    }

    let mut app = App::new();
    app.error_handler(|err: HttpError| {
        Response::json(&ErrorBody {
            error: err.message().to_string(),
            status: err.status(),
        })
        .status(err.status())
    });
    app.get("/exists", |_r: Request| Response::send("ok"));

    // Unmatched route (404) flows through the error handler.
    let res = app.dispatch(request_with_method("GET", "/missing")).await;
    assert_eq!(res.status, 404);
    assert_eq!(res.content_type, "application/json");
    assert!(res.body_text().contains("\"status\":404"));

    // Method mismatch (405) flows through the error handler too.
    let res = app.dispatch(request_with_method("POST", "/exists")).await;
    assert_eq!(res.status, 405);
    assert_eq!(res.content_type, "application/json");
}

#[tokio::test]
async fn response_formats_sse_events() {
    let events = stream::iter(vec![
        SseEvent::new("hello").event("greeting").id("1"),
        SseEvent::new("goodbye"),
    ]);
    let res = Response::sse(events).into_hyper();

    assert_eq!(
        res.headers().get("content-type").unwrap(),
        "text/event-stream"
    );

    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        String::from_utf8_lossy(&body),
        "id: 1\nevent: greeting\ndata: hello\n\ndata: goodbye\n\n"
    );
}

#[test]
fn sse_comment_events_format_as_comments() {
    assert_eq!(SseEvent::comment("keep-alive").format(), ": keep-alive\n\n");
    // Regular events keep their existing shape.
    assert_eq!(
        SseEvent::new("hola").id("1").format(),
        "id: 1\ndata: hola\n\n"
    );
}

#[test]
fn request_exposes_last_event_id() {
    let mut req = dummy_request("");
    assert!(req.last_event_id().is_none());
    req.headers
        .insert("last-event-id".to_string(), "42".to_string());
    assert_eq!(req.last_event_id(), Some("42"));
}

#[tokio::test]
async fn sse_with_heartbeat_fills_idle_gaps_and_ends_with_source() {
    // One immediate event, then a gap several heartbeats long.
    let events = stream::unfold(0, |state| async move {
        match state {
            0 => Some((SseEvent::new("primero"), 1)),
            1 => {
                tokio::time::sleep(std::time::Duration::from_millis(120)).await;
                Some((SseEvent::new("segundo"), 2))
            }
            _ => None,
        }
    });
    let res = Response::sse_with_heartbeat(events, std::time::Duration::from_millis(40));
    assert_eq!(res.content_type, "text/event-stream");

    // Collecting returns only because the merged stream ends with the source.
    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("data: primero"), "body: {text}");
    assert!(
        text.contains(": keep-alive"),
        "expected heartbeat in {text}"
    );
    assert!(text.contains("data: segundo"), "body: {text}");
}

#[tokio::test]
async fn gzip_middleware_skips_websocket_upgrade_responses() {
    let mut app = App::new();
    app.layer(middleware::gzip());
    app.get("/ws", |req: Request| Response::websocket(&req).unwrap());

    let mut req = request_with_method("GET", "/ws");
    req.headers
        .insert("accept-encoding".to_string(), "gzip".to_string());
    req.headers
        .insert("upgrade".to_string(), "websocket".to_string());
    req.headers
        .insert("connection".to_string(), "Upgrade".to_string());
    req.headers.insert(
        "sec-websocket-key".to_string(),
        "dGhlIHNhbXBsZSBub25jZQ==".to_string(),
    );
    req.headers
        .insert("sec-websocket-version".to_string(), "13".to_string());

    let res = app.dispatch(req).await.into_hyper();

    assert_eq!(res.status(), 101);
    assert!(res.headers().get(CONTENT_ENCODING).is_none());
}

fn request_with_method(method: &str, path: &str) -> Request {
    let mut req = dummy_request("");
    req.method = method.to_string();
    req.path = path.to_string();
    req
}

#[test]
fn response_body_accessors_and_no_desync_for_streams() {
    let bytes_res = Response::send("hi");
    assert_eq!(bytes_res.status, 200);
    assert_eq!(bytes_res.content_type, "text/plain; charset=utf-8");
    assert_eq!(bytes_res.body_bytes(), Some(&b"hi"[..]));
    assert_eq!(bytes_res.body_text(), "hi");

    // A streamed response keeps no in-memory body to desync from its stream.
    let stream_res = Response::stream(stream::iter(vec![Ok(Bytes::from_static(b"x"))]));
    assert_eq!(stream_res.body_bytes(), None);
}

#[test]
fn request_builder_builds_full_request() {
    struct Cfg {
        name: &'static str,
    }

    let req = Request::builder()
        .method("POST")
        .path("/users/42?active=true&tag=a&tag=b")
        .header("X-Tag", "uno")
        .header("x-tag", "dos")
        .cookie("sid", "abc")
        .param("id", "42")
        .state(Cfg { name: "test" })
        .body(r#"{"n":1}"#)
        .build();

    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/users/42");
    assert_eq!(req.query("active"), Some("true"));
    assert_eq!(req.query_all("tag"), vec!["a", "b"]);
    // Header names are lowercased like the real server; map keeps last value,
    // headers_all keeps every value.
    assert_eq!(req.header("X-Tag"), Some("dos"));
    assert_eq!(req.headers_all("x-tag"), vec!["uno", "dos"]);
    assert_eq!(req.cookie("sid"), Some("abc"));
    assert_eq!(req.param("id"), Some("42"));
    assert_eq!(req.bytes(), br#"{"n":1}"#);
    assert_eq!(req.state::<Cfg>().unwrap().name, "test");
}

#[tokio::test]
async fn test_client_drives_app_without_tcp() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        let res = next(req).await;
        res.header("x-mw", "ran")
    });
    app.get("/hello/:name", |req: Request| {
        let name = req.param("name").unwrap_or("?");
        let lang = req.query("lang").unwrap_or("en");
        Response::send(&format!("hola {} ({})", name, lang))
    });
    app.post("/echo", |req: Request| Response::send(&req.text()));

    let client = TestClient::new(app);

    let res = client.get("/hello/ada?lang=es").send().await;
    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "hola ada (es)");
    assert_eq!(res.headers.get("x-mw").unwrap(), "ran");

    let res = client.post("/echo").body("ping").send().await;
    assert_eq!(res.body_text(), "ping");

    let res = client.get("/missing").send().await;
    assert_eq!(res.status, 404);
}

#[tokio::test]
async fn test_client_honors_body_limit_and_timeout() {
    let mut app = App::new();
    app.max_body_size(4);
    app.request_timeout(std::time::Duration::from_millis(30));
    app.post("/up", |_r: Request| Response::send("ok"));
    app.get("/slow", |_r: Request| async {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        Response::send("late")
    });

    let client = TestClient::new(app);

    let res = client.post("/up").body("way too large").send().await;
    assert_eq!(res.status, 413);

    let res = client.get("/slow").send().await;
    assert_eq!(res.status, 408);
}

#[test]
fn send_sets_text_plain_and_200() {
    let res = Response::send("hello");
    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "hello");
    assert_eq!(res.content_type, "text/plain; charset=utf-8");
}

#[test]
fn not_found_sets_404() {
    let res = Response::not_found();
    assert_eq!(res.status, 404);
    assert_eq!(res.body_text(), "404 Not Found");
}

#[test]
fn bad_request_sets_400() {
    let res = Response::bad_request();
    assert_eq!(res.status, 400);
    assert_eq!(res.content_type, "text/plain; charset=utf-8");
}

#[test]
fn json_serializes_value_with_200_and_json_content_type() {
    #[derive(Serialize)]
    struct User {
        id: u32,
        name: &'static str,
    }
    let res = Response::json(&User { id: 1, name: "Ada" });
    assert_eq!(res.status, 200);
    assert_eq!(res.content_type, "application/json");
    assert_eq!(res.body_text(), r#"{"id":1,"name":"Ada"}"#);
}

#[test]
fn json_serialization_error_degrades_to_500() {
    // serde_json cannot serialize a map with non-string (tuple) keys.
    let mut map: HashMap<(i32, i32), i32> = HashMap::new();
    map.insert((1, 2), 3);
    let res = Response::json(&map);
    assert_eq!(res.status, 500);
    assert_eq!(res.content_type, "text/plain; charset=utf-8");
}

#[test]
fn into_hyper_maps_status_and_content_type_header() {
    let res = Response::send("hi").into_hyper();
    assert_eq!(res.status(), 200);
    assert_eq!(
        res.headers().get(hyper::header::CONTENT_TYPE).unwrap(),
        "text/plain; charset=utf-8"
    );
}

#[test]
fn response_allows_arbitrary_headers() {
    let res = Response::send("ok")
        .header("x-trace-id", "abc-123")
        .into_hyper();

    assert_eq!(res.headers().get("x-trace-id").unwrap(), "abc-123");
}

#[test]
fn response_redirect_sets_location_header() {
    let res = Response::redirect("/login").into_hyper();

    assert_eq!(res.status(), 302);
    assert_eq!(res.headers().get(LOCATION).unwrap(), "/login");
}

#[test]
fn response_cookie_appends_set_cookie_headers() {
    let res = Response::send("ok")
        .cookie("sid", "abc")
        .cookie("theme", "dark")
        .into_hyper();
    let cookies: Vec<_> = res
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect();

    assert_eq!(cookies.len(), 2);
    assert!(cookies.iter().any(|value| value.starts_with("sid=abc")));
    assert!(cookies.iter().any(|value| value.starts_with("theme=dark")));
}

#[test]
fn cookie_builder_renders_attributes() {
    let header = Cookie::new("sid", "abc")
        .domain("example.com")
        .max_age_secs(3600)
        .secure(true)
        .http_only(true)
        .same_site(SameSite::Strict)
        .to_header_value();
    assert_eq!(
        header,
        "sid=abc; Path=/; Domain=example.com; Max-Age=3600; Secure; HttpOnly; SameSite=Strict"
    );

    let res = Response::send("ok")
        .set_cookie(Cookie::new("a", "1"))
        .clear_cookie("old");
    let cookies: Vec<_> = res
        .headers
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect();
    assert!(cookies.contains(&"a=1; Path=/".to_string()), "{cookies:?}");
    assert!(
        cookies.contains(&"old=; Path=/; Max-Age=0".to_string()),
        "{cookies:?}"
    );
}

#[test]
fn signed_values_roundtrip_and_reject_tampering() {
    let signed = sign_value("secret", "user42");
    assert_ne!(signed, "user42");
    assert_eq!(verify_value("secret", &signed).as_deref(), Some("user42"));
    assert_eq!(verify_value("wrong-secret", &signed), None);
    assert_eq!(verify_value("secret", "user42.forged"), None);
    assert_eq!(verify_value("secret", "no-signature"), None);
}

#[tokio::test]
async fn sessions_middleware_assigns_and_persists_session() {
    let sessions = Sessions::new("top-secret");
    let mut app = App::new();
    app.layer(sessions.middleware());
    let store = sessions.clone();
    app.get("/visit", move |req: Request| {
        let id = req.session_id().expect("session id set").to_string();
        let visits = store
            .get(&id, "visits")
            .and_then(|count| count.parse::<u32>().ok())
            .unwrap_or(0)
            + 1;
        store.set(&id, "visits", &visits.to_string());
        Response::send(&visits.to_string())
    });

    let client = TestClient::new(app);

    // First visit creates the session and sets a signed cookie.
    let res = client.get("/visit").send().await;
    assert_eq!(res.body_text(), "1");
    let set_cookie = res
        .headers
        .get(SET_COOKIE)
        .expect("session cookie set")
        .to_str()
        .unwrap()
        .to_string();
    let (name, value) = set_cookie
        .split(';')
        .next()
        .unwrap()
        .split_once('=')
        .unwrap();

    // Replaying the cookie resumes the same session.
    let res = client.get("/visit").cookie(name, value).send().await;
    assert_eq!(res.body_text(), "2");

    // A tampered cookie gets a fresh session (and a new Set-Cookie).
    let res = client.get("/visit").cookie(name, "forged.sig").send().await;
    assert_eq!(res.body_text(), "1");
    assert!(res.headers.get(SET_COOKIE).is_some());
}

#[test]
fn query_params_are_parsed_and_url_decoded() {
    let query = parse_query("q=rust+rest&tag=web&tag=api&empty=&flag&encoded=hello%20world");
    let req = Request {
        method: "GET".to_string(),
        path: "/buscar".to_string(),
        raw_query: Some(
            "q=rust+rest&tag=web&tag=api&empty=&flag&encoded=hello%20world".to_string(),
        ),
        query,
        headers: HashMap::new(),
        cookies: HashMap::new(),
        body: Bytes::new(),
        params: HashMap::new(),
        state: StateStore::default(),
        upgrade: None,
        remote_addr: None,
        header_pairs: Vec::new(),
    };

    assert_eq!(req.query("q"), Some("rust rest"));
    assert_eq!(req.query("empty"), Some(""));
    assert_eq!(req.query("flag"), Some(""));
    assert_eq!(req.query("encoded"), Some("hello world"));
    assert_eq!(req.query_all("tag"), vec!["web", "api"]);
}

#[test]
fn request_cookies_are_parsed_from_cookie_header() {
    let cookies = parse_cookies("sid=abc; theme=dark; empty=");
    let mut req = dummy_request("");
    req.cookies = cookies;

    assert_eq!(req.cookie("sid"), Some("abc"));
    assert_eq!(req.cookie("theme"), Some("dark"));
    assert_eq!(req.cookie("empty"), Some(""));
}

#[test]
fn request_json_deserializes_body() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct User {
        id: u32,
        name: String,
    }
    let req = dummy_request(r#"{"id":1,"name":"Ada"}"#);
    let user: User = req.json().unwrap();
    assert_eq!(
        user,
        User {
            id: 1,
            name: "Ada".to_string()
        }
    );
}

#[test]
fn request_json_errors_on_invalid_body() {
    let req = dummy_request("not json");
    assert!(req.json::<serde_json::Value>().is_err());
}

#[test]
fn request_form_parses_urlencoded_body() {
    #[derive(Deserialize)]
    struct Login {
        user: String,
        tags: Vec<String>,
    }

    let req = Request::builder()
        .method("POST")
        .header("content-type", "application/x-www-form-urlencoded")
        .body("user=ada+lovelace&tags=a&tags=b")
        .build();

    let form: Login = req.form().unwrap();
    assert_eq!(form.user, "ada lovelace");
    assert_eq!(form.tags, vec!["a", "b"]);

    let Form(extracted) = req.extract::<Form<Login>>().unwrap();
    assert_eq!(extracted.user, "ada lovelace");

    let bad = dummy_request("%%%not-a-form=%zz");
    assert!(bad.form::<Login>().is_err());
}

#[test]
fn request_multipart_parses_fields_and_binary_files() {
    let mut body = Vec::new();
    body.extend_from_slice(
        b"--XBOUND\r\ncontent-disposition: form-data; name=\"campo\"\r\n\r\nhola\r\n",
    );
    body.extend_from_slice(
        b"--XBOUND\r\ncontent-disposition: form-data; name=\"archivo\"; filename=\"a.bin\"\r\ncontent-type: application/octet-stream\r\n\r\n",
    );
    body.extend_from_slice(&[0xFF, 0x00, 0xFE]);
    body.extend_from_slice(b"\r\n--XBOUND--\r\n");

    let req = Request::builder()
        .method("POST")
        .header("content-type", "multipart/form-data; boundary=XBOUND")
        .body(body)
        .build();

    let parts = req.multipart().unwrap();
    assert_eq!(parts.len(), 2);

    assert_eq!(parts[0].name, "campo");
    assert_eq!(parts[0].filename, None);
    assert_eq!(parts[0].text(), "hola");

    assert_eq!(parts[1].name, "archivo");
    assert_eq!(parts[1].filename.as_deref(), Some("a.bin"));
    assert_eq!(
        parts[1].content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(&parts[1].data[..], &[0xFF, 0x00, 0xFE]);

    // Without a multipart content type the call fails cleanly.
    assert!(dummy_request("x").multipart().is_err());
}

#[test]
fn request_body_bytes_preserve_non_utf8_and_text_is_lossy() {
    // Bytes that are not valid UTF-8 must survive intact through `bytes()`,
    // while `text()` exposes a lossy view for text consumers.
    let raw: &[u8] = &[0xff, 0xfe, b'h', b'i'];
    let mut req = dummy_request("");
    req.body = Bytes::copy_from_slice(raw);

    assert_eq!(req.bytes(), raw);
    assert_eq!(req.text(), String::from_utf8_lossy(raw));
}

#[test]
fn match_pattern_captures_params_and_rejects_mismatches() {
    let pattern = parse_pattern("/users/:id/posts");
    let params = match_pattern(&pattern, &path_segments("/users/42/posts")).unwrap();
    assert_eq!(params.get("id").map(String::as_str), Some("42"));
    assert!(match_pattern(&pattern, &path_segments("/users/42")).is_none());
    assert!(match_pattern(&pattern, &path_segments("/users/42/comments")).is_none());
}

#[test]
fn router_matches_method_and_path_param() {
    let mut router = Router::new();
    router.get("/users", |_r: Request| Response::send("list"));
    router.get("/users/:id", |req: Request| {
        Response::send(req.param("id").unwrap_or("?"))
    });

    assert!(router.route("GET", "/users").is_some());
    assert!(router.route("POST", "/users").is_none());
    assert!(router.route("GET", "/nope/extra").is_none());

    let (_handler, _mws, params) = router.route("GET", "/users/42").expect("should match");
    assert_eq!(params.get("id").map(String::as_str), Some("42"));
}

#[test]
fn router_supports_extra_methods_and_all() {
    let mut router = Router::new();
    router.patch("/items/:id", |_r: Request| Response::send("patch"));
    router.options("/items", |_r: Request| Response::send("options"));
    router.head("/items", |_r: Request| Response::send("head"));
    router.all("/health", |_r: Request| Response::send("ok"));

    assert!(router.route("PATCH", "/items/1").is_some());
    assert!(router.route("OPTIONS", "/items").is_some());
    assert!(router.route("HEAD", "/items").is_some());
    assert!(router.route("GET", "/health").is_some());
    assert!(router.route("POST", "/health").is_some());
}

#[test]
fn mount_concatenates_prefixes_across_nesting() {
    let mut users = Router::new();
    users.get("/:id", |req: Request| {
        Response::send(req.param("id").unwrap_or("?"))
    });

    let mut api = Router::new();
    api.mount("/users", users);

    let mut root = Router::new();
    root.mount("/api", api);

    let (_handler, _mws, params) = root.route("GET", "/api/users/42").expect("should match");
    assert_eq!(params.get("id").map(String::as_str), Some("42"));
    // Only `/:id` was registered, so the bare collection path does not match.
    assert!(root.route("GET", "/api/users").is_none());
}

#[test]
fn router_prefers_static_over_param_regardless_of_registration_order() {
    let mut router = Router::new();
    // The param route is registered FIRST; specificity must still win.
    router.get("/users/:id", |req: Request| {
        Response::send(req.param("id").unwrap_or("?"))
    });
    router.get("/users/me", |_r: Request| Response::send("me"));

    let (_h, _m, params) = router.route("GET", "/users/me").expect("should match");
    assert!(
        params.is_empty(),
        "static /users/me should win over /users/:id, captured {params:?}"
    );

    let (_h, _m, params) = router.route("GET", "/users/42").expect("should match");
    assert_eq!(params.get("id").map(String::as_str), Some("42"));
}

#[test]
fn router_prefers_param_over_wildcard_and_backtracks_across_branches() {
    let mut router = Router::new();
    // Wildcard registered first; the more specific param route must win.
    router.get("/files/*rest", |_r: Request| Response::send("wild"));
    router.get("/files/:name", |_r: Request| Response::send("param"));

    let (_h, _m, params) = router.route("GET", "/files/readme").expect("should match");
    assert_eq!(params.get("name").map(String::as_str), Some("readme"));

    // Deeper paths only the wildcard can absorb.
    let (_h, _m, params) = router.route("GET", "/files/a/b").expect("should match");
    assert_eq!(params.get("rest").map(String::as_str), Some("a/b"));

    // A static branch that dead-ends must backtrack to the param route
    // (also exercises index invalidation after further registration).
    router.get("/users/me/profile", |_r: Request| Response::send("prof"));
    router.get("/users/:id", |req: Request| {
        Response::send(req.param("id").unwrap_or("?"))
    });
    let (_h, _m, params) = router.route("GET", "/users/me").expect("should match");
    assert_eq!(params.get("id").map(String::as_str), Some("me"));

    // Method-aware backtracking: POST /users/me must not shadow GET.
    router.post("/users/me", |_r: Request| Response::send("post me"));
    let (_h, _m, params) = router.route("GET", "/users/me").expect("should match");
    assert_eq!(params.get("id").map(String::as_str), Some("me"));
    let (_h, _m, params) = router.route("POST", "/users/me").expect("should match");
    assert!(params.is_empty());
}

#[tokio::test]
async fn router_prefers_exact_method_over_all_on_same_path() {
    let mut router = Router::new();
    // `.all()` registered first; an exact-method route must still win for GET.
    router.all("/health", |_r: Request| Response::send("all"));
    router.get("/health", |_r: Request| Response::send("get"));

    let (handler, _m, _p) = router.route("GET", "/health").expect("should match");
    assert_eq!(handler(dummy_request("")).await.body_text(), "get");

    let (handler, _m, _p) = router.route("DELETE", "/health").expect("should match");
    assert_eq!(handler(dummy_request("")).await.body_text(), "all");
}

#[test]
fn app_lists_registered_routes_for_introspection() {
    let mut app = App::new();
    app.get("/", |_r: Request| Response::send("root"));
    app.post("/users", |_r: Request| Response::send("create"));
    app.get("/users/:id", |_r: Request| Response::send("show"));
    let mut files = Router::new();
    files.get("/*path", |_r: Request| Response::send("file"));
    app.mount("/files", files);

    let listed: Vec<(String, String)> = app
        .routes()
        .iter()
        .map(|route| (route.method.clone(), route.path.clone()))
        .collect();

    assert!(listed.contains(&("GET".to_string(), "/".to_string())));
    assert!(listed.contains(&("POST".to_string(), "/users".to_string())));
    assert!(listed.contains(&("GET".to_string(), "/users/:id".to_string())));
    assert!(listed.contains(&("GET".to_string(), "/files/*path".to_string())));
}

#[tokio::test]
async fn trailing_slash_policy_controls_non_canonical_paths() {
    // Default (Ignore): a trailing slash still matches.
    let mut app = App::new();
    app.get("/users", |_r: Request| Response::send("list"));
    let res = app.dispatch(request_with_method("GET", "/users/")).await;
    assert_eq!(res.status, 200);

    // Strict: non-canonical paths 404; canonical ones and "/" are untouched.
    let mut app = App::new();
    app.trailing_slash(TrailingSlash::Strict);
    app.get("/users", |_r: Request| Response::send("list"));
    app.get("/", |_r: Request| Response::send("root"));
    assert_eq!(
        app.dispatch(request_with_method("GET", "/users/"))
            .await
            .status,
        404
    );
    assert_eq!(
        app.dispatch(request_with_method("GET", "/users"))
            .await
            .status,
        200
    );
    assert_eq!(
        app.dispatch(request_with_method("GET", "/")).await.status,
        200
    );

    // Redirect: 308 to the canonical path, preserving the query string.
    let mut app = App::new();
    app.trailing_slash(TrailingSlash::Redirect);
    app.get("/users", |_r: Request| Response::send("list"));
    let mut req = request_with_method("GET", "/users/");
    req.raw_query = Some("page=2".to_string());
    let res = app.dispatch(req).await;
    assert_eq!(res.status, 308);
    assert_eq!(res.headers.get("location").unwrap(), "/users?page=2");
}

#[tokio::test]
async fn openapi_document_and_docs_routes_are_served() {
    let mut app = App::new();
    app.get("/users", |_r: Request| Response::send("list"))
        .summary("Lista usuarios")
        .tag("users");
    app.post("/users", |_r: Request| Response::send("create"));
    app.get("/users/:id", |_r: Request| Response::send("show"));
    app.all("/health", |_r: Request| Response::send("ok"));
    app.serve_docs("/docs", "Mi API", "0.2.0");

    let doc = app.openapi("Mi API", "0.2.0");
    assert_eq!(doc["openapi"], "3.0.3");
    assert_eq!(doc["info"]["title"], "Mi API");
    assert_eq!(doc["info"]["version"], "0.2.0");
    assert!(doc["paths"]["/users"]["get"].is_object());
    assert!(doc["paths"]["/users"]["post"].is_object());
    assert_eq!(doc["paths"]["/users"]["get"]["summary"], "Lista usuarios");
    assert_eq!(doc["paths"]["/users"]["get"]["tags"][0], "users");
    let param = &doc["paths"]["/users/{id}"]["get"]["parameters"][0];
    assert_eq!(param["name"], "id");
    assert_eq!(param["in"], "path");
    assert_eq!(param["required"], true);
    // `all()` routes have no single HTTP method and are not listed.
    assert!(doc["paths"]["/health"].is_null());

    let client = TestClient::new(app);

    let spec = client.get("/docs/openapi.json").send().await;
    assert_eq!(spec.status, 200);
    assert_eq!(spec.content_type, "application/json");
    let body: serde_json::Value = serde_json::from_slice(spec.body_bytes().unwrap()).unwrap();
    assert!(body["paths"]["/users/{id}"]["get"].is_object());

    let ui = client.get("/docs").send().await;
    assert_eq!(ui.status, 200);
    assert!(ui.content_type.starts_with("text/html"));
    assert!(ui.body_text().contains("/docs/openapi.json"));
}

#[test]
fn router_index_refreshes_after_mount() {
    let mut root = Router::new();
    // Force a lookup (and any lazy index build) before mounting.
    assert!(root.route("GET", "/api/ping").is_none());

    let mut api = Router::new();
    api.get("/ping", |_r: Request| Response::send("pong"));
    root.mount("/api", api);

    assert!(root.route("GET", "/api/ping").is_some());
}

#[tokio::test]
async fn dispatch_runs_sync_handler() {
    let mut app = App::new();
    app.get("/", |_r: Request| Response::send("sync"));
    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "sync");
}

#[tokio::test]
async fn dispatch_runs_async_handler() {
    let mut app = App::new();
    app.get("/", |_r: Request| async move { Response::send("async") });
    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "async");
}

#[tokio::test]
async fn dispatch_accepts_result_handlers() {
    let mut app = App::new();
    app.get("/", |_r: Request| -> Result<Response, &'static str> {
        Ok(Response::send("ok"))
    });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "ok");
}

#[tokio::test]
async fn dispatch_converts_handler_errors_to_500() {
    let mut app = App::new();
    app.get("/", |_r: Request| -> Result<Response, &'static str> {
        Err("fallo")
    });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.status, 500);
}

#[tokio::test]
async fn dispatch_catches_panics_as_500_responses() {
    let mut app = App::new();
    app.get("/", |_r: Request| -> Response { panic!("boom") });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.status, 500);
}

#[tokio::test]
async fn request_can_access_shared_state() {
    struct Config {
        app_name: &'static str,
    }

    let mut app = App::new();
    app.state(Config {
        app_name: "rustrest",
    });
    app.get("/", |req: Request| {
        let config = req.state::<Config>().expect("state exists");
        Response::send(config.app_name)
    });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.body_text(), "rustrest");
}

#[tokio::test]
async fn dispatch_unmatched_returns_404() {
    let app = App::new();
    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.status, 404);
}

#[tokio::test]
async fn unmatched_method_returns_405_with_allow() {
    let mut app = App::new();
    app.get("/only", |_r: Request| Response::send("get"));

    let res = app.dispatch(request_with_method("POST", "/only")).await;

    assert_eq!(res.status, 405);
    assert_eq!(res.headers.get("allow").unwrap(), "GET, HEAD, OPTIONS");
}

#[tokio::test]
async fn head_is_auto_served_from_get() {
    let mut app = App::new();
    app.get("/page", |_r: Request| Response::send("hello"));

    let res = app.dispatch(request_with_method("HEAD", "/page")).await;

    // Matched via the GET route; body stripped because the method is HEAD.
    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "");
}

#[tokio::test]
async fn options_is_auto_answered_with_allow() {
    let mut app = App::new();
    app.get("/thing", |_r: Request| Response::send("g"));
    app.post("/thing", |_r: Request| Response::send("p"));

    let res = app.dispatch(request_with_method("OPTIONS", "/thing")).await;

    assert_eq!(res.status, 204);
    let allow = res.headers.get("allow").unwrap().to_str().unwrap();
    assert!(allow.contains("GET"), "allow: {allow}");
    assert!(allow.contains("POST"), "allow: {allow}");
    assert!(allow.contains("OPTIONS"), "allow: {allow}");
}

#[tokio::test]
async fn middleware_wraps_handler_and_runs() {
    let mut app = App::new();
    app.get("/", |_r: Request| Response::send("handler"));

    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    app.layer(move |req: Request, next: Next| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            next(req).await
        }
    });

    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "handler");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn middleware_can_short_circuit() {
    let mut app = App::new();
    app.get("/", |_r: Request| Response::send("handler"));
    app.layer(|_req: Request, _next: Next| async move { Response::send("blocked") });

    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "blocked");
}

#[tokio::test]
async fn middlewares_nest_in_registration_order() {
    let mut app = App::new();
    app.get("/", |_r: Request| Response::send("h"));

    let order = Arc::new(Mutex::new(Vec::<&'static str>::new()));
    let o1 = Arc::clone(&order);
    app.layer(move |req: Request, next: Next| {
        let o1 = Arc::clone(&o1);
        async move {
            o1.lock().unwrap().push("mw1-in");
            let res = next(req).await;
            o1.lock().unwrap().push("mw1-out");
            res
        }
    });
    let o2 = Arc::clone(&order);
    app.layer(move |req: Request, next: Next| {
        let o2 = Arc::clone(&o2);
        async move {
            o2.lock().unwrap().push("mw2-in");
            let res = next(req).await;
            o2.lock().unwrap().push("mw2-out");
            res
        }
    });

    let _ = app.dispatch(dummy_request("")).await;
    assert_eq!(
        *order.lock().unwrap(),
        vec!["mw1-in", "mw2-in", "mw2-out", "mw1-out"]
    );
}

#[tokio::test]
async fn per_route_middleware_applies_only_to_that_route() {
    let hits = Arc::new(AtomicUsize::new(0));

    let mut app = App::new();
    let counter = Arc::clone(&hits);
    app.get("/guarded", |_r: Request| Response::send("guarded"))
        .layer(move |req: Request, next: Next| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                next(req).await
            }
        });
    app.get("/open", |_r: Request| Response::send("open"));

    let res = app.dispatch(request_with_method("GET", "/guarded")).await;
    assert_eq!(res.body_text(), "guarded");
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // A different route does not run the per-route middleware.
    let res = app.dispatch(request_with_method("GET", "/open")).await;
    assert_eq!(res.body_text(), "open");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn router_layer_scopes_middleware_to_its_routes() {
    let hits = Arc::new(AtomicUsize::new(0));

    let mut scoped = Router::new();
    let counter = Arc::clone(&hits);
    scoped.layer(move |req: Request, next: Next| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            next(req).await
        }
    });
    scoped.get("/thing", |_r: Request| Response::send("scoped"));

    let mut app = App::new();
    app.get("/", |_r: Request| Response::send("root"));
    app.mount("/api", scoped);

    // A request under the mount runs the scoped middleware.
    let mut req = dummy_request("");
    req.path = "/api/thing".to_string();
    let res = app.dispatch(req).await;
    assert_eq!(res.body_text(), "scoped");
    assert_eq!(hits.load(Ordering::SeqCst), 1);

    // A request outside the mount does not.
    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "root");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn app_static_files_serves_files_with_content_type() {
    let root = std::env::temp_dir().join(format!(
        "rustrest-static-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("app.css"), "body { color: red; }").unwrap();

    let mut app = App::new();
    app.static_files("/assets", &root);

    let res = app
        .dispatch(request_with_method("GET", "/assets/app.css"))
        .await;

    assert_eq!(res.status, 200);
    assert_eq!(res.content_type, "text/css; charset=utf-8");
    // Files are streamed, so read the body from the hyper response.
    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"body { color: red; }");

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn static_files_support_conditional_and_range_requests() {
    let root = std::env::temp_dir().join(format!(
        "rustrest-static-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("static.txt"), "0123456789").unwrap();

    let mut app = App::new();
    app.static_files("/assets", &root);
    let client = TestClient::new(app);

    // Full GET: 200 with validators and a streamed, exact-length body.
    let res = client.get("/assets/static.txt").send().await;
    assert_eq!(res.status, 200);
    assert_eq!(res.headers.get("content-length").unwrap(), "10");
    assert_eq!(res.headers.get("accept-ranges").unwrap(), "bytes");
    assert!(res.headers.get("last-modified").is_some());
    let etag = res
        .headers
        .get("etag")
        .expect("etag set")
        .to_str()
        .unwrap()
        .to_string();
    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"0123456789");

    // If-None-Match revalidation -> 304.
    let res = client
        .get("/assets/static.txt")
        .header("if-none-match", &etag)
        .send()
        .await;
    assert_eq!(res.status, 304);

    // Byte range -> 206 with Content-Range.
    let res = client
        .get("/assets/static.txt")
        .header("range", "bytes=2-5")
        .send()
        .await;
    assert_eq!(res.status, 206);
    assert_eq!(res.headers.get("content-range").unwrap(), "bytes 2-5/10");
    assert_eq!(res.headers.get("content-length").unwrap(), "4");
    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"2345");

    // Suffix range (last N bytes).
    let res = client
        .get("/assets/static.txt")
        .header("range", "bytes=-3")
        .send()
        .await;
    assert_eq!(res.status, 206);
    assert_eq!(res.headers.get("content-range").unwrap(), "bytes 7-9/10");
    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"789");

    // Unsatisfiable range -> 416 with the total size.
    let res = client
        .get("/assets/static.txt")
        .header("range", "bytes=50-")
        .send()
        .await;
    assert_eq!(res.status, 416);
    assert_eq!(res.headers.get("content-range").unwrap(), "bytes */10");

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn static_files_rejects_path_traversal() {
    let root = std::env::temp_dir().join(format!(
        "rustrest-static-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();

    let mut app = App::new();
    app.static_files("/assets", &root);

    let res = app
        .dispatch(request_with_method("GET", "/assets/../secret.txt"))
        .await;

    assert_eq!(res.status, 400);

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn response_streams_body_chunks() {
    let chunks = stream::iter(vec![
        Ok(Bytes::from_static(b"hello ")),
        Ok(Bytes::from_static(b"stream")),
    ]);
    let res = Response::stream(chunks)
        .content_type("text/plain; charset=utf-8")
        .into_hyper();
    let body = res.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(&body[..], b"hello stream");
}

#[tokio::test]
async fn handle_strips_body_for_head_requests() {
    let mut app = App::new();
    app.head("/", |_r: Request| Response::send("no body"));

    let res = app.dispatch(request_with_method("HEAD", "/")).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "");
}
