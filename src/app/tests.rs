use super::router::{match_pattern, parse_pattern, path_segments};
use super::*;
use futures_util::stream;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use hyper::header::{CONTENT_ENCODING, LOCATION, SEC_WEBSOCKET_ACCEPT, SET_COOKIE};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
    }
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
fn websocket_handshake_sets_upgrade_headers() {
    let mut req = dummy_request("");
    req.method = "GET".to_string();
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

    assert!(req.is_websocket_upgrade());

    let res = Response::websocket(&req).unwrap().into_hyper();

    assert_eq!(res.status(), 101);
    assert_eq!(
        res.headers().get(SEC_WEBSOCKET_ACCEPT).unwrap(),
        "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
    );
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
    assert_eq!(res.body_text(), "body { color: red; }");
    assert_eq!(res.content_type, "text/css; charset=utf-8");

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
