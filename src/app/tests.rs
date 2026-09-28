use super::router::{MatchedRoute, match_pattern, parse_pattern, path_segments};
use super::*;
use futures_util::stream;
use http_body_util::BodyExt;
use hyper::body::Bytes;
use hyper::header::{
    ALLOW, CACHE_CONTROL, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue,
    LOCATION, RETRY_AFTER, SEC_WEBSOCKET_VERSION, SET_COOKIE, TRAILER, TRANSFER_ENCODING, VARY,
    WWW_AUTHENTICATE,
};
use hyper::{HeaderMap, Method, StatusCode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const TEST_SESSION_SECRET: &str = "0123456789abcdef0123456789abcdef";

fn dummy_request(body: &str) -> Request {
    Request::builder().body(body.to_string()).build()
}

fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    std::env::temp_dir().join(format!(
        "rustrest-{prefix}-{}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn http_error_preserves_code_headers_and_private_source() {
    let error = HttpError::new(
        StatusCode::TOO_MANY_REQUESTS,
        "rate_limited",
        "Demasiadas solicitudes",
    )
    .header(RETRY_AFTER, HeaderValue::from_static("30"))
    .with_source(std::io::Error::other("redis unavailable"));

    assert_eq!(error.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(error.code(), "rate_limited");
    assert_eq!(error.public_message(), "Demasiadas solicitudes");
    assert_eq!(error.headers().get(RETRY_AFTER).unwrap(), "30");
    assert_eq!(error.source().unwrap().to_string(), "redis unavailable");
}

#[test]
fn problem_details_never_exposes_internal_source() {
    let response = Response::from_error(
        HttpError::internal_server_error("Error interno")
            .with_source(std::io::Error::other("database password leaked")),
    );
    let json: serde_json::Value = serde_json::from_slice(response.body_bytes().unwrap()).unwrap();

    assert_eq!(json["status"], 500);
    assert_eq!(json["code"], "internal_server_error");
    assert_eq!(json["detail"], "Error interno");
    assert!(!response.body_text().contains("database password"));
}

#[test]
fn unauthorized_problem_preserves_authenticate_header() {
    let response = Response::from_error(HttpError::unauthorized("Autenticacion requerida").header(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer realm=\"api\""),
    ));

    assert_eq!(
        response.headers.get(WWW_AUTHENTICATE).unwrap(),
        "Bearer realm=\"api\""
    );
}

#[test]
fn problem_response_preserves_duplicate_authenticate_headers() {
    let response = Response::from_error(
        HttpError::unauthorized("Autenticacion requerida")
            .header(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"api\""),
            )
            .append_header(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"legacy\""),
            ),
    );

    let values: Vec<_> = response
        .headers
        .get_all(WWW_AUTHENTICATE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect();
    assert_eq!(values, ["Bearer realm=\"api\"", "Basic realm=\"legacy\""]);
}

#[test]
fn http_error_header_replaces_duplicate_values() {
    let error = HttpError::unauthorized("Autenticacion requerida")
        .append_header(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Bearer realm=\"api\""),
        )
        .append_header(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"legacy\""),
        )
        .header(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Digest realm=\"replacement\""),
        );

    let values: Vec<_> = error
        .headers()
        .get_all(WWW_AUTHENTICATE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect();
    assert_eq!(values, ["Digest realm=\"replacement\""]);
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
    for invalid in [
        WebSocketConfig::new().pong_timeout(Duration::ZERO),
        WebSocketConfig::new().idle_timeout(Duration::ZERO),
        WebSocketConfig::new().max_connection_lifetime(Duration::ZERO),
        WebSocketConfig::new().pong_timeout(Duration::MAX),
        WebSocketConfig::new().close_timeout(Duration::MAX),
    ] {
        assert!(invalid.validate().is_err());
    }
}

#[test]
fn websocket_routes_validate_against_app_defaults_before_serving() {
    let mut app = App::new();
    app.websocket_defaults(WebSocketConfig::new().outbound_capacity(0));
    let _ = app.websocket("/ws", |_socket| async move {});

    let error = app
        .validate_websockets()
        .expect_err("invalid defaults must reject startup");

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn websocket_dispatch_uses_app_runtime_and_releases_failed_spawn() {
    let mut app = App::new();
    let _ = app.websocket("/ws", |_socket| async move {});
    let runtime = app.websocket_runtime();

    let request = Request::builder()
        .method("GET")
        .path("/ws")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .remote_addr("127.0.0.1:4501".parse().unwrap())
        .build();

    let response = app.dispatch(request).await;

    assert_eq!(response.status, 400);
    assert_eq!(runtime.stats().accepted_connections, 1);
    assert_eq!(runtime.stats().active_connections, 0);
    assert_eq!(runtime.stats().closed_connections, 1);
}

#[tokio::test]
async fn websocket_handshake_rejections_use_global_error_handler() {
    let mut app = App::new();
    app.error_handler(|err: HttpError| {
        let status = err.status();
        Response::json(&serde_json::json!({ "code": err.code() })).status(status.as_u16())
    });
    let _ = app.websocket("/ws", |_socket| async move {});

    let request = Request::builder()
        .method("GET")
        .path("/ws")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "12")
        .build();

    let response = app.dispatch(request).await;

    assert_eq!(response.status, 426);
    assert_eq!(response.content_type, "application/json");
    assert_eq!(response.body_text(), r#"{"code":"upgrade_required"}"#);
    assert_eq!(response.headers.get(SEC_WEBSOCKET_VERSION).unwrap(), "13");
}

#[tokio::test]
async fn websocket_admission_rejections_use_global_error_handler() {
    let defaults = WebSocketConfig::new().max_connections_per_ip(1);
    let mut app = App::new();
    app.websocket_defaults(defaults.clone());
    app.error_handler(|err: HttpError| {
        let status = err.status();
        Response::json(&serde_json::json!({ "code": err.code() })).status(status.as_u16())
    });
    let _ = app.websocket("/ws", |_socket| async move {});

    let runtime = app.websocket_runtime();
    let config =
        super::websocket::ResolvedWebSocketConfig::from_layers(&defaults, &WebSocketConfig::new());
    let _permit = runtime
        .admit(
            "/ws",
            Some("127.0.0.1:4501".parse().unwrap()),
            None,
            &config,
        )
        .unwrap();
    let request = Request::builder()
        .method("GET")
        .path("/ws")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .remote_addr("127.0.0.1:4502".parse().unwrap())
        .build();

    let response = app.dispatch(request).await;

    assert_eq!(response.status, 429);
    assert_eq!(response.content_type, "application/json");
    assert_eq!(response.body_text(), r#"{"code":"websocket_ip_capacity"}"#);
    assert_eq!(response.headers.get(RETRY_AFTER).unwrap(), "1");
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
fn websocket_same_host_origin_uses_transport_default_port() {
    let policy = OriginPolicy::same_host().allow_missing(false);

    assert!(policy.allows_for_transport(Some("http://app.example.com"), "app.example.com", false,));
    assert!(!policy.allows_for_transport(
        Some("https://app.example.com"),
        "app.example.com",
        false,
    ));
    assert!(policy.allows_for_transport(Some("https://app.example.com"), "app.example.com", true,));
    assert!(!policy.allows_for_transport(Some("http://app.example.com"), "app.example.com", true,));
}

#[test]
fn request_builder_defaults_to_http_11_and_can_mark_secure_transport() {
    let plain = Request::builder().build();
    assert_eq!(plain.version(), hyper::Version::HTTP_11);
    assert!(!plain.is_secure());

    let secure = Request::builder().secure(true).build();
    assert!(secure.is_secure());
}

#[tokio::test]
async fn mounted_websocket_request_records_normalized_route_pattern() {
    let mut chat = Router::new();
    let _ = chat.websocket("/:channel", |_socket| async move {});

    let mut api = Router::new();
    let _ = api.mount("/chat", chat);

    let mut app = App::new();
    let _ = app.mount("/api", api);
    app.layer(|req: Request, _next: Next| async move {
        Response::send(req.route_pattern().unwrap_or("missing"))
    });

    let response = app
        .dispatch(request_with_method("GET", "/api/chat/42"))
        .await;

    assert_eq!(response.body_text(), "/api/chat/:channel");
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

#[tokio::test]
async fn typed_extractors_read_json_path_query_and_state() {
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
    req.set_header("content-type", "application/json").unwrap();
    req.raw_query = Some("active=true&tag=rust&tag=http".to_string());
    req.query = parse_query(req.raw_query.as_deref().unwrap());
    req.params.insert("id".to_string(), "42".to_string());
    req.state = state;

    let Json(user) = req.extract::<Json<CreateUser>>().await.unwrap();
    let Path(path) = req.extract_parts::<Path<UserPath>>().await.unwrap();
    let Query(query) = req.extract_parts::<Query<UserQuery>>().await.unwrap();
    let State(config) = req.extract_parts::<State<Config>>().await.unwrap();

    assert_eq!(user.name, "Ada");
    assert_eq!(path.id, 42);
    assert!(query.active);
    assert_eq!(query.tag, vec!["rust", "http"]);
    assert_eq!(config.app_name, "rustrest");
}

#[tokio::test]
async fn extra_extractors_cover_scalars_bodies_wrappers_and_maps() {
    #[derive(Deserialize)]
    struct MyCookies {
        sid: String,
    }
    #[derive(Deserialize)]
    struct MyHeaders {
        #[serde(rename = "x-api-key")]
        key: String,
    }

    let mut parts_req = dummy_request("cuerpo");
    parts_req.params.insert("id".to_string(), "42".to_string());
    parts_req.set_cookie("sid", "abc").unwrap();
    parts_req.set_header("x-api-key", "k1").unwrap();

    // Scalar Path for single-param routes (numbers and strings).
    let Path(id) = parts_req.extract_parts::<Path<u32>>().await.unwrap();
    assert_eq!(id, 42);
    let Path(raw) = parts_req.extract_parts::<Path<String>>().await.unwrap();
    assert_eq!(raw, "42");

    // Raw body extractors.
    let mut req = dummy_request("cuerpo");
    let bytes = req.extract::<Bytes>().await.unwrap();
    assert_eq!(&bytes[..], b"cuerpo");
    let mut req = dummy_request("cuerpo");
    let text = req.extract::<String>().await.unwrap();
    assert_eq!(text, "cuerpo");

    // Option only suppresses a genuine absence. Invalid or unsupported input
    // is still rejected instead of silently becoming None.
    let mut req = dummy_request("not json");
    let invalid = match req.extract::<Option<Json<serde_json::Value>>>().await {
        Ok(_) => panic!("invalid optional JSON unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(invalid.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let mut req = dummy_request("");
    let missing: Option<TypedHeader<headers::UserAgent>> = req.extract_parts().await.unwrap();
    assert!(missing.is_none());
    let mut req = dummy_request("not json");
    let failed: Result<Json<serde_json::Value>, HttpError> = req.extract().await.unwrap();
    assert!(failed.is_err());

    // Typed cookie/header maps.
    let Cookies(cookies) = parts_req
        .extract_parts::<Cookies<MyCookies>>()
        .await
        .unwrap();
    assert_eq!(cookies.sid, "abc");
    let Headers(headers) = parts_req
        .extract_parts::<Headers<MyHeaders>>()
        .await
        .unwrap();
    assert_eq!(headers.key, "k1");
}

#[tokio::test]
async fn request_header_mutations_keep_typed_and_duplicate_views_synchronized() {
    let mut req = Request::builder()
        .header("authorization", "Bearer original")
        .header("x-tag", "uno")
        .build();

    req.append_header("x-tag", "dos").unwrap();
    assert_eq!(req.header("x-tag"), Some("dos"));
    assert_eq!(req.headers_all("x-tag"), vec!["uno", "dos"]);

    req.set_header("authorization", "Bearer replacement")
        .unwrap();
    let TypedHeader(auth) = req
        .extract_parts::<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>()
        .await
        .unwrap();
    assert_eq!(auth.token(), "replacement");

    req.remove_header("authorization");
    assert!(req.header("authorization").is_none());
    assert!(req.headers_all("authorization").is_empty());
    let error = match req
        .extract_parts::<TypedHeader<headers::Authorization<headers::authorization::Bearer>>>()
        .await
    {
        Ok(_) => panic!("removed Authorization remained visible to typed extraction"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "missing_header");

    assert!(req.set_header("bad header", "value").is_err());
    assert!(req.set_header("x-test", "value\r\ninjected: yes").is_err());

    req.set_header("cookie", "sid=one; theme=dark").unwrap();
    assert_eq!(req.cookie("sid"), Some("one"));
    req.append_header("cookie", "sid=two").unwrap();
    assert_eq!(req.cookie("sid"), Some("two"));
    req.remove_header("cookie");
    assert!(req.cookies().is_empty());

    req.set_cookie("sid", "three").unwrap();
    assert_eq!(req.header("cookie"), Some("sid=three"));
    req.remove_cookie("sid");
    assert!(req.header("cookie").is_none());
}

#[tokio::test]
async fn extractor_failures_keep_deserializer_details_private() {
    #[derive(Deserialize)]
    struct NumericQuery {
        #[serde(rename = "token")]
        _token: u64,
    }
    #[derive(Deserialize)]
    struct NumericHeaders {
        #[serde(rename = "x-private-token")]
        _token: u64,
    }

    let mut query_request = Request::builder()
        .path("/?token=secreto")
        .query("token=secreto")
        .build();
    let query_error = {
        let mut parts = query_request.parts_mut();
        match Query::<NumericQuery>::from_request_parts(&mut parts).await {
            Ok(_) => panic!("expected an invalid query"),
            Err(error) => error,
        }
    };
    assert_eq!(query_error.code(), "invalid_query");
    assert_eq!(
        query_error.public_message(),
        "La cadena de consulta no es valida"
    );
    assert!(query_error.source().is_some());
    assert!(
        !Response::from_error(query_error)
            .body_text()
            .contains("secreto")
    );

    let mut header_request = Request::builder()
        .header("x-private-token", "secreto")
        .build();
    let header_error = {
        let mut parts = header_request.parts_mut();
        match Headers::<NumericHeaders>::from_request_parts(&mut parts).await {
            Ok(_) => panic!("expected invalid headers"),
            Err(error) => error,
        }
    };
    assert_eq!(header_error.code(), "invalid_headers");
    assert_eq!(
        header_error.public_message(),
        "Los encabezados de la solicitud no son validos"
    );
    assert!(header_error.source().is_some());
    assert!(
        !Response::from_error(header_error)
            .body_text()
            .contains("secreto")
    );
}

#[tokio::test]
async fn json_extractor_requires_json_content_type() {
    #[derive(Deserialize)]
    struct CreateUser {
        #[serde(rename = "name")]
        _name: String,
    }

    let mut req = Request::builder()
        .method("POST")
        .body(br#"{"name":"Ada"}"#.to_vec())
        .build();

    let error = match Json::<CreateUser>::from_request(&mut req).await {
        Ok(_) => panic!("expected JSON extractor to reject missing content-type"),
        Err(error) => error,
    };
    assert_eq!(error.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(error.code(), "unsupported_media_type");

    let mut duplicate = Request::builder()
        .method("POST")
        .header("content-type", "application/json")
        .header("content-type", "text/plain")
        .body(br#"{"name":"Ada"}"#.to_vec())
        .build();
    let error = match Json::<CreateUser>::from_request(&mut duplicate).await {
        Ok(_) => panic!("expected JSON extractor to reject duplicate content-type"),
        Err(error) => error,
    };
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.code(), "duplicate_header");
}

#[tokio::test]
async fn body_extractor_reports_consumed_body() {
    #[derive(Deserialize)]
    struct CreateUser {
        #[serde(rename = "name")]
        _name: String,
    }

    let mut req = Request::builder()
        .method("POST")
        .header("content-type", "application/json")
        .body(br#"{"name":"Ada"}"#.to_vec())
        .build();

    let _stream = req.take_body_stream().unwrap();

    let error = match Json::<CreateUser>::from_request(&mut req).await {
        Ok(_) => panic!("expected consumed body rejection"),
        Err(error) => error,
    };
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    assert_eq!(error.code(), "body_already_consumed");
}

#[tokio::test]
async fn matched_path_extracts_without_consuming_the_body() {
    let mut req = Request::builder()
        .path("/users/42")
        .matched_path("/users/:id")
        .body("payload")
        .build();

    let MatchedPath(path) = {
        let mut parts = req.parts_mut();
        MatchedPath::from_request_parts(&mut parts).await.unwrap()
    };
    assert_eq!(path, "/users/:id");
    assert_eq!(req.text().await.unwrap(), "payload");
}

#[tokio::test]
async fn typed_handler_arguments_extract_parts_before_body() {
    #[derive(Clone)]
    struct Config {
        prefix: &'static str,
    }
    #[derive(Deserialize)]
    struct CreateUser {
        name: String,
    }

    async fn create_user(
        State(config): State<Config>,
        Path(team_id): Path<u64>,
        Json(input): Json<CreateUser>,
    ) -> Result<Response, HttpError> {
        Ok(Response::send(&format!("{}:{}:{}", config.prefix, team_id, input.name)).status(201))
    }

    let mut app = App::new();
    app.state(Config { prefix: "team" });
    app.post("/teams/:team_id/users", create_user).unwrap();

    let response = app
        .dispatch(
            Request::builder()
                .method("POST")
                .path("/teams/7/users")
                .header("content-type", "application/json")
                .body(br#"{"name":"Ada"}"#.to_vec())
                .build(),
        )
        .await;

    assert_eq!(response.status, 201);
    assert_eq!(response.body_text(), "team:7:Ada");
}

#[tokio::test]
async fn request_extensions_flow_from_middleware_to_typed_handlers() {
    #[derive(Clone)]
    struct TraceId(&'static str);

    let mut app = App::new();
    app.layer(|mut req: Request, next: Next| async move {
        req.insert_extension(TraceId("req-1"));
        next(req).await
    });
    app.get("/trace", |Extension(trace): Extension<TraceId>| {
        Response::send(trace.0)
    })
    .unwrap();

    let response = app
        .dispatch(Request::builder().path("/trace").body("payload").build())
        .await;

    assert_eq!(response.status, 200);
    assert_eq!(response.body_text(), "req-1");
}

#[test]
fn typed_handler_argument_arities_compile() {
    #[derive(Clone)]
    struct Config;
    #[derive(Deserialize)]
    struct Input {
        name: String,
    }
    #[derive(Deserialize)]
    struct ListQuery {
        active: Option<bool>,
    }

    fn one(Path(id): Path<u64>) -> Response {
        Response::send(&id.to_string())
    }

    async fn two(Path(id): Path<u64>, Query(query): Query<ListQuery>) -> Response {
        Response::send(&format!("{}:{:?}", id, query.active))
    }

    async fn four(
        method: hyper::Method,
        version: hyper::Version,
        MatchedPath(path): MatchedPath,
        ConnectInfo(addr): ConnectInfo,
    ) -> Response {
        Response::send(&format!("{method:?}:{version:?}:{path}:{addr:?}"))
    }

    #[allow(clippy::too_many_arguments)]
    async fn eight(
        State(_config): State<Config>,
        Path(id): Path<u64>,
        Query(query): Query<ListQuery>,
        MatchedPath(path): MatchedPath,
        OriginalUri(uri): OriginalUri,
        ConnectInfo(addr): ConnectInfo,
        method: hyper::Method,
        Json(input): Json<Input>,
    ) -> Response {
        Response::send(&format!(
            "{id}:{:?}:{path}:{uri}:{addr:?}:{method}:{}",
            query.active, input.name
        ))
    }

    let mut app = App::new();
    app.state(Config);
    app.get("/one/:id", one).unwrap();
    app.get("/two/:id", two).unwrap();
    app.get("/four", four).unwrap();
    app.post("/eight/:id", eight).unwrap();
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
            error: err.public_message(),
            status: err.status().as_u16(),
        })
        .status(err.status().as_u16())
    });
    let _ = app.get("/", |_req: Request| -> Result<Response, HttpError> {
        Err(HttpError::bad_request("Invalid name"))
    });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.status, 400);
    assert_eq!(res.body_text(), r#"{"error":"Invalid name","status":400}"#);
}

#[tokio::test]
async fn outbound_middleware_sees_the_custom_rendered_error_response() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        let response = next(req).await;
        response.header("x-outbound-policy", "applied")
    });
    app.error_handler(|error: HttpError| {
        Response::send("rendered").status(error.status().as_u16())
    });
    let _ = app.get("/", |_request: Request| -> Result<Response, HttpError> {
        Err(HttpError::forbidden("denied"))
    });

    let response = app.dispatch(dummy_request("")).await;

    assert_eq!(response.status, 403);
    assert_eq!(response.body_text(), "rendered");
    assert_eq!(
        response.headers.get("x-outbound-policy").unwrap(),
        "applied"
    );
}

#[tokio::test]
async fn panicking_global_error_handler_falls_back_to_generic_500() {
    let mut app = App::new();
    app.error_handler(|_err: HttpError| panic!("detalle interno sensible"));
    let _ = app.get("/ok", |_req: Request| Response::send("ok"));

    let response = app.dispatch(request_with_method("GET", "/missing")).await;

    assert_eq!(response.status, 500);
    assert!(!response.body_text().contains("detalle interno sensible"));
    assert_eq!(
        app.dispatch(request_with_method("GET", "/ok")).await.status,
        200
    );
}

#[tokio::test]
async fn global_error_handler_yields_to_mandatory_error_headers() {
    let mut app = App::new();
    app.error_handler(|err: HttpError| {
        let status = err.status();
        Response::json(&serde_json::json!({ "code": err.code() }))
            .status(status.as_u16())
            .header(ALLOW.as_str(), "CUSTOM")
            .header(RETRY_AFTER.as_str(), "999")
            .header(WWW_AUTHENTICATE.as_str(), "Custom first")
            .append_header(WWW_AUTHENTICATE.as_str(), "Custom second")
    });
    let _ = app.get("/method", |_req: Request| Response::send("ok"));
    let _ = app.get("/rate", |_req: Request| -> Result<Response, HttpError> {
        Err(HttpError::too_many_requests("Demasiadas solicitudes")
            .header(RETRY_AFTER, HeaderValue::from_static("30")))
    });
    let _ = app.get("/auth", |_req: Request| -> Result<Response, HttpError> {
        Err(HttpError::unauthorized("Autenticacion requerida")
            .header(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Bearer realm=\"api\""),
            )
            .append_header(
                WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"legacy\""),
            ))
    });

    let method = app.dispatch(request_with_method("POST", "/method")).await;
    assert_eq!(method.headers.get_all(ALLOW).iter().count(), 1);
    assert_eq!(method.headers.get(ALLOW).unwrap(), "GET, HEAD, OPTIONS");

    let rate = app.dispatch(request_with_method("GET", "/rate")).await;
    assert_eq!(rate.headers.get_all(RETRY_AFTER).iter().count(), 1);
    assert_eq!(rate.headers.get(RETRY_AFTER).unwrap(), "30");

    let auth = app.dispatch(request_with_method("GET", "/auth")).await;
    let challenges: Vec<_> = auth
        .headers
        .get_all(WWW_AUTHENTICATE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect();
    assert_eq!(
        challenges,
        ["Bearer realm=\"api\"", "Basic realm=\"legacy\""]
    );
}

#[tokio::test]
async fn builtin_middlewares_add_cors_request_id_gzip_and_tracing() {
    let mut app = App::new();
    app.layer(middleware::tracing());
    app.layer(middleware::request_id());
    app.layer(middleware::cors());
    app.layer(middleware::gzip());
    let _ = app.get("/", |req: Request| {
        Response::send(req.header("x-request-id").unwrap_or("no-id"))
    });

    let mut req = dummy_request("");
    req.set_header("accept-encoding", "br, gzip").unwrap();
    req.set_header("x-request-id", "req-123").unwrap();

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
async fn request_id_rejects_ambiguous_oversized_or_non_visible_values() {
    let mut app = App::new();
    app.layer(middleware::request_id());
    app.get("/", |req: Request| {
        Response::send(req.header("x-request-id").unwrap_or("missing"))
    })
    .unwrap();
    let client = TestClient::new(app);

    let preserved = client
        .get("/")
        .header("x-request-id", "external-123")
        .send()
        .await;
    assert_eq!(preserved.body_text(), "external-123");

    let duplicate = client
        .get("/")
        .header("x-request-id", "first")
        .header("x-request-id", "second")
        .send()
        .await;
    assert!(duplicate.body_text().starts_with("req-"));

    let oversized = client
        .get("/")
        .header("x-request-id", &"x".repeat(129))
        .send()
        .await;
    assert!(oversized.body_text().starts_with("req-"));
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
    let _ = app.get("/ping", |_r: Request| Response::send("pong"));
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
    let _ = app.get("/doc", |_r: Request| Response::send("contenido estable"));

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
async fn etag_revalidation_validates_selected_representation_length_before_304() {
    let mut app = App::new();
    app.layer(middleware::etag());
    let _ = app.get("/valid", |_request: Request| {
        Response::send("data").header("etag", "\"v1\"")
    });
    let _ = app.get("/mismatch", |_request: Request| {
        Response::send("data")
            .header("etag", "\"v1\"")
            .header(CONTENT_LENGTH.as_str(), "999")
    });
    let _ = app.get("/trailers", |_request: Request| {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("late"));
        Response::send("data")
            .header("etag", "\"v1\"")
            .header(CONTENT_LENGTH.as_str(), "4")
            .with_trailers(trailers)
    });
    let client = TestClient::new(app);

    let valid = client
        .get("/valid")
        .header("if-none-match", "\"v1\"")
        .send()
        .await;
    assert_eq!(valid.status, 304);
    assert_eq!(valid.headers.get(CONTENT_LENGTH).unwrap(), "4");
    assert!(valid.body_bytes().is_none());

    let mismatch = client
        .get("/mismatch")
        .header("if-none-match", "\"v1\"")
        .send()
        .await;
    assert_eq!(mismatch.status, 500);

    let conflicting_trailers = client
        .get("/trailers")
        .header("if-none-match", "\"v1\"")
        .send()
        .await;
    assert_eq!(conflicting_trailers.status, 500);
}

#[tokio::test]
async fn timeout_middleware_cuts_off_slow_handlers() {
    let mut app = App::new();
    let _ = app
        .get("/slow", |_r: Request| async {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            Response::send("late")
        })
        .unwrap()
        .layer(middleware::timeout(std::time::Duration::from_millis(40)));
    let _ = app
        .get("/fast", |_r: Request| Response::send("quick"))
        .unwrap()
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
    app.error_handler(|err: HttpError| {
        let status = err.status();
        Response::json(&serde_json::json!({ "code": err.code() })).status(status.as_u16())
    });
    app.layer(middleware::rate_limit(
        2,
        std::time::Duration::from_millis(80),
    ));
    let _ = app.get("/", |_r: Request| Response::send("ok"));

    // Two requests from the same IP pass; the third is throttled. The port
    // must not matter — limiting is per IP.
    assert_eq!(app.dispatch(request_from("1.1.1.1:1000")).await.status, 200);
    assert_eq!(app.dispatch(request_from("1.1.1.1:1001")).await.status, 200);
    let throttled = app.dispatch(request_from("1.1.1.1:1002")).await;
    assert_eq!(throttled.status, 429);
    assert!(throttled.headers.get("retry-after").is_some());
    assert_eq!(throttled.body_text(), r#"{"code":"too_many_requests"}"#);

    // A different client is unaffected.
    assert_eq!(app.dispatch(request_from("2.2.2.2:1000")).await.status, 200);

    // Once the window expires the client is admitted again.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(app.dispatch(request_from("1.1.1.1:1003")).await.status, 200);
}

#[tokio::test]
async fn bounded_rate_limit_uses_a_shared_overflow_bucket() {
    fn request_from(addr: &str) -> Request {
        let mut request = dummy_request("");
        request.remote_addr = Some(addr.parse().unwrap());
        request
    }

    let mut app = App::new();
    app.layer(middleware::RateLimit::new(1, Duration::from_secs(60)).max_clients(1));
    let _ = app.get("/", |_request: Request| Response::send("ok"));

    assert_eq!(app.dispatch(request_from("1.1.1.1:1")).await.status, 200);
    // The first untracked client uses the single overflow bucket.
    assert_eq!(app.dispatch(request_from("2.2.2.2:1")).await.status, 200);
    // Further untracked clients share that bucket instead of growing storage.
    assert_eq!(app.dispatch(request_from("3.3.3.3:1")).await.status, 429);
    // The tracked client's independent bucket is still enforced.
    assert_eq!(app.dispatch(request_from("1.1.1.1:2")).await.status, 429);
}

#[tokio::test]
async fn compression_negotiates_encoding_and_skips_small_bodies() {
    let mut app = App::new();
    app.layer(middleware::compression());
    let big = "x".repeat(2048);
    let _ = app.get("/big", move |_r: Request| Response::send(&big));
    let _ = app.get("/small", |_r: Request| Response::send("tiny"));

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

    // Higher q-value wins; Vary advertises the negotiation.
    let res = client
        .get("/big")
        .header("accept-encoding", "deflate, gzip;q=0.8")
        .send()
        .await;
    assert_eq!(res.headers.get(CONTENT_ENCODING).unwrap(), "deflate");
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
    let _ = app.get("/data", |_r: Request| Response::send("data"));
    let _ = app.post("/data", |_r: Request| Response::send("created"));

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
    assert_eq!(
        res.headers.get("vary").unwrap(),
        "Origin, Access-Control-Request-Method, Access-Control-Request-Headers"
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
    assert_eq!(res.headers.get("vary").unwrap(), "Origin");

    // Same-origin/non-CORS request untouched.
    let res = client.get("/data").send().await;
    assert!(res.headers.get("access-control-allow-origin").is_none());

    // Security-sensitive singleton fields reject ambiguous duplicates.
    let res = client
        .get("/data")
        .header("origin", "https://app.example.com")
        .header("origin", "https://evil.example")
        .send()
        .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);

    let res = client
        .options("/data")
        .header("origin", "https://app.example.com")
        .header("access-control-request-method", "POST")
        .header("access-control-request-method", "DELETE")
        .send()
        .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);

    // Repeated list-valued request-header fields are combined in arrival order.
    let res = client
        .options("/data")
        .header("origin", "https://app.example.com")
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "x-first")
        .header("access-control-request-headers", "x-second")
        .send()
        .await;
    assert_eq!(
        res.headers.get("access-control-allow-headers").unwrap(),
        "x-first, x-second"
    );
}

#[tokio::test]
async fn router_guards_block_requests_and_scoped_fallbacks_handle_misses() {
    let mut api = Router::new();
    api.guard(|req: &Request| req.header("x-api-key") == Some("secret"));
    let _ = api.get("/private", |_req: Request| Response::send("private"));
    let _ = api.fallback(|_req: Request| Response::send("fallback api").status(404));

    let mut app = App::new();
    let _ = app.mount("/api", api);

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
            error: err.public_message().to_string(),
            status: err.status().as_u16(),
        })
        .status(err.status().as_u16())
    });
    let _ = app.get("/exists", |_r: Request| Response::send("ok"));

    // Unmatched route (404) flows through the error handler.
    let res = app.dispatch(request_with_method("GET", "/missing")).await;
    assert_eq!(res.status, 404);
    assert_eq!(res.content_type, "application/json");
    assert!(res.body_text().contains("\"status\":404"));

    // Method mismatch (405) flows through the error handler too.
    let res = app.dispatch(request_with_method("POST", "/exists")).await;
    assert_eq!(res.status, 405);
    assert_eq!(res.content_type, "application/json");
    assert_eq!(res.headers.get(ALLOW).unwrap(), "GET, HEAD, OPTIONS");
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
    assert!(res.headers().get(CONNECTION).is_none());

    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        String::from_utf8_lossy(&body),
        "id: 1\nevent: greeting\ndata: hello\n\ndata: goodbye\n\n"
    );
}

#[tokio::test]
async fn zero_sse_heartbeat_disables_heartbeats_without_spinning() {
    let events = stream::iter(vec![SseEvent::new("uno"), SseEvent::new("dos")]);
    let body = tokio::time::timeout(
        Duration::from_millis(100),
        Response::sse_with_heartbeat(events, Duration::ZERO)
            .into_hyper()
            .into_body()
            .collect(),
    )
    .await
    .expect("zero heartbeat must not spin")
    .unwrap()
    .to_bytes();
    let text = String::from_utf8_lossy(&body);
    assert!(text.contains("data: uno"));
    assert!(text.contains("data: dos"));
    assert!(!text.contains("keep-alive"));
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
fn sse_data_cannot_inject_fields_with_bare_carriage_returns() {
    assert_eq!(
        SseEvent::new("safe\rid: injected\r\nnext\n").format(),
        "data: safe\ndata: id: injected\ndata: next\ndata: \n\n"
    );
}

#[test]
fn request_exposes_last_event_id() {
    let mut req = dummy_request("");
    assert!(req.last_event_id().is_none());
    req.set_header("last-event-id", "42").unwrap();
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
#[allow(deprecated)]
async fn gzip_middleware_skips_websocket_upgrade_responses() {
    let mut app = App::new();
    app.layer(middleware::gzip());
    let _ = app.get("/ws", |req: Request| Response::websocket(&req).unwrap());

    let req = Request::builder()
        .method("GET")
        .path("/ws")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .header("accept-encoding", "gzip")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("sec-websocket-version", "13")
        .build();

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
    let stream_res = Response::stream(stream::iter(vec![Ok::<_, Infallible>(Bytes::from_static(
        b"x",
    ))]));
    assert_eq!(stream_res.body_bytes(), None);
}

#[tokio::test]
async fn fallible_stream_closes_the_body_after_the_error() {
    let stream = stream::iter(vec![
        Ok::<_, std::io::Error>(Bytes::from_static(b"first")),
        Err(std::io::Error::other("read failed")),
    ]);
    let response = Response::stream(stream);

    let result = response.into_hyper().into_body().collect().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn response_can_emit_http_trailers() {
    let mut trailers = HeaderMap::new();
    trailers.insert("x-checksum", HeaderValue::from_static("abc"));
    let stream = stream::once(async { Ok::<_, std::io::Error>(Bytes::from_static(b"data")) });
    let response = Response::stream(stream)
        .with_trailers(trailers)
        .into_hyper();
    assert_eq!(response.headers().get(TRAILER).unwrap(), "x-checksum");

    let collected = response.into_body().collect().await.unwrap();

    assert_eq!(
        collected.trailers().unwrap().get("x-checksum").unwrap(),
        "abc"
    );
}

#[test]
fn response_rejects_forbidden_trailer_fields() {
    for forbidden in [
        CONTENT_LENGTH,
        CONTENT_TYPE,
        SET_COOKIE,
        TRANSFER_ENCODING,
        hyper::header::VARY,
        LOCATION,
        RETRY_AFTER,
        hyper::header::HeaderName::from_static("keep-alive"),
    ] {
        let forbidden_name = forbidden.as_str().to_string();
        let mut trailers = HeaderMap::new();
        trailers.insert(forbidden, HeaderValue::from_static("1"));
        assert!(
            Response::send("data").try_with_trailers(trailers).is_err(),
            "accepted forbidden trailer {forbidden_name}"
        );
    }
}

#[tokio::test]
async fn response_allows_etag_trailers_but_rejects_connection_nominated_trailers() {
    let mut trailers = HeaderMap::new();
    trailers.insert(hyper::header::ETAG, HeaderValue::from_static("\"late\""));
    let response = Response::send("data").with_trailers(trailers).into_hyper();
    assert_eq!(response.status(), 200);
    let collected = response.into_body().collect().await.unwrap();
    assert_eq!(
        collected.trailers().unwrap().get(hyper::header::ETAG),
        Some(&HeaderValue::from_static("\"late\""))
    );

    let mut trailers = HeaderMap::new();
    trailers.insert("x-private", HeaderValue::from_static("late"));
    let response = Response::send("data")
        .header(CONNECTION.as_str(), "x-private")
        .with_trailers(trailers)
        .into_hyper();
    assert_eq!(response.status(), 500);

    let mut trailers = HeaderMap::new();
    trailers.insert("x-private", HeaderValue::from_static("late"));
    let mut response = Response::send("data")
        .header(CONNECTION.as_str(), "x-private")
        .header(hyper::header::TE.as_str(), "trailers")
        .with_trailers(trailers);
    response.strip_http2_connection_headers();
    assert!(response.headers.get(hyper::header::TE).is_none());
    assert_eq!(response.into_hyper().status(), 500);
}

#[test]
fn buffered_response_body_can_only_be_replaced_when_buffered() {
    let mut response = Response::send("old");
    assert!(response.replace_body_bytes(Bytes::from_static(b"new")));
    assert_eq!(response.body_bytes(), Some(&b"new"[..]));

    let mut streamed = Response::stream(stream::iter(vec![Ok::<_, Infallible>(
        Bytes::from_static(b"stream"),
    )]));
    assert!(!streamed.replace_body_bytes(Bytes::from_static(b"ignored")));
    assert!(streamed.body_bytes().is_none());
}

#[tokio::test]
async fn test_client_finalizes_content_length_and_bodyless_statuses() {
    let mut app = App::new();
    let _ = app.get("/mismatch", |_request: Request| {
        Response::send("four").header(CONTENT_LENGTH.as_str(), "3")
    });
    let _ = app.get("/conflicting", |_request: Request| {
        Response::send("four")
            .append_header(CONTENT_LENGTH.as_str(), "4")
            .append_header(CONTENT_LENGTH.as_str(), "5")
    });
    let _ = app.get("/duplicate", |_request: Request| {
        Response::send("four")
            .append_header(CONTENT_LENGTH.as_str(), "4")
            .append_header(CONTENT_LENGTH.as_str(), "4")
    });
    let _ = app.get("/ambiguous", |_request: Request| {
        Response::send("four")
            .header(CONTENT_LENGTH.as_str(), "4")
            .header(TRANSFER_ENCODING.as_str(), "chunked")
    });
    let _ = app.get("/transfer", |_request: Request| {
        Response::send("four").header(TRANSFER_ENCODING.as_str(), "gzip, chunked")
    });
    let _ = app.get("/length-with-trailers", |_request: Request| {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("abc"));
        Response::send("four")
            .header(CONTENT_LENGTH.as_str(), "4")
            .with_trailers(trailers)
    });
    let _ = app.get("/trailers", |_request: Request| {
        let mut trailers = HeaderMap::new();
        trailers.insert("x-checksum", HeaderValue::from_static("abc"));
        Response::send("four").with_trailers(trailers)
    });
    let _ = app.get("/declared-trailer-only", |_request: Request| {
        Response::send("four").header(TRAILER.as_str(), "x-checksum")
    });
    for status in [103, 204, 205] {
        let path = format!("/{status}");
        let _ = app.get(&path, move |_request: Request| {
            Response::send("ignored")
                .status(status)
                .header(CONTENT_LENGTH.as_str(), "7")
        });
    }
    let _ = app.get("/304", |_request: Request| {
        Response::send("data")
            .status(304)
            .header(CONTENT_LENGTH.as_str(), "4")
    });
    let _ = app.get("/304-mismatch", |_request: Request| {
        Response::send("data")
            .status(304)
            .header(CONTENT_LENGTH.as_str(), "999")
    });
    let client = TestClient::new(app);

    assert_eq!(client.get("/mismatch").send().await.status, 500);
    assert_eq!(client.get("/conflicting").send().await.status, 500);
    assert_eq!(client.get("/ambiguous").send().await.status, 500);
    assert_eq!(client.get("/transfer").send().await.status, 500);
    assert_eq!(client.get("/length-with-trailers").send().await.status, 500);
    assert_eq!(
        client.get("/declared-trailer-only").send().await.status,
        500
    );

    let mut trailers = HeaderMap::new();
    trailers.insert("x-checksum", HeaderValue::from_static("abc"));
    let network = Response::send("four")
        .header(CONTENT_LENGTH.as_str(), "4")
        .with_trailers(trailers)
        .into_hyper();
    assert_eq!(network.status(), 500);

    let duplicate = client.get("/duplicate").send().await;
    assert_eq!(duplicate.status, 200);
    assert_eq!(duplicate.headers.get_all(CONTENT_LENGTH).iter().count(), 1);
    assert_eq!(duplicate.headers.get(CONTENT_LENGTH).unwrap(), "4");

    let early = client.get("/103").send().await;
    assert_eq!(early.status, 500);
    assert_eq!(
        early.headers.get(CONTENT_TYPE).unwrap(),
        "application/problem+json"
    );

    for status in [204, 205] {
        let response = client.get(&format!("/{status}")).send().await;
        assert_eq!(response.status, status);
        assert!(response.body_bytes().is_none());
        assert!(response.headers.get(CONTENT_LENGTH).is_none());
        assert!(response.headers.get(TRANSFER_ENCODING).is_none());
    }
    let not_modified = client.get("/304").send().await;
    assert_eq!(not_modified.status, 304);
    assert!(not_modified.body_bytes().is_none());
    assert_eq!(not_modified.headers.get(CONTENT_LENGTH).unwrap(), "4");
    assert_eq!(client.get("/304-mismatch").send().await.status, 500);

    let trailers = client.get("/trailers").send().await;
    assert_eq!(trailers.status, 200);
    assert_eq!(trailers.headers.get(TRAILER).unwrap(), "x-checksum");
    assert!(trailers.headers.get(CONTENT_LENGTH).is_none());
}

#[test]
fn checked_response_builders_reject_invalid_status_and_headers() {
    assert!(Response::send("x").try_status(99).is_err());
    assert!(Response::send("x").try_header("bad header", "x").is_err());
    assert!(Response::send("x").try_header("x-ok", "bad\r\n").is_err());
}

#[test]
fn final_responses_reject_interim_statuses_and_incomplete_switches() {
    for status in [100, 102, 103] {
        assert_eq!(Response::send("x").status(status).finalize().status, 500);
    }
    assert_eq!(Response::send("").status(101).finalize().status, 500);
    assert_eq!(
        Response::send("")
            .status(101)
            .header(CONNECTION.as_str(), "Upgrade")
            .header("upgrade", "websocket")
            .header("sec-websocket-accept", "not-a-valid-websocket-accept")
            .finalize()
            .status,
        500
    );
}

#[tokio::test]
async fn invalid_fluent_response_builder_renders_structured_500() {
    let res = Response::send("ok").status(99).into_hyper();

    assert_eq!(res.status(), 500);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "internal_server_error");
    assert_eq!(json["detail"], "No se pudo construir la respuesta");
}

#[tokio::test]
async fn invalid_sse_event_terminates_the_response_body_with_error() {
    let events = stream::iter(vec![SseEvent::new("hola").id("bad\nid")]);
    let result = Response::sse(events)
        .into_hyper()
        .into_body()
        .collect()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn request_builder_builds_full_request() {
    struct Cfg {
        name: &'static str,
    }

    let mut req = Request::builder()
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
    assert_eq!(req.header("cookie"), Some("sid=abc"));
    assert_eq!(req.headers_all("cookie"), vec!["sid=abc"]);
    assert_eq!(req.param("id"), Some("42"));
    assert_eq!(req.bytes().await.unwrap(), br#"{"n":1}"#[..]);
    assert_eq!(req.state::<Cfg>().unwrap().name, "test");
}

#[test]
fn request_builder_clears_stale_queries_uses_last_cookie_and_reports_json_errors() {
    struct FailingJson;

    impl Serialize for FailingJson {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("fallo intencional"))
        }
    }

    let request = Request::builder()
        .path("/first?stale=true")
        .path("/second")
        .cookie("sid", "explicit")
        .cookie("theme", "builder")
        .header("cookie", "sid=first")
        .header("cookie", "sid=last")
        .build();
    assert_eq!(request.path, "/second");
    assert!(request.raw_query.is_none());
    assert!(request.query.is_empty());
    assert_eq!(request.cookie("sid"), Some("last"));
    assert_eq!(request.cookie("theme"), Some("builder"));
    assert_eq!(
        request.headers_all("cookie"),
        vec!["theme=builder", "sid=first", "sid=last"]
    );
    assert_eq!(request.header("cookie"), Some("sid=last"));
    assert!(Request::builder().try_json(&FailingJson).is_err());
}

#[tokio::test]
async fn test_client_drives_app_without_tcp() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        let res = next(req).await;
        res.header("x-mw", "ran")
    });
    let _ = app.get("/hello/:name", |req: Request| {
        let name = req.param("name").unwrap_or("?");
        let lang = req.query("lang").unwrap_or("en");
        Response::send(&format!("hola {} ({})", name, lang))
    });
    let _ = app.post("/echo", |mut req: Request| async move {
        req.text().await.map(|text| Response::send(&text))
    });

    let client = TestClient::new(app);

    let res = client.get("/hello/ada?lang=es").send().await;
    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "hola ada (es)");
    assert_eq!(res.headers.get("x-mw").unwrap(), "ran");
    assert_eq!(
        res.headers.get(CONTENT_TYPE).unwrap(),
        "text/plain; charset=utf-8"
    );
    assert_eq!(res.headers.get(CONTENT_LENGTH).unwrap(), "13");

    let res = client.post("/echo").body("ping").send().await;
    assert_eq!(res.body_text(), "ping");

    let res = client.get("/missing").send().await;
    assert_eq!(res.status, 404);
}

#[tokio::test]
async fn test_client_rejects_buffered_content_length_mismatches_before_the_handler() {
    let hits = Arc::new(AtomicUsize::new(0));
    let mut app = App::new();
    let handler_hits = Arc::clone(&hits);
    app.post("/upload", move |_request: Request| {
        handler_hits.fetch_add(1, Ordering::SeqCst);
        Response::send("ok")
    })
    .unwrap();
    let client = TestClient::new(app);

    for declared in ["2", "4"] {
        let response = client
            .post("/upload")
            .header(CONTENT_LENGTH.as_str(), declared)
            .body("abc")
            .send()
            .await;
        assert_eq!(response.status, 400, "declared={declared}");
    }

    let response = client
        .post("/upload")
        .header(TRANSFER_ENCODING.as_str(), "chunked")
        .header(CONTENT_LENGTH.as_str(), "3")
        .body("abc")
        .send()
        .await;
    assert_eq!(response.status, 400);
    assert!(response.body_text().contains("ambiguous_message_framing"));
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_client_honors_body_limit_and_timeout() {
    let mut app = App::new();
    app.max_body_size(4);
    app.request_timeout(std::time::Duration::from_millis(30));
    let _ = app.post("/up", |mut req: Request| async move {
        req.bytes().await?;
        Ok::<_, HttpError>(Response::send("ok"))
    });
    let _ = app.get("/slow", |_r: Request| async {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        Response::send("late")
    });

    let client = TestClient::new(app);

    let res = client.post("/up").body("way too large").send().await;
    assert_eq!(res.status, 413);

    let res = client.get("/slow").send().await;
    assert_eq!(res.status, 408);

    let res = client.head("/slow").send().await;
    assert_eq!(res.status, 408);
    assert_eq!(res.body_text(), "");
    assert!(
        res.headers
            .get(CONTENT_LENGTH)
            .is_some_and(|value| value != "0")
    );
}

#[tokio::test]
async fn optional_json_and_direct_collection_cannot_bypass_the_hard_body_limit() {
    let mut body = RequestBody::buffered(Bytes::from_static(b"12345"), 4);
    let error = body.collect(usize::MAX).await.unwrap_err();
    assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE);

    let mut app = App::new();
    app.max_body_size(4);
    app.post("/optional", |mut req: Request| async move {
        match req.extract::<Option<Json<serde_json::Value>>>().await {
            Ok(_) => Response::send("accepted"),
            Err(error) => error.into_response(),
        }
    })
    .unwrap();

    let response = TestClient::new(app)
        .post("/optional")
        .header(CONTENT_TYPE.as_str(), "application/json")
        .body(r#"{"x":1}"#)
        .send()
        .await;
    assert_eq!(response.status, 413);
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
    assert_eq!(res.content_type, "application/problem+json");
    let body: serde_json::Value = serde_json::from_slice(res.body_bytes().unwrap()).unwrap();
    assert_eq!(body["code"], "not_found");
    assert_eq!(body["detail"], "Not Found");
}

#[test]
fn bad_request_sets_400() {
    let res = Response::bad_request();
    assert_eq!(res.status, 400);
    assert_eq!(res.content_type, "application/problem+json");
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
    assert_eq!(res.content_type, "application/problem+json");
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
fn response_cookie_uses_the_same_strict_serialization_as_cookie_builder() {
    let expected = Cookie::new("sid bad\t", "a b;\u{7f}c")
        .http_only(true)
        .to_header_value();
    let response = Response::send("ok")
        .cookie("sid bad\t", "a b;\u{7f}c")
        .finalize();
    assert_eq!(
        response.headers.get(SET_COOKIE).unwrap().to_str().unwrap(),
        expected
    );
    assert_eq!(expected, "sidbad=abc; Path=/; HttpOnly");
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
fn clear_cookie_with_preserves_the_original_cookie_scope() {
    let response = Response::send("ok").clear_cookie_with(
        Cookie::new("sid", "discarded")
            .path("/admin")
            .domain("example.com")
            .secure(true)
            .http_only(true)
            .same_site(SameSite::Strict),
    );

    assert_eq!(
        response.headers.get(SET_COOKIE).unwrap(),
        "sid=; Path=/admin; Domain=example.com; Max-Age=0; Secure; HttpOnly; SameSite=Strict"
    );
}

#[test]
fn cookie_builder_prevents_attribute_injection_and_enforces_prefix_rules() {
    let injected = Cookie::new("sid\r\nX-Evil", "abc;\r\nSecure")
        .path("/app; Domain=evil.example\r\n")
        .domain("example.com; Secure")
        .to_header_value();
    assert_eq!(
        injected,
        "sidX-Evil=abcSecure; Path=/app Domain=evil.example; Domain=example.comSecure"
    );
    assert!(!injected.contains('\r'));
    assert!(!injected.contains('\n'));
    assert!(!injected.contains("; Domain=evil.example"));

    let same_site_none = Cookie::new("sid", "abc")
        .same_site(SameSite::None)
        .to_header_value();
    assert!(same_site_none.contains("; Secure"));

    let host_cookie = Cookie::new("__Host-session", "abc")
        .path("/nested")
        .domain("example.com")
        .to_header_value();
    assert_eq!(host_cookie, "__Host-session=abc; Path=/; Secure");
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
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let mut app = App::new();
    app.layer(sessions.middleware());
    let store = sessions.clone();
    let _ = app.get("/visit", move |req: Request| {
        let id = req.session_id().expect("session id set").to_string();
        let visits = store
            .get(&id, "visits")
            .and_then(|count| count.parse::<u32>().ok())
            .unwrap_or(0)
            + 1;
        store
            .set(&id, "visits", &visits.to_string())
            .expect("session value fits configured bounds");
        Response::send(&visits.to_string())
    });

    let client = TestClient::new(app);

    // First visit creates the session and sets a signed cookie.
    let res = client.get("/visit").send().await;
    assert_eq!(res.body_text(), "1");
    assert_eq!(res.headers.get(CACHE_CONTROL).unwrap(), "private");
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
    assert!(
        res.headers
            .get(SET_COOKIE)
            .is_some_and(|cookie| cookie.to_str().unwrap().contains("Max-Age="))
    );

    // A tampered cookie gets a fresh session (and a new Set-Cookie).
    let res = client.get("/visit").cookie(name, "forged.sig").send().await;
    assert_eq!(res.body_text(), "1");
    assert!(res.headers.get(SET_COOKIE).is_some());
}

#[tokio::test]
async fn sessions_admit_lazily_and_never_evict_live_data_at_capacity() {
    let sessions = Sessions::new(TEST_SESSION_SECRET).max_sessions(2);
    let handler_sessions = sessions.clone();
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/read", |request: Request| {
        Response::send(if request.session_id().is_some() {
            "transient"
        } else {
            "missing"
        })
    })
    .unwrap();
    app.get("/write", move |request: Request| {
        let result = handler_sessions.set(
            request.session_id().expect("transient session id"),
            "owner",
            "resident",
        );
        Response::send(if result.is_ok() { "admitted" } else { "full" })
    })
    .unwrap();
    let client = TestClient::new(app);

    for _ in 0..128 {
        let response = client.get("/read").send().await;
        assert_eq!(response.body_text(), "transient");
        assert!(response.headers.get(SET_COOKIE).is_none());
    }
    assert!(
        sessions.is_empty(),
        "read-only traffic must not consume capacity"
    );

    let first = client.get("/write").send().await;
    let second = client.get("/write").send().await;
    let resident_ids = [&first, &second].map(|response| {
        let signed = response
            .headers
            .get(SET_COOKIE)
            .expect("admitted session cookie")
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .split_once('=')
            .unwrap()
            .1;
        verify_value(TEST_SESSION_SECRET, signed).expect("valid signed session id")
    });

    for _ in 0..128 {
        let response = client.get("/write").send().await;
        assert_eq!(response.body_text(), "full");
        assert!(response.headers.get(SET_COOKIE).is_none());
    }
    assert_eq!(sessions.len(), 2);
    for id in resident_ids {
        assert_eq!(sessions.get(&id, "owner").as_deref(), Some("resident"));
    }

    let stale = client
        .get("/read")
        .cookie("rustrest_session", "forged.signature")
        .send()
        .await;
    assert!(
        stale.headers.get(SET_COOKIE).is_some_and(|cookie| cookie
            .to_str()
            .unwrap()
            .starts_with("rustrest_session=; Path=/; Max-Age=0")),
        "an invalid cookie must be deleted when no replacement is persisted",
    );
}

#[tokio::test]
async fn sessions_replace_spoofed_internal_ids_before_handlers_run() {
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/id", |req: Request| {
        let trusted = req.session_id().unwrap_or("missing");
        let all = req.headers_all("x-session-id");
        Response::send(&format!("{trusted}|{}", all.join(",")))
    })
    .unwrap();

    let response = TestClient::new(app)
        .get("/id")
        .header("x-session-id", "attacker-controlled")
        .send()
        .await;
    let body = response.body_text();
    let (trusted, all) = body.split_once('|').unwrap();
    assert_ne!(trusted, "attacker-controlled");
    assert_eq!(all, trusted);
}

#[tokio::test]
async fn sessions_make_existing_cache_policies_private_and_fail_closed() {
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/public", |_req: Request| {
        Response::send("ok").header(CACHE_CONTROL.as_str(), "public, max-age=3600")
    })
    .unwrap();
    app.get("/invalid", |_req: Request| {
        let mut response = Response::send("ok");
        response.headers.insert(
            CACHE_CONTROL,
            HeaderValue::from_bytes(&[0x80]).expect("obs-text is representable by HeaderValue"),
        );
        response
    })
    .unwrap();
    let client = TestClient::new(app);

    let response = client.get("/public").send().await;
    let directives = response
        .headers
        .get_all(CACHE_CONTROL)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(directives, ["public, max-age=3600", "private"]);

    let response = client.get("/invalid").send().await;
    assert_eq!(
        response.headers.get(CACHE_CONTROL).unwrap(),
        "private, no-store"
    );
}

#[tokio::test]
async fn sessions_mark_new_cookies_secure_on_tls_requests() {
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let handler_sessions = sessions.clone();
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/session", move |req: Request| {
        handler_sessions
            .set(
                req.session_id().expect("transient session id"),
                "used",
                "yes",
            )
            .unwrap();
        Response::send("ok")
    })
    .unwrap();

    let request = Request::builder()
        .method("GET")
        .path("/session")
        .secure(true)
        .build();
    let response = app.dispatch(request).await;
    let cookie = response
        .headers
        .get(SET_COOKIE)
        .expect("session cookie set")
        .to_str()
        .unwrap();

    assert!(cookie.contains("; Secure"));
    assert!(cookie.contains("; HttpOnly"));
    assert!(cookie.contains("; SameSite=Lax"));
}

#[test]
fn sessions_validate_secrets_and_cookie_names() {
    assert!(Sessions::try_new("short").is_err());
    assert!(
        Sessions::try_new(TEST_SESSION_SECRET)
            .unwrap()
            .try_cookie_name("bad;name")
            .is_err()
    );
}

#[test]
fn sessions_configuration_is_fallible_and_frozen_after_cloning() {
    let sessions = Sessions::try_new(TEST_SESSION_SECRET)
        .unwrap()
        .try_cookie_name("sid")
        .unwrap()
        .try_idle_timeout(Duration::from_secs(30))
        .unwrap()
        .try_max_sessions(100)
        .unwrap()
        .try_max_entries_per_session(8)
        .unwrap()
        .try_max_session_key_bytes(32)
        .unwrap()
        .try_max_session_value_bytes(128)
        .unwrap()
        .try_max_session_data_bytes(512)
        .unwrap()
        .try_secure_cookies(true)
        .unwrap()
        .try_same_site(SameSite::Strict)
        .unwrap();
    let shared = sessions.clone();

    let error = match sessions.try_same_site(SameSite::Lax) {
        Ok(_) => panic!("configuration must be frozen while a clone exists"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("antes de clonarla"));

    let frozen = Sessions::new(TEST_SESSION_SECRET);
    let _shared = frozen.clone();
    let panic = match std::panic::catch_unwind(|| frozen.max_sessions(2)) {
        Ok(_) => panic!("infallible builders must fail loudly after cloning"),
        Err(panic) => panic,
    };
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or_default();
    assert!(message.contains("configure Sessions before cloning"));

    // Once every other clone is gone, the sole owner can still be configured
    // safely without forking the shared store's behavior.
    assert!(shared.try_max_sessions(200).is_ok());
}

#[tokio::test]
async fn sessions_reject_duplicate_configured_cookie_names_before_routing() {
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let hits = Arc::new(AtomicUsize::new(0));
    let route_hits = Arc::clone(&hits);
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/", move |_request: Request| {
        route_hits.fetch_add(1, Ordering::Relaxed);
        Response::send("unexpected")
    })
    .unwrap();

    let response = TestClient::new(app)
        .get("/")
        .header(
            "cookie",
            "rustrest_session=attacker; rustrest_session=victim",
        )
        .send()
        .await;

    assert_eq!(response.status, 400);
    assert!(response.body_text().contains("duplicate_session_cookie"));
    assert_eq!(response.headers.get(CACHE_CONTROL).unwrap(), "private");
    assert!(response.headers.get(SET_COOKIE).is_none());
    assert_eq!(hits.load(Ordering::Relaxed), 0);
    assert!(sessions.is_empty());
}

#[test]
fn sessions_enforce_entry_key_value_and_aggregate_limits() {
    let sessions = Sessions::new(TEST_SESSION_SECRET)
        .max_entries_per_session(2)
        .max_session_key_bytes(3)
        .max_session_value_bytes(4)
        .max_session_data_bytes(8);

    assert_eq!(
        sessions.set("id", "", "x").unwrap_err().kind(),
        SessionDataErrorKind::EmptyKey
    );
    assert_eq!(
        sessions.set("id", "long", "x").unwrap_err().kind(),
        SessionDataErrorKind::KeyTooLarge
    );
    assert_eq!(
        sessions.set("id", "a", "12345").unwrap_err().kind(),
        SessionDataErrorKind::ValueTooLarge
    );
    assert!(
        sessions.is_empty(),
        "rejected values must not create sessions"
    );

    sessions.set("id", "a", "1234").unwrap();
    sessions.set("id", "b", "1").unwrap();
    assert_eq!(
        sessions.set("id", "c", "1").unwrap_err().kind(),
        SessionDataErrorKind::TooManyEntries
    );

    let aggregate = Sessions::new(TEST_SESSION_SECRET)
        .max_entries_per_session(4)
        .max_session_key_bytes(3)
        .max_session_value_bytes(4)
        .max_session_data_bytes(6);
    aggregate.set("id", "a", "1234").unwrap();
    assert_eq!(
        aggregate.set("id", "b", "1").unwrap_err().kind(),
        SessionDataErrorKind::SessionTooLarge
    );
}

#[tokio::test]
async fn sessions_report_only_live_entries_and_do_not_override_handler_cookie_changes() {
    let sessions = Sessions::new(TEST_SESSION_SECRET).idle_timeout(Duration::from_millis(10));
    sessions.set("direct", "a", "1").unwrap();
    assert_eq!(sessions.len(), 1);
    tokio::time::sleep(Duration::from_millis(15)).await;
    assert_eq!(sessions.len(), 0);

    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/", |_request: Request| {
        Response::send("ok").clear_cookie("rustrest_session")
    })
    .unwrap();
    let response = TestClient::new(app).get("/").send().await;
    let cookies: Vec<_> = response
        .headers
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect();
    assert_eq!(cookies, ["rustrest_session=; Path=/; Max-Age=0"]);
}

#[tokio::test]
async fn sessions_round_cookie_max_age_up_to_cover_subsecond_timeouts() {
    let sessions = Sessions::new(TEST_SESSION_SECRET).idle_timeout(Duration::from_millis(1_500));
    let handler_sessions = sessions.clone();
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/", move |request: Request| {
        handler_sessions
            .set(
                request.session_id().expect("transient session id"),
                "used",
                "yes",
            )
            .unwrap();
        Response::send("ok")
    })
    .unwrap();

    let response = TestClient::new(app).get("/").send().await;
    let cookie = response.headers.get(SET_COOKIE).unwrap().to_str().unwrap();
    assert!(cookie.contains("; Max-Age=2"), "{cookie}");
}

#[tokio::test]
async fn sessions_do_not_reissue_ids_that_expire_while_handlers_run() {
    let sessions = Sessions::new(TEST_SESSION_SECRET).idle_timeout(Duration::from_millis(15));
    let create_sessions = sessions.clone();
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/create", move |request: Request| {
        create_sessions
            .set(
                request.session_id().expect("transient session id"),
                "used",
                "yes",
            )
            .unwrap();
        Response::send("created")
    })
    .unwrap();
    app.get("/slow", |_request: Request| async {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Response::send("ok")
    })
    .unwrap();

    let client = TestClient::new(app);
    let created = client.get("/create").send().await;
    let signed = created
        .headers
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .split_once('=')
        .unwrap()
        .1;
    let response = client
        .get("/slow")
        .cookie("rustrest_session", signed)
        .send()
        .await;
    let cookie = response.headers.get(SET_COOKIE).unwrap().to_str().unwrap();
    assert!(
        cookie.starts_with("rustrest_session=; Path=/; Max-Age=0"),
        "{cookie}"
    );
    assert!(sessions.is_empty());
}

#[tokio::test]
async fn sessions_clear_client_cookie_when_handler_invalidates_server_session() {
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let create_sessions = sessions.clone();
    let clear_sessions = sessions.clone();
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/create", move |request: Request| {
        let id = request.session_id().expect("transient session id");
        create_sessions.set(id, "used", "yes").unwrap();
        Response::send("created")
    })
    .unwrap();
    app.get("/clear", move |request: Request| {
        clear_sessions.clear(request.session_id().expect("resident session id"));
        Response::send("ok")
    })
    .unwrap();

    let client = TestClient::new(app);
    let created = client.get("/create").send().await;
    let signed = created
        .headers
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .split_once('=')
        .unwrap()
        .1;
    let response = client
        .get("/clear")
        .cookie("rustrest_session", signed)
        .send()
        .await;
    let cookie = response.headers.get(SET_COOKIE).unwrap().to_str().unwrap();
    assert!(
        cookie.starts_with("rustrest_session=; Path=/; Max-Age=0"),
        "{cookie}"
    );
    assert!(sessions.is_empty());
}

#[tokio::test]
async fn sessions_only_suppress_automatic_cookie_for_the_same_root_host_scope() {
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let scoped_sessions = sessions.clone();
    let invalid_sessions = sessions.clone();
    let mut app = App::new();
    app.layer(sessions.middleware());
    app.get("/scoped", move |request: Request| {
        scoped_sessions
            .set(
                request.session_id().expect("transient session id"),
                "used",
                "yes",
            )
            .unwrap();
        Response::send("ok")
            .set_cookie(Cookie::new("rustrest_session", "narrow").path("/scoped"))
            .set_cookie(
                Cookie::new("rustrest_session", "domain")
                    .domain("example.com")
                    .path("/"),
            )
    })
    .unwrap();
    app.get("/root", |_request: Request| {
        Response::send("ok").set_cookie(Cookie::new("rustrest_session", "handler"))
    })
    .unwrap();
    app.get("/invalid", move |request: Request| {
        invalid_sessions
            .set(
                request.session_id().expect("transient session id"),
                "used",
                "yes",
            )
            .unwrap();
        let mut response = Response::send("ok");
        response.headers.append(
            SET_COOKIE,
            HeaderValue::from_bytes(&[0x80]).expect("obs-text is a valid raw header value"),
        );
        response
    })
    .unwrap();
    let client = TestClient::new(app);

    let scoped = client.get("/scoped").send().await;
    let scoped_cookies = scoped
        .headers
        .get_all(SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(scoped_cookies.len(), 3, "{scoped_cookies:?}");
    assert!(
        scoped_cookies
            .iter()
            .any(|cookie| cookie.starts_with("rustrest_session=narrow; Path=/scoped"))
    );
    assert!(
        scoped_cookies
            .iter()
            .any(|cookie| cookie.contains("; Domain=example.com"))
    );
    assert!(
        scoped_cookies.iter().any(|cookie| {
            cookie.starts_with("rustrest_session=")
                && cookie.contains("; Path=/; Max-Age=")
                && !cookie.contains("; Domain=")
        }),
        "{scoped_cookies:?}"
    );

    let root = client.get("/root").send().await;
    let root_cookies = root.headers.get_all(SET_COOKIE).iter().collect::<Vec<_>>();
    assert_eq!(root_cookies.len(), 1);
    assert_eq!(
        root_cookies[0].to_str().unwrap(),
        "rustrest_session=handler; Path=/"
    );

    let invalid = client.get("/invalid").send().await;
    let invalid_cookies = invalid
        .headers
        .get_all(SET_COOKIE)
        .iter()
        .collect::<Vec<_>>();
    assert_eq!(invalid_cookies.len(), 2);
    assert!(invalid_cookies.iter().any(|value| {
        value
            .to_str()
            .is_ok_and(|cookie| cookie.starts_with("rustrest_session="))
    }));
}

#[tokio::test]
async fn sessions_expire_and_remain_bounded() {
    let sessions = Sessions::new(TEST_SESSION_SECRET)
        .idle_timeout(Duration::from_millis(25))
        .max_sessions(2);
    let handler_sessions = sessions.clone();
    let mut app = App::new();
    app.layer(sessions.middleware());
    let _ = app.get("/session", move |request: Request| {
        let result = handler_sessions.set(
            request.session_id().expect("transient session id"),
            "used",
            "yes",
        );
        Response::send(if result.is_ok() { "admitted" } else { "full" })
    });
    let client = TestClient::new(app);

    let first = client.get("/session").send().await;
    let first_cookie = first
        .headers
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .split_once('=')
        .unwrap()
        .1
        .to_string();
    tokio::time::sleep(Duration::from_millis(2)).await;
    let second = client.get("/session").send().await;
    assert_eq!(second.body_text(), "admitted");
    tokio::time::sleep(Duration::from_millis(2)).await;
    let third = client.get("/session").send().await;
    assert_eq!(third.body_text(), "full");
    assert!(third.headers.get(SET_COOKIE).is_none());
    assert_eq!(sessions.len(), 2);

    // A valid resident session still resumes at capacity: anonymous churn did
    // not evict it.
    let replayed = client
        .get("/session")
        .cookie("rustrest_session", &first_cookie)
        .send()
        .await;
    assert!(replayed.headers.get(SET_COOKIE).is_some());
    assert_eq!(replayed.body_text(), "admitted");
    assert_eq!(sessions.len(), 2);

    tokio::time::sleep(Duration::from_millis(30)).await;
    let expired = client
        .get("/session")
        .cookie("rustrest_session", &first_cookie)
        .send()
        .await;
    assert!(expired.headers.get(SET_COOKIE).is_some());
    assert!(sessions.len() <= 2);
}

#[tokio::test]
async fn sessions_force_secure_for_none_and_cookie_prefixes() {
    for sessions in [
        Sessions::new(TEST_SESSION_SECRET).same_site(SameSite::None),
        Sessions::new(TEST_SESSION_SECRET).cookie_name("__Host-session"),
        Sessions::new(TEST_SESSION_SECRET).secure_cookies(true),
    ] {
        let handler_sessions = sessions.clone();
        let mut app = App::new();
        app.layer(sessions.middleware());
        let _ = app.get("/", move |request: Request| {
            handler_sessions
                .set(
                    request.session_id().expect("transient session id"),
                    "used",
                    "yes",
                )
                .unwrap();
            Response::send("ok")
        });
        let response = TestClient::new(app).get("/").send().await;
        assert!(
            response
                .headers
                .get(SET_COOKIE)
                .unwrap()
                .to_str()
                .unwrap()
                .contains("; Secure")
        );
    }
}

#[test]
fn query_params_are_parsed_and_url_decoded() {
    let query = parse_query("q=rust+rest&tag=web&tag=api&empty=&flag&encoded=hello%20world");
    let req = Request {
        version: hyper::Version::HTTP_11,
        method: "GET".to_string(),
        path: "/buscar".to_string(),
        raw_query: Some(
            "q=rust+rest&tag=web&tag=api&empty=&flag&encoded=hello%20world".to_string(),
        ),
        query,
        headers: HashMap::new(),
        cookies: HashMap::new(),
        body: RequestBody::buffered(Bytes::new(), super::body::DEFAULT_BODY_LIMIT),
        body_limit: super::body::DEFAULT_BODY_LIMIT,
        params: HashMap::new(),
        route_pattern: None,
        websocket_runtime: WebSocketRuntimeHandle::local(),
        resolved_websocket_config: None,
        state: StateStore::default(),
        extensions: StateStore::default(),
        upgrade: None,
        remote_addr: None,
        secure_transport: false,
        header_pairs: Vec::new(),
        session_id: None,
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
    for (name, value) in cookies {
        req.set_cookie(&name, &value).unwrap();
    }

    assert_eq!(req.cookie("sid"), Some("abc"));
    assert_eq!(req.cookie("theme"), Some("dark"));
    assert_eq!(req.cookie("empty"), Some(""));
}

#[tokio::test]
async fn request_json_deserializes_body() {
    #[derive(Deserialize, PartialEq, Debug)]
    struct User {
        id: u32,
        name: String,
    }
    let mut req = dummy_request(r#"{"id":1,"name":"Ada"}"#);
    let user: User = req.json().await.unwrap();
    assert_eq!(
        user,
        User {
            id: 1,
            name: "Ada".to_string()
        }
    );
}

#[tokio::test]
async fn request_json_errors_on_invalid_body() {
    let mut req = dummy_request("not json");
    assert!(req.json::<serde_json::Value>().await.is_err());
}

#[tokio::test]
async fn request_form_parses_urlencoded_body() {
    #[derive(Deserialize)]
    struct Login {
        user: String,
        tags: Vec<String>,
    }

    let mut req = Request::builder()
        .method("POST")
        .header("content-type", "application/x-www-form-urlencoded")
        .body("user=ada+lovelace&tags=a&tags=b")
        .build();

    let form: Login = req.form().await.unwrap();
    assert_eq!(form.user, "ada lovelace");
    assert_eq!(form.tags, vec!["a", "b"]);

    let mut req = Request::builder()
        .method("POST")
        .header("content-type", "application/x-www-form-urlencoded")
        .body("user=ada+lovelace&tags=a&tags=b")
        .build();
    let Form(extracted) = req.extract::<Form<Login>>().await.unwrap();
    assert_eq!(extracted.user, "ada lovelace");

    let mut bad = dummy_request("%%%not-a-form=%zz");
    let error = match bad.form::<Login>().await {
        Ok(_) => panic!("invalid form unexpectedly parsed"),
        Err(error) => error,
    };
    assert_eq!(error.code(), "invalid_form");
    assert_eq!(
        error.public_message(),
        "El cuerpo no contiene un formulario valido"
    );
    assert!(!error.public_message().contains("missing field"));
    assert!(error.source().is_some());
}

#[tokio::test]
async fn request_multipart_parses_fields_and_binary_files() {
    let mut body = Vec::new();
    body.extend_from_slice(
        b"--XBOUND\r\ncontent-disposition: form-data; name=\"campo\"\r\n\r\nhola\r\n",
    );
    body.extend_from_slice(
        b"--XBOUND\r\ncontent-disposition: form-data; name=\"archivo\"; filename=\"a.bin\"\r\ncontent-type: application/octet-stream\r\n\r\n",
    );
    body.extend_from_slice(&[0xFF, 0x00, 0xFE]);
    body.extend_from_slice(b"\r\n--XBOUND--\r\n");

    let mut req = Request::builder()
        .method("POST")
        .header("content-type", "multipart/form-data; boundary=XBOUND")
        .body(body)
        .build();

    let parts = req.multipart().await.unwrap();
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
    assert!(dummy_request("x").multipart().await.is_err());
}

#[tokio::test]
async fn multipart_honors_delimiter_lines_and_quoted_parameters() {
    let body = concat!(
        "preamble\r\n",
        "--AaB03x\r\n",
        "Content-Disposition: form-data; name=\"field;one\"; ",
        "filename=\"a\\\";b.txt\"\r\n",
        "Content-Type: text/plain\r\n",
        "\r\n",
        "first\r\n--AaB03x-not-a-delimiter\r\nlast\r\n",
        "--AaB03x-- \t\r\n",
        "epilogue"
    );
    let mut request = Request::builder()
        .method("POST")
        .header(
            "content-type",
            "multipart/form-data; charset=\"utf;8\"; boundary=\"AaB03x\"",
        )
        .body(body)
        .build();

    let parts = request.multipart().await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].name, "field;one");
    assert_eq!(parts[0].filename.as_deref(), Some("a\";b.txt"));
    assert_eq!(parts[0].text(), "first\r\n--AaB03x-not-a-delimiter\r\nlast");
}

#[tokio::test]
async fn multipart_rejects_control_characters_in_part_header_values() {
    let bodies: [&[u8]; 3] = [
        b"--B\r\nContent-Disposition: form-data; name=\"a\x7fb\"\r\n\r\nx\r\n--B--\r\n",
        b"--B\r\nContent-Disposition: form-data; name=\"a\\\x01b\"\r\n\r\nx\r\n--B--\r\n",
        b"--B\r\nContent-Disposition: form-data; name=\"a\"\r\nContent-Type: text/plain\x01evil\r\n\r\nx\r\n--B--\r\n",
    ];

    for body in bodies {
        let mut request = Request::builder()
            .method("POST")
            .header("content-type", "multipart/form-data; boundary=B")
            .body(Bytes::copy_from_slice(body))
            .build();
        let error = request.multipart().await.expect_err("control accepted");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn multipart_enforces_independent_parser_limits() {
    let two_parts = concat!(
        "--B\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\none\r\n",
        "--B\r\nContent-Disposition: form-data; name=\"b\"\r\n\r\ntwo\r\n",
        "--B--\r\n"
    );
    let cases = [
        (two_parts, MultipartLimits::new().max_parts(1), "part count"),
        (
            "--B\r\nContent-Disposition: form-data; name=\"a\"\r\nX-Test: yes\r\n\r\none\r\n--B--\r\n",
            MultipartLimits::new().max_headers_per_part(1),
            "header count",
        ),
        (
            "--B\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\none\r\n--B--\r\n",
            MultipartLimits::new().max_header_bytes(8),
            "header bytes",
        ),
        (
            "--B\r\nContent-Disposition: form-data; name=\"a\"\r\n\r\nfour\r\n--B--\r\n",
            MultipartLimits::new().max_part_bytes(3),
            "part bytes",
        ),
    ];

    for (body, limits, label) in cases {
        let mut request = Request::builder()
            .method("POST")
            .header("content-type", "multipart/form-data; boundary=B")
            .body(body)
            .build();
        let error = request
            .multipart_with_limits(limits)
            .await
            .expect_err(label);
        assert_eq!(error.status(), StatusCode::PAYLOAD_TOO_LARGE, "{label}");
    }
}

#[tokio::test]
async fn request_body_bytes_preserve_non_utf8_and_text_is_strict() {
    let raw: &[u8] = &[0xff, 0xfe, b'h', b'i'];
    let mut req = Request::builder().body(Bytes::copy_from_slice(raw)).build();

    assert_eq!(req.bytes().await.unwrap(), raw);
    assert_eq!(req.text().await.unwrap_err().code(), "invalid_utf8");
    assert_eq!(
        req.text_lossy().await.unwrap(),
        String::from_utf8_lossy(raw)
    );
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
fn route_patterns_normalize_percent_encoding_and_bound_segment_depth() {
    let pattern = RoutePattern::parse("/literal/%7bname%7d/%3avalue").unwrap();
    assert_eq!(pattern.as_str(), "/literal/%7Bname%7D/%3Avalue");
    assert_eq!(
        RoutePattern::parse("/bad/%zz").unwrap_err().kind(),
        RouteErrorKind::InvalidPercentEncoding
    );
    assert_eq!(
        RoutePattern::parse("/users/:bad.name").unwrap_err().kind(),
        RouteErrorKind::InvalidParameter
    );

    let too_deep = "/x".repeat(257);
    assert_eq!(
        RoutePattern::parse(&too_deep).unwrap_err().kind(),
        RouteErrorKind::TooManySegments
    );
    let router = Router::new();
    let error = match router.resolve_method("GET", &too_deep, None) {
        Ok(_) => panic!("over-deep request path unexpectedly resolved"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), RouteMatchErrorKind::TooManySegments);
}

#[tokio::test]
async fn encoded_static_route_literals_match_and_generate_canonical_urls() {
    let mut app = App::new();
    app.get("/literal/%7Bname%7D/%3Avalue", |_request: Request| {
        Response::send("ok")
    })
    .unwrap()
    .name("literal")
    .unwrap();
    assert_eq!(
        app.url_for("literal", std::iter::empty::<(&str, &str)>())
            .unwrap(),
        "/literal/%7Bname%7D/%3Avalue"
    );

    let response = TestClient::new(app)
        .get("/literal/%7bname%7d/%3avalue")
        .send()
        .await;
    assert_eq!(response.status, 200);
}

fn route_match(router: &Router, method: &str, path: &str) -> Option<MatchedRoute> {
    router.resolve_method(method, path, None).unwrap()
}

#[test]
fn router_matches_method_and_path_param() {
    let mut router = Router::new();
    let _ = router
        .get("/users", |_r: Request| Response::send("list"))
        .unwrap();
    let _ = router
        .get("/users/:id", |req: Request| {
            Response::send(req.param("id").unwrap_or("?"))
        })
        .unwrap();

    assert!(route_match(&router, "GET", "/users").is_some());
    assert!(route_match(&router, "POST", "/users").is_none());
    assert!(route_match(&router, "GET", "/nope/extra").is_none());

    let matched = route_match(&router, "GET", "/users/42").expect("should match");
    assert_eq!(matched.params.get("id").map(String::as_str), Some("42"));
}

#[test]
fn router_supports_extra_methods_and_all() {
    let mut router = Router::new();
    let _ = router
        .patch("/items/:id", |_r: Request| Response::send("patch"))
        .unwrap();
    let _ = router
        .options("/items", |_r: Request| Response::send("options"))
        .unwrap();
    let _ = router
        .head("/items", |_r: Request| Response::send("head"))
        .unwrap();
    let _ = router
        .all("/health", |_r: Request| Response::send("ok"))
        .unwrap();

    assert!(route_match(&router, "PATCH", "/items/1").is_some());
    assert!(route_match(&router, "OPTIONS", "/items").is_some());
    assert!(route_match(&router, "HEAD", "/items").is_some());
    assert!(route_match(&router, "GET", "/health").is_some());
    assert!(route_match(&router, "POST", "/health").is_some());
}

#[test]
fn mount_concatenates_prefixes_across_nesting() {
    let mut users = Router::new();
    users
        .get("/:id", |req: Request| {
            Response::send(req.param("id").unwrap_or("?"))
        })
        .unwrap();

    let mut api = Router::new();
    api.mount("/users", users).unwrap();

    let mut root = Router::new();
    root.mount("/api", api).unwrap();

    let matched = route_match(&root, "GET", "/api/users/42").expect("should match");
    assert_eq!(matched.params.get("id").map(String::as_str), Some("42"));
    // Only `/:id` was registered, so the bare collection path does not match.
    assert!(route_match(&root, "GET", "/api/users").is_none());
}

#[test]
fn router_prefers_static_over_param_regardless_of_registration_order() {
    let mut router = Router::new();
    // The param route is registered FIRST; specificity must still win.
    let _ = router
        .get("/users/:id", |req: Request| {
            Response::send(req.param("id").unwrap_or("?"))
        })
        .unwrap();
    let _ = router
        .get("/users/me", |_r: Request| Response::send("me"))
        .unwrap();

    let params = route_match(&router, "GET", "/users/me")
        .expect("should match")
        .params;
    assert!(
        params.is_empty(),
        "static /users/me should win over /users/:id, captured {params:?}"
    );

    let params = route_match(&router, "GET", "/users/42")
        .expect("should match")
        .params;
    assert_eq!(params.get("id").map(String::as_str), Some("42"));
}

#[test]
fn router_prefers_param_over_wildcard_and_backtracks_across_branches() {
    let mut router = Router::new();
    // Wildcard registered first; the more specific param route must win.
    let _ = router
        .get("/files/*rest", |_r: Request| Response::send("wild"))
        .unwrap();
    let _ = router
        .get("/files/:name", |_r: Request| Response::send("param"))
        .unwrap();

    let params = route_match(&router, "GET", "/files/readme")
        .expect("should match")
        .params;
    assert_eq!(params.get("name").map(String::as_str), Some("readme"));

    // Deeper paths only the wildcard can absorb.
    let params = route_match(&router, "GET", "/files/a/b")
        .expect("should match")
        .params;
    assert_eq!(params.get("rest").map(String::as_str), Some("a/b"));

    // A static branch that dead-ends must backtrack to the param route
    // (also exercises index invalidation after further registration).
    let _ = router
        .get("/users/me/profile", |_r: Request| Response::send("prof"))
        .unwrap();
    let _ = router
        .get("/users/:id", |req: Request| {
            Response::send(req.param("id").unwrap_or("?"))
        })
        .unwrap();
    let params = route_match(&router, "GET", "/users/me")
        .expect("should match")
        .params;
    assert_eq!(params.get("id").map(String::as_str), Some("me"));

    // Method-aware backtracking: POST /users/me must not shadow GET.
    let _ = router
        .post("/users/me", |_r: Request| Response::send("post me"))
        .unwrap();
    let params = route_match(&router, "GET", "/users/me")
        .expect("should match")
        .params;
    assert_eq!(params.get("id").map(String::as_str), Some("me"));
    let params = route_match(&router, "POST", "/users/me")
        .expect("should match")
        .params;
    assert!(params.is_empty());
}

#[tokio::test]
async fn router_prefers_exact_method_over_all_on_same_path() {
    let mut router = Router::new();
    // `.all()` registered first; an exact-method route must still win for GET.
    let _ = router
        .all("/health", |_r: Request| Response::send("all"))
        .unwrap();
    let _ = router
        .get("/health", |_r: Request| Response::send("get"))
        .unwrap();

    let matched = route_match(&router, "GET", "/health").expect("should match");
    assert_eq!(
        (matched.handler)(dummy_request("")).await.body_text(),
        "get"
    );

    let matched = route_match(&router, "DELETE", "/health").expect("should match");
    assert_eq!(
        (matched.handler)(dummy_request("")).await.body_text(),
        "all"
    );
}

#[test]
fn app_lists_registered_routes_for_introspection() {
    let mut app = App::new();
    let _ = app.get("/", |_r: Request| Response::send("root")).unwrap();
    let _ = app
        .post("/users", |_r: Request| Response::send("create"))
        .unwrap();
    let _ = app
        .get("/users/:id", |_r: Request| Response::send("show"))
        .unwrap();
    let mut files = Router::new();
    let _ = files
        .get("/*path", |_r: Request| Response::send("file"))
        .unwrap();
    app.mount("/files", files).unwrap();

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
    let _ = app
        .get("/users", |_r: Request| Response::send("list"))
        .unwrap();
    let res = app.dispatch(request_with_method("GET", "/users/")).await;
    assert_eq!(res.status, 200);

    // Strict: non-canonical paths 404; canonical ones and "/" are untouched.
    let mut app = App::new();
    app.trailing_slash(TrailingSlash::Strict);
    let _ = app
        .get("/users", |_r: Request| Response::send("list"))
        .unwrap();
    let _ = app.get("/", |_r: Request| Response::send("root")).unwrap();
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
    let _ = app
        .get("/users", |_r: Request| Response::send("list"))
        .unwrap();
    let mut req = request_with_method("GET", "/users/");
    req.raw_query = Some("page=2".to_string());
    let res = app.dispatch(req).await;
    assert_eq!(res.status, 308);
    assert_eq!(res.headers.get("location").unwrap(), "/users?page=2");

    // A Location beginning with `//` is scheme-relative in browsers. Collapse
    // only the leading slash run; interior duplicate slashes remain untouched.
    let res = app
        .dispatch(request_with_method("GET", "//evil.example/"))
        .await;
    assert_eq!(res.status, 308);
    assert_eq!(res.headers.get("location").unwrap(), "/evil.example");

    // WHATWG URL parsers normalize backslashes to slashes before resolving a
    // network-path reference, so keep them percent-encoded in Location.
    let res = app
        .dispatch(request_with_method("GET", "/\\evil.example/"))
        .await;
    assert_eq!(res.status, 308);
    assert_eq!(res.headers.get("location").unwrap(), "/%5Cevil.example");

    let res = app
        .dispatch(request_with_method("GET", "///nested//path/"))
        .await;
    assert_eq!(res.status, 308);
    assert_eq!(res.headers.get("location").unwrap(), "/nested//path");
}

#[tokio::test]
async fn openapi_document_and_docs_routes_are_served() {
    let mut app = App::new();
    let _ = app
        .get("/users", |_r: Request| Response::send("list"))
        .unwrap()
        .summary("Lista usuarios")
        .tag("users");
    let _ = app
        .post("/users", |_r: Request| Response::send("create"))
        .unwrap();
    let _ = app
        .get("/users/:id", |_r: Request| Response::send("show"))
        .unwrap();
    let _ = app
        .all("/health", |_r: Request| Response::send("ok"))
        .unwrap();
    let _ = app
        .route(
            Method::from_bytes(b"PURGE").unwrap(),
            "/cache",
            |_r: Request| Response::send("ok"),
        )
        .unwrap();
    let _ = app
        .get("/literal/%7Bname%7D", |_r: Request| Response::send("ok"))
        .unwrap();
    app.serve_docs("/docs", "Mi API", "0.2.0").unwrap();

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
    // Methods without an OpenAPI Path Item key remain visible in a
    // namespaced extension rather than becoming invalid operations.
    assert_eq!(
        doc["paths"]["/health"]["x-rustrest-custom-methods"][0]["method"],
        "*"
    );
    assert_eq!(
        doc["paths"]["/cache"]["x-rustrest-custom-methods"][0]["method"],
        "PURGE"
    );
    assert!(doc["paths"]["/literal/%7Bname%7D"]["get"].is_object());
    assert!(doc["paths"]["/literal/{name}"].is_null());

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
fn swagger_ui_escapes_html_and_javascript_configuration() {
    let html = super::openapi::swagger_ui_html(
        r#"</title><script>alert("title")</script>"#,
        "/docs/spec.json\"});</script><script>alert('url')</script>",
    );

    assert!(!html.contains("</title><script>"));
    assert!(html.contains("&lt;/title&gt;&lt;script&gt;"));
    assert!(!html.contains("</script><script>alert('url')"));
    assert!(html.contains(
        r#"url: "/docs/spec.json\"});\u003C/script\u003E\u003Cscript\u003Ealert('url')\u003C/script\u003E","#
    ));
    assert!(html.contains("swagger-ui-dist@5.17.14/"));
    assert!(!html.contains("swagger-ui-dist@5/"));
    assert!(html.contains("script-src 'sha256-"));
    assert!(!html.contains("script-src 'unsafe-inline'"));
}

#[test]
fn openapi_preserves_first_duplicate_operation_and_reports_variants() {
    let routes = vec![
        RouteInfo {
            method: "GET".to_string(),
            path: "/users/:id".to_string(),
            summary: Some("primera".to_string()),
            description: None,
            tags: vec![],
        },
        RouteInfo {
            method: "GET".to_string(),
            path: "/users/:id".to_string(),
            summary: Some("segunda".to_string()),
            description: None,
            tags: vec![],
        },
    ];

    let document = super::openapi::build_document("API", "1", &routes);
    let operation = &document["paths"]["/users/{id}"]["get"];
    assert_eq!(operation["summary"], "primera");
    assert_eq!(operation["x-rustrest-duplicate-routes"], 2);
}

#[test]
fn router_index_refreshes_after_mount() {
    let mut root = Router::new();
    // Force a lookup (and any lazy index build) before mounting.
    assert!(route_match(&root, "GET", "/api/ping").is_none());

    let mut api = Router::new();
    let _ = api
        .get("/ping", |_r: Request| Response::send("pong"))
        .unwrap();
    root.mount("/api", api).unwrap();

    assert!(route_match(&root, "GET", "/api/ping").is_some());
}

#[tokio::test]
async fn dispatch_runs_sync_handler() {
    let mut app = App::new();
    let _ = app.get("/", |_r: Request| Response::send("sync"));
    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "sync");
}

#[tokio::test]
async fn dispatch_runs_async_handler() {
    let mut app = App::new();
    let _ = app.get("/", |_r: Request| async move { Response::send("async") });
    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "async");
}

#[tokio::test]
async fn dispatch_accepts_result_handlers() {
    let mut app = App::new();
    let _ = app.get("/", |_r: Request| -> Result<Response, &'static str> {
        Ok(Response::send("ok"))
    });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "ok");
}

#[tokio::test]
async fn dispatch_converts_handler_errors_to_500() {
    let mut app = App::new();
    let _ = app.get("/", |_r: Request| -> Result<Response, &'static str> {
        Err("fallo interno sensible")
    });

    let res = app.dispatch(dummy_request("")).await;

    assert_eq!(res.status, 500);
    let problem: serde_json::Value = serde_json::from_slice(res.body_bytes().unwrap()).unwrap();
    assert_eq!(problem["detail"], "Error interno del servidor");
    assert_eq!(problem["code"], "internal_server_error");
    assert!(!res.body_text().contains("sensible"));
}

#[test]
fn string_handler_errors_keep_details_private() {
    let error = "token=secreto".to_string().into_http_error();
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error.public_message(), "Error interno del servidor");
    assert_eq!(error.source().unwrap().to_string(), "token=secreto");
    assert!(!Response::from_error(error).body_text().contains("secreto"));
}

#[tokio::test]
async fn dispatch_catches_panics_as_500_responses() {
    let mut app = App::new();
    let _ = app.get("/", |_r: Request| -> Response { panic!("boom") });

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
    let _ = app.get("/", |req: Request| {
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
    let _ = app.get("/only", |_r: Request| Response::send("get"));

    let res = app.dispatch(request_with_method("POST", "/only")).await;

    assert_eq!(res.status, 405);
    assert_eq!(res.headers.get("allow").unwrap(), "GET, HEAD, OPTIONS");
}

#[tokio::test]
async fn connect_is_rejected_until_a_transport_owning_tunnel_api_exists() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move { next(req).await.header("x-global", "ran") });
    let _ = app.all("/tunnel", |_req: Request| Response::send("unsafe"));

    let response = app
        .dispatch(request_with_method("CONNECT", "/tunnel"))
        .await;
    assert_eq!(response.status, 501);
    assert_eq!(response.headers.get("x-global").unwrap(), "ran");
    let problem: serde_json::Value =
        serde_json::from_slice(response.body_bytes().unwrap()).unwrap();
    assert_eq!(problem["code"], "connect_not_supported");
}

#[tokio::test]
async fn connect_rejection_cannot_be_rewritten_to_a_tunnel_success() {
    let mut app = App::new();
    app.error_handler(|_error| Response::send("personalizado").status(200));
    app.layer(|req: Request, next: Next| async move {
        let mut response = next(req).await;
        response.status = 204;
        response
    });

    let response = app
        .run_request(request_with_method("CONNECT", "/tunnel"))
        .await;

    assert_eq!(response.status, 501);
}

#[tokio::test]
async fn head_is_auto_served_from_get() {
    let mut app = App::new();
    let _ = app.get("/page", |_r: Request| Response::send("hello"));

    let res = app.run_request(request_with_method("HEAD", "/page")).await;

    // Matched via the GET route; body stripped because the method is HEAD.
    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "");
    assert_eq!(res.headers.get(CONTENT_LENGTH).unwrap(), "5");
}

#[tokio::test]
async fn head_rejects_a_length_that_disagrees_with_the_selected_representation() {
    let mut app = App::new();
    let _ = app.get("/page", |_r: Request| {
        Response::send("hello").header("content-length", "99")
    });

    let res = app.run_request(request_with_method("HEAD", "/page")).await;

    assert_eq!(res.status, 500);
    assert_eq!(res.body_text(), "");
}

#[tokio::test]
async fn options_is_auto_answered_with_allow() {
    let mut app = App::new();
    let _ = app.get("/thing", |_r: Request| Response::send("g"));
    let _ = app.post("/thing", |_r: Request| Response::send("p"));

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
    let _ = app.get("/", |_r: Request| Response::send("handler"));

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
    let _ = app.get("/", |_r: Request| Response::send("handler"));
    app.layer(|_req: Request, _next: Next| async move { Response::send("blocked") });

    let res = app.dispatch(dummy_request("")).await;
    assert_eq!(res.body_text(), "blocked");
}

#[tokio::test]
async fn middlewares_nest_in_registration_order() {
    let mut app = App::new();
    let _ = app.get("/", |_r: Request| Response::send("h"));

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
    let _ = app
        .get("/guarded", |_r: Request| Response::send("guarded"))
        .unwrap()
        .layer(move |req: Request, next: Next| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                next(req).await
            }
        });
    let _ = app
        .get("/open", |_r: Request| Response::send("open"))
        .unwrap();

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
    let _ = scoped.get("/thing", |_r: Request| Response::send("scoped"));

    let mut app = App::new();
    let _ = app.get("/", |_r: Request| Response::send("root"));
    let _ = app.mount("/api", scoped);

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
    let root = unique_temp_dir("static");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("app.css"), "body { color: red; }").unwrap();

    let mut app = App::new();
    let _ = app.static_files("/assets", &root);

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
    let root = unique_temp_dir("static");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("static.txt"), "0123456789").unwrap();

    let mut app = App::new();
    let _ = app.static_files("/assets", &root);
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
    assert!(etag.starts_with("W/\""));
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

    // Range units are case-insensitive.
    let res = client
        .get("/assets/static.txt")
        .header("range", "ByTeS=0-1")
        .send()
        .await;
    assert_eq!(res.status, 206);
    assert_eq!(res.headers.get("content-range").unwrap(), "bytes 0-1/10");
    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"01");

    // Repeated Range fields are combined as a multi-range request. Since this
    // minimal server does not emit multipart/byteranges, it safely ignores
    // the combined range and serves the complete representation.
    let res = client
        .get("/assets/static.txt")
        .header("range", "bytes=0-1")
        .header("range", "bytes=8-9")
        .send()
        .await;
    assert_eq!(res.status, 200);
    let body = res
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"0123456789");

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

    // Range has no effect on HEAD: metadata describes the full selected
    // representation, including for a range that would be unsatisfiable.
    for range in ["bytes=2-5", "bytes=50-"] {
        let res = client
            .request("HEAD", "/assets/static.txt")
            .header("range", range)
            .send()
            .await;
        assert_eq!(res.status, 200);
        assert_eq!(res.headers.get("content-length").unwrap(), "10");
        assert!(res.headers.get("content-range").is_none());
        assert!(res.body_bytes().is_none());
    }

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn static_files_rejects_path_traversal() {
    let root = unique_temp_dir("static");
    fs::create_dir_all(&root).unwrap();

    let mut app = App::new();
    let _ = app.static_files("/assets", &root);

    let res = app
        .dispatch(request_with_method("GET", "/assets/../secret.txt"))
        .await;

    assert_eq!(res.status, 400);

    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn static_files_deny_dotfiles_by_default_and_require_explicit_opt_in() {
    let root = unique_temp_dir("static-dotfiles");
    fs::create_dir_all(root.join(".config")).unwrap();
    fs::write(root.join(".env"), "SECRET=hidden").unwrap();
    fs::write(root.join(".config").join("value"), "allowed explicitly").unwrap();

    let mut safe_app = App::new();
    safe_app.static_files("/assets", &root).unwrap();
    let safe_client = TestClient::new(safe_app);
    assert_eq!(safe_client.get("/assets/.env").send().await.status, 404);
    assert_eq!(safe_client.get("/assets/%2Eenv").send().await.status, 404);
    assert_eq!(
        safe_client.get("/assets/.config/value").send().await.status,
        404
    );

    let mut opted_in = App::new();
    opted_in
        .static_files_with_options(
            "/assets",
            &root,
            StaticFilesOptions::new().dotfiles(Dotfiles::Allow),
        )
        .unwrap();
    let response = TestClient::new(opted_in)
        .get("/assets/.env")
        .send()
        .await
        .into_hyper();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"SECRET=hidden");

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn static_files_rejects_a_missing_root_at_registration() {
    let root = unique_temp_dir("missing-static-root");
    let mut app = App::new();
    let error = app.static_files("/assets", &root).unwrap_err();
    assert_eq!(error.kind(), RouteErrorKind::InvalidStaticRoot);
}

#[tokio::test]
async fn static_files_pins_the_open_root_directory() {
    let base = unique_temp_dir("static-root-capability");
    let root = base.join("public");
    let moved = base.join("original");
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("value.txt"), "original").unwrap();

    let mut app = App::new();
    app.static_files("/assets", &root).unwrap();

    fs::rename(&root, &moved).unwrap();
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("value.txt"), "replacement").unwrap();

    let response = app
        .dispatch(request_with_method("GET", "/assets/value.txt"))
        .await
        .into_hyper();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"original");

    fs::remove_dir_all(base).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn static_files_allows_internal_symlinks_and_rejects_external_ones() {
    use std::os::unix::fs::symlink;

    let base = unique_temp_dir("static-symlinks");
    let root = base.join("public");
    let outside = base.join("secret-dir");
    fs::create_dir_all(&root).unwrap();
    fs::create_dir_all(&outside).unwrap();
    fs::write(root.join("real.txt"), "contenido publico").unwrap();
    fs::write(outside.join("secret.txt"), "contenido secreto").unwrap();
    symlink("real.txt", root.join("internal.txt")).unwrap();
    symlink("../secret-dir", root.join("leak")).unwrap();

    let mut app = App::new();
    app.static_files("/assets", &root).unwrap();

    let internal = app
        .dispatch(request_with_method("GET", "/assets/internal.txt"))
        .await;
    assert_eq!(internal.status, 200);
    let body = internal
        .into_hyper()
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(&body[..], b"contenido publico");

    let external = app
        .dispatch(request_with_method("GET", "/assets/leak/secret.txt"))
        .await;
    assert_eq!(external.status, 404);

    fs::remove_dir_all(base).unwrap();
}

#[tokio::test]
async fn response_streams_body_chunks() {
    let chunks = stream::iter(vec![
        Ok::<_, Infallible>(Bytes::from_static(b"hello ")),
        Ok::<_, Infallible>(Bytes::from_static(b"stream")),
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
    let _ = app.head("/", |_r: Request| Response::send("no body"));

    let res = app.run_request(request_with_method("HEAD", "/")).await;

    assert_eq!(res.status, 200);
    assert_eq!(res.body_text(), "");
}

// RFC 9110 §9.3.2: HEAD is GET without content. A shallower fallback,
// `all()` or static root must not shadow the GET route for HEAD.
#[tokio::test]
async fn head_mirrors_get_even_when_a_fallback_or_wildcard_exists() {
    let mut app = App::new();
    app.get("/users", |_r: Request| Response::send("users"))
        .unwrap();
    app.all("/files/*rest", |_r: Request| Response::send("any"))
        .unwrap();
    app.get("/files/report", |_r: Request| Response::send("report"))
        .unwrap();
    app.fallback(|_r: Request| Response::send("Not found").status(404))
        .unwrap();

    let head = app.run_request(request_with_method("HEAD", "/users")).await;
    assert_eq!(head.status, 200);
    assert_eq!(head.body_text(), "");
    assert_eq!(
        head.headers
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some("5")
    );

    // Same-node exact GET beats all() for HEAD as well.
    let head = app
        .run_request(request_with_method("HEAD", "/files/report"))
        .await;
    assert_eq!(
        head.headers
            .get(CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()),
        Some("6")
    );

    let missing = app.run_request(request_with_method("HEAD", "/nope")).await;
    assert_eq!(missing.status, 404);
}

#[tokio::test]
async fn explicit_head_route_still_beats_get_and_websocket_get_is_never_head() {
    let mut app = App::new();
    app.get("/x", |_r: Request| Response::send("get")).unwrap();
    app.head("/x", |_r: Request| {
        Response::send("h").header("x-head", "1")
    })
    .unwrap();
    app.websocket("/ws", |_socket| async {}).unwrap();
    app.fallback(|_r: Request| Response::send("fallback").status(404))
        .unwrap();

    let head = app.run_request(request_with_method("HEAD", "/x")).await;
    assert_eq!(head.headers.get("x-head").unwrap(), "1");

    let ws = app.run_request(request_with_method("HEAD", "/ws")).await;
    assert_eq!(
        ws.status, 404,
        "the fallback answers, not the WebSocket route"
    );
}

// RFC 9110 §15.5.2: a 401 MUST carry WWW-Authenticate. Headers and an
// explicit status set on an error response must survive a custom renderer.
#[tokio::test]
async fn custom_error_handler_keeps_headers_and_status_set_on_error_responses() {
    let mut app = App::new();
    app.error_handler(|error| Response::send(error.code()).status(error.status().as_u16()));
    app.get("/private", |_r: Request| {
        Response::from_error(HttpError::unauthorized("Credenciales requeridas"))
            .header("www-authenticate", "Bearer")
            .header("cache-control", "no-store")
    })
    .unwrap();
    app.get("/unprocessable", |_r: Request| {
        Response::from_error(HttpError::bad_request("Datos no validos")).status(422)
    })
    .unwrap();

    let res = app
        .run_request(request_with_method("GET", "/private"))
        .await;
    assert_eq!(res.status, 401);
    assert_eq!(res.headers.get(WWW_AUTHENTICATE).unwrap(), "Bearer");
    assert_eq!(res.headers.get(CACHE_CONTROL).unwrap(), "no-store");
    assert_eq!(res.body_text(), "unauthorized");

    let res = app
        .run_request(request_with_method("GET", "/unprocessable"))
        .await;
    assert_eq!(res.status, 422);
}

// `Option<E>` turns a *genuinely absent* value into `None` (0.3 semantics);
// malformed input still propagates.
#[tokio::test]
async fn optional_extractors_treat_genuine_absence_as_none() {
    #[derive(Clone)]
    struct User;
    struct Db;
    #[derive(Deserialize)]
    struct Paging {
        #[allow(dead_code)]
        page: u32,
    }
    #[derive(Deserialize)]
    struct Input {
        #[allow(dead_code)]
        name: String,
    }

    let mut app = App::new();
    app.get("/ext", |user: Option<Extension<User>>| {
        Response::send(if user.is_some() { "user" } else { "anon" })
    })
    .unwrap();
    app.get("/state", |db: Option<State<Db>>| {
        Response::send(if db.is_some() { "db" } else { "no-db" })
    })
    .unwrap();
    app.get("/query", |paging: Option<Query<Paging>>| {
        Response::send(if paging.is_some() { "paged" } else { "unpaged" })
    })
    .unwrap();
    app.post("/json", |input: Option<Json<Input>>| async move {
        Response::send(if input.is_some() { "body" } else { "no-body" })
    })
    .unwrap();

    let client = TestClient::new(app);
    assert_eq!(client.get("/ext").send().await.body_text(), "anon");
    assert_eq!(client.get("/state").send().await.body_text(), "no-db");
    assert_eq!(client.get("/query").send().await.body_text(), "unpaged");
    assert_eq!(client.post("/json").send().await.body_text(), "no-body");

    // Present but malformed values are still errors.
    assert_eq!(client.get("/query?page=x").send().await.status, 400);
    assert_eq!(
        client
            .post("/json")
            .header("content-type", "text/plain")
            .body("{}")
            .send()
            .await
            .status,
        415
    );
    assert_eq!(
        client
            .post("/json")
            .header("content-type", "application/json")
            .body("{bad")
            .send()
            .await
            .status,
        400
    );
}

// A scalar path parameter is the decoded segment text, never a JSON document:
// quotes are data and JSON escapes cannot smuggle control characters.
#[tokio::test]
async fn scalar_path_parameters_are_not_json_decoded() {
    let mut app = App::new();
    app.get("/tags/:tag", |Path(tag): Path<String>| Response::send(&tag))
        .unwrap();
    app.get("/ids/:id", |Path(id): Path<u64>| {
        Response::send(&id.to_string())
    })
    .unwrap();
    app.get("/flags/:flag", |Path(flag): Path<bool>| {
        Response::send(&flag.to_string())
    })
    .unwrap();
    let client = TestClient::new(app);

    assert_eq!(
        client.get("/tags/%22quoted%22").send().await.body_text(),
        "\"quoted\""
    );
    let escaped = client
        .get("/tags/%22a%5Cu0000b%5Cr%5Cnc%22")
        .send()
        .await
        .body_text()
        .to_string();
    assert_eq!(escaped, r#""a\u0000b\r\nc""#);
    assert!(!escaped.contains('\0') && !escaped.contains('\n'));
    assert_eq!(client.get("/tags/plain").send().await.body_text(), "plain");
    assert_eq!(client.get("/ids/007").send().await.body_text(), "7");
    assert_eq!(client.get("/ids/-1").send().await.status, 400);
    assert_eq!(client.get("/flags/true").send().await.body_text(), "true");
}

#[tokio::test]
async fn session_id_is_never_taken_from_a_client_header_outside_the_middleware() {
    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let mut app = App::new();
    let mut scoped = Router::new();
    scoped.layer(sessions.middleware());
    scoped
        .get("/id", |req: Request| {
            Response::send(req.session_id().unwrap_or("none"))
        })
        .unwrap();
    app.mount("/with", scoped).unwrap();
    app.get("/without", |req: Request| {
        Response::send(req.session_id().unwrap_or("none"))
    })
    .unwrap();
    let client = TestClient::new(app);

    let outside = client
        .get("/without")
        .header("x-session-id", "victim-session")
        .send()
        .await;
    assert_eq!(outside.body_text(), "none");
    let inside = client
        .get("/with/id")
        .header("x-session-id", "victim-session")
        .send()
        .await;
    assert_ne!(inside.body_text(), "victim-session");
    assert_ne!(inside.body_text(), "none");
}

#[tokio::test]
async fn request_timeout_response_still_passes_through_global_middleware() {
    let mut app = App::new();
    app.request_timeout(Duration::from_millis(30));
    app.layer(|req: Request, next: Next| async move { next(req).await.header("x-outer", "1") });
    app.error_handler(|error| {
        Response::send(error.code())
            .status(error.status().as_u16())
            .header("x-rendered", "1")
    });
    app.get("/slow", |_r: Request| async {
        tokio::time::sleep(Duration::from_secs(5)).await;
        Response::send("late")
    })
    .unwrap();

    let res = app.run_request(request_with_method("GET", "/slow")).await;
    assert_eq!(res.status, 408);
    assert_eq!(res.headers.get("x-outer").unwrap(), "1");
    assert_eq!(res.headers.get("x-rendered").unwrap(), "1");
}

#[tokio::test]
async fn matched_path_is_absent_for_synthesized_misses() {
    let mut app = App::new();
    app.layer(|req: Request, next: Next| async move {
        let matched = req.route_pattern().unwrap_or("none").to_string();
        next(req).await.header("x-matched", &matched)
    });
    app.get("/users/:id", |_r: Request| Response::send("ok"))
        .unwrap();
    let client = TestClient::new(app);

    let hit = client.get("/users/42").send().await;
    assert_eq!(hit.headers.get("x-matched").unwrap(), "/users/:id");
    for (method, path) in [
        ("GET", "/nope/123"),
        ("DELETE", "/users/42"),
        ("OPTIONS", "/users/42"),
    ] {
        let res = client.request(method, path).send().await;
        assert_eq!(
            res.headers.get("x-matched").unwrap(),
            "none",
            "{method} {path}"
        );
    }
}

#[tokio::test]
async fn compression_keeps_vary_on_not_modified_responses() {
    let mut app = App::new();
    app.layer(middleware::compression());
    app.get("/cached", |_r: Request| Response::send("").status(304))
        .unwrap();
    let res = TestClient::new(app)
        .get("/cached")
        .header("accept-encoding", "gzip")
        .send()
        .await;
    assert_eq!(res.status, 304);
    assert!(
        res.headers
            .get(VARY)
            .unwrap()
            .to_str()
            .unwrap()
            .eq_ignore_ascii_case("accept-encoding")
    );
}

#[tokio::test]
async fn cors_any_origin_with_credentials_never_grants_the_null_origin() {
    let mut app = App::new();
    app.layer(
        middleware::Cors::new()
            .allow_any_origin()
            .allow_credentials(true),
    );
    app.get("/me", |_r: Request| Response::send("secret"))
        .unwrap();
    let client = TestClient::new(app);

    let real = client
        .get("/me")
        .header("origin", "https://app.example")
        .send()
        .await;
    assert_eq!(
        real.headers.get("access-control-allow-origin").unwrap(),
        "https://app.example"
    );
    let sandboxed = client.get("/me").header("origin", "null").send().await;
    assert!(
        sandboxed
            .headers
            .get("access-control-allow-origin")
            .is_none()
    );
    assert!(
        sandboxed
            .headers
            .get("access-control-allow-credentials")
            .is_none()
    );
}

#[test]
fn url_for_never_builds_scheme_relative_or_shifted_urls() {
    let mut app = App::new();
    app.get("/*path", |_r: Request| Response::send("any"))
        .unwrap()
        .name("any")
        .unwrap();
    app.get("/users/:id", |_r: Request| Response::send("user"))
        .unwrap()
        .name("user")
        .unwrap();

    let url = app.url_for("any", [("path", "/evil.example/x")]).unwrap();
    assert_eq!(url, "/evil.example/x");
    assert!(!url.starts_with("//"));
    let error = app.url_for("user", [("id", "")]).unwrap_err();
    assert_eq!(error.kind(), RouteErrorKind::MissingUrlParameter);
}

#[test]
fn redirects_emit_valid_uri_references_and_require_a_redirect_status() {
    let res = Response::redirect("/café/b c?q=ñ&x=%20#frag");
    assert_eq!(res.status, 302);
    assert_eq!(
        res.headers.get(LOCATION).unwrap(),
        "/caf%C3%A9/b%20c?q=%C3%B1&x=%20#frag"
    );
    // A literal percent that is not an escape is encoded; valid escapes stay.
    assert_eq!(
        Response::redirect("/100%/ok%2F")
            .headers
            .get(LOCATION)
            .unwrap(),
        "/100%25/ok%2F"
    );
    assert_eq!(Response::redirect_with_status("/new", 308).status, 308);
    // Only 3xx statuses are redirects; anything else is a construction error
    // rendered as 500 at the HTTP boundary.
    let invalid = Response::redirect_with_status("/new", 200).finalize();
    assert_eq!(invalid.status, 500);
}

// Router-scoped middleware (CORS, guards) also applies to the OPTIONS and 405
// responses synthesized for that router's own paths; route-level layers do not.
#[tokio::test]
async fn router_scoped_middleware_covers_synthesized_options_and_405() {
    let mut api = Router::new();
    api.layer(middleware::Cors::new().allow_origin("https://app.example"));
    api.get("/items", |_r: Request| Response::send("items"))
        .unwrap()
        .layer(|_req: Request, _next: Next| async move {
            Response::send("route layer must not run for other methods").status(418)
        });
    let mut admin = Router::new();
    admin.guard(|_req: &Request| false);
    admin
        .get("/users", |_r: Request| Response::send("secret"))
        .unwrap();
    let mut app = App::new();
    app.mount("/api", api).unwrap();
    app.mount("/admin", admin).unwrap();
    let client = TestClient::new(app);

    let preflight = client
        .request("OPTIONS", "/api/items")
        .header("origin", "https://app.example")
        .header("access-control-request-method", "GET")
        .send()
        .await;
    assert_eq!(
        preflight
            .headers
            .get("access-control-allow-origin")
            .unwrap(),
        "https://app.example"
    );
    let not_allowed = client.request("DELETE", "/api/items").send().await;
    assert_eq!(not_allowed.status, 405);

    let probe = client.request("DELETE", "/admin/users").send().await;
    assert_eq!(probe.status, 403, "a guard must not leak routes via 405");
    assert!(probe.headers.get(ALLOW).is_none());
}

// OWASP session management: rotate the id when privilege changes (login) so
// a fixated pre-login id is useless afterwards.
#[tokio::test]
async fn sessions_regenerate_rotates_the_id_and_keeps_the_data() {
    fn cookie_pair(response: &Response) -> String {
        response
            .headers
            .get(SET_COOKIE)
            .expect("session cookie")
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    let sessions = Sessions::new(TEST_SESSION_SECRET);
    let mut app = App::new();
    app.layer(sessions.middleware());
    let writer = sessions.clone();
    app.get("/cart", move |req: Request| {
        writer
            .set(req.session_id().unwrap(), "cart", "3 items")
            .unwrap();
        Response::send("ok")
    })
    .unwrap();
    let login = sessions.clone();
    app.get("/login", move |req: Request| {
        let id = login.regenerate(req.session_id().unwrap()).unwrap();
        login.set(&id, "user", "ada").unwrap();
        Response::send("logged in")
    })
    .unwrap();
    let reader = sessions.clone();
    app.get("/whoami", move |req: Request| {
        let id = req.session_id().unwrap();
        Response::send(&format!(
            "{}|{}",
            reader.get(id, "user").unwrap_or_default(),
            reader.get(id, "cart").unwrap_or_default()
        ))
    })
    .unwrap();
    let client = TestClient::new(app);

    let before = cookie_pair(&client.get("/cart").send().await);
    let login_response = client.get("/login").header("cookie", &before).send().await;
    let after = cookie_pair(&login_response);
    assert_ne!(before, after, "login must issue a new session id");

    let current = client.get("/whoami").header("cookie", &after).send().await;
    assert_eq!(current.body_text(), "ada|3 items");
    let fixated = client.get("/whoami").header("cookie", &before).send().await;
    assert_eq!(fixated.body_text(), "|", "the old id must no longer work");
    assert_eq!(sessions.len(), 1);
}
