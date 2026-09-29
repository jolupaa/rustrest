//! Compile-time ergonomics that the public API promises.

use rustrest::{App, Request, Response, Router, middleware};

// `?` works on route registration inside the usual `io::Result` main.
fn build() -> std::io::Result<App> {
    let mut app = App::new();
    app.get("/", |_req: Request| Response::send("hola"))?;
    let mut api = Router::new();
    api.get("/ping", |_req: Request| Response::send("pong"))?;
    app.mount("/api", api)?;
    Ok(app)
}

#[test]
fn route_errors_convert_into_io_errors() {
    assert!(build().is_ok());
    let mut app = App::new();
    app.get("/dup", |_req: Request| Response::send("a"))
        .unwrap();
    let error: std::io::Error = app
        .get("/dup", |_req: Request| Response::send("b"))
        .map(|_| ())
        .unwrap_err()
        .into();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("/dup"), "{error}");
}

// `middleware::from_fn` infers the closure's parameter types, so the
// `next: Next` annotation required by `App::layer` closures is not needed.
#[tokio::test]
async fn from_fn_middleware_needs_no_type_annotations() {
    let mut app = App::new();
    app.layer(middleware::from_fn(|req, next| async move {
        next(req).await.header("x-from-fn", "1")
    }));
    let mut api = Router::new();
    api.layer(middleware::from_fn(|req, next| async move {
        next(req).await.header("x-router", "1")
    }));
    api.get("/ping", |_req: Request| Response::send("pong"))
        .unwrap()
        .layer(middleware::from_fn(|req, next| async move {
            next(req).await.header("x-route", "1")
        }));
    app.mount("/api", api).unwrap();

    let response = rustrest::TestClient::new(app).get("/api/ping").send().await;
    for header in ["x-from-fn", "x-router", "x-route"] {
        assert_eq!(response.headers.get(header).unwrap(), "1", "{header}");
    }
}

#[tokio::test]
async fn a_server_that_could_never_admit_a_connection_fails_to_start() {
    let mut app = App::new();
    app.max_connections(0);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(2), app.serve(listener))
        .await
        .expect("serve must fail fast")
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn public_dependency_types_are_reexported() {
    let _status: rustrest::http::StatusCode = rustrest::http::StatusCode::OK;
    let _bytes: rustrest::Bytes = rustrest::Bytes::from_static(b"x");
    fn typed<T: rustrest::headers::Header>() {}
    typed::<rustrest::headers::ContentType>();
}
