use hyper::Method;
use rustrest::{App, HostPattern, Request, Response, RouteErrorKind, RouteMatchErrorKind, Router};

fn handler(_req: Request) -> Response {
    Response::send("ok")
}

#[test]
fn invalid_and_duplicate_routes_are_rejected_at_registration() {
    let mut router = Router::new();
    assert_eq!(
        router.get("users/:id", handler).unwrap_err().kind(),
        RouteErrorKind::MissingLeadingSlash
    );
    assert_eq!(
        router.get("/users/:id/:id", handler).unwrap_err().kind(),
        RouteErrorKind::DuplicateParameter
    );
    assert_eq!(
        router.get("/files/*path/more", handler).unwrap_err().kind(),
        RouteErrorKind::NonTerminalWildcard
    );

    router.get("/users/:id", handler).unwrap();
    assert_eq!(
        router.get("/users/:name", handler).unwrap_err().kind(),
        RouteErrorKind::ConflictingPattern
    );
}

#[test]
fn named_route_generates_an_encoded_url() {
    let mut app = App::new();
    app.get("/users/:id/files/*path", handler)
        .unwrap()
        .name("users.file")
        .unwrap();

    let url = app
        .url_for(
            "users.file",
            [("id", "José / admin"), ("path", "reports/June 2026.pdf")],
        )
        .unwrap();
    assert_eq!(
        url,
        "/users/Jos%C3%A9%20%2F%20admin/files/reports/June%202026.pdf"
    );
}

#[test]
fn captured_params_reject_invalid_percent_encoding() {
    let mut router = Router::new();
    router.get("/users/:id", handler).unwrap();

    let malformed = match router.resolve(&Method::GET, "/users/%ZZ", None) {
        Err(error) => error,
        Ok(_) => panic!("expected invalid path encoding"),
    };
    assert_eq!(malformed.kind(), RouteMatchErrorKind::InvalidPathEncoding);

    let invalid_utf8 = match router.resolve(&Method::GET, "/users/%C3%28", None) {
        Err(error) => error,
        Ok(_) => panic!("expected invalid UTF-8 in path parameter"),
    };
    assert_eq!(
        invalid_utf8.kind(),
        RouteMatchErrorKind::InvalidPathEncoding
    );
}

#[test]
fn custom_method_and_host_constraints_are_routed() {
    let mut router = Router::new();
    router
        .route(Method::CONNECT, "/tunnel/:id", handler)
        .unwrap()
        .host("api.example.com")
        .unwrap();

    assert!(
        router
            .resolve(&Method::CONNECT, "/tunnel/7", Some("api.example.com"))
            .unwrap()
            .is_some()
    );
    assert!(
        router
            .resolve(&Method::CONNECT, "/tunnel/7", Some("www.example.com"))
            .unwrap()
            .is_none()
    );

    router
        .get("/wild/:id", handler)
        .unwrap()
        .host("*.example.com")
        .unwrap();
    assert!(
        router
            .resolve(&Method::GET, "/wild/7", Some("API.EXAMPLE.COM:443"))
            .unwrap()
            .is_some()
    );
    assert!(
        router
            .resolve(&Method::GET, "/wild/7", Some("example.com"))
            .unwrap()
            .is_none()
    );

    assert_eq!(
        HostPattern::parse("user@example.com").unwrap_err().kind(),
        RouteErrorKind::InvalidHostPattern
    );
}

#[test]
fn nested_mount_keeps_disjoint_host_duplicates_valid() {
    let mut api_a = Router::new();
    api_a
        .get("/status", handler)
        .unwrap()
        .host("a.example.com")
        .unwrap();

    let mut api_b = Router::new();
    api_b
        .get("/status", handler)
        .unwrap()
        .host("b.example.com")
        .unwrap();

    let mut grouped = Router::new();
    grouped.mount("/", api_a).unwrap();
    grouped.mount("/", api_b).unwrap();

    let mut root = Router::new();
    root.mount("/api", grouped).unwrap();

    assert!(
        root.resolve(&Method::GET, "/api/status", Some("a.example.com"))
            .unwrap()
            .is_some()
    );
    assert!(
        root.resolve(&Method::GET, "/api/status", Some("b.example.com"))
            .unwrap()
            .is_some()
    );
}

#[test]
fn failed_route_handle_configuration_removes_the_pending_route() {
    let mut router = Router::new();
    router
        .get("/one", handler)
        .unwrap()
        .name("route.one")
        .unwrap();

    let duplicate_name = router
        .get("/two", handler)
        .unwrap()
        .name("route.one")
        .unwrap_err();
    assert_eq!(duplicate_name.kind(), RouteErrorKind::DuplicateName);
    assert!(
        router
            .resolve(&Method::GET, "/two", None)
            .unwrap()
            .is_none()
    );

    let invalid_host = router
        .get("/admin", handler)
        .unwrap()
        .host("user@example.com")
        .unwrap_err();
    assert_eq!(invalid_host.kind(), RouteErrorKind::InvalidHostPattern);
    assert!(
        router
            .resolve(&Method::GET, "/admin", Some("example.com"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn multi_route_registration_failures_do_not_partially_mutate() {
    let mut app = App::new();
    app.head("/assets/*path", handler).unwrap();

    let error = app.static_files("/assets", "public").unwrap_err();
    assert_eq!(error.kind(), RouteErrorKind::DuplicateRoute);
    assert!(
        app.routes()
            .iter()
            .all(|route| route.method != "GET" || route.path != "/assets/*path")
    );

    let mut docs_app = App::new();
    docs_app.get("/docs", handler).unwrap();
    let error = docs_app.serve_docs("/docs", "Mi API", "0.3.0").unwrap_err();
    assert_eq!(error.kind(), RouteErrorKind::DuplicateRoute);
    assert!(
        docs_app
            .routes()
            .iter()
            .all(|route| route.path != "/docs/openapi.json")
    );
}
