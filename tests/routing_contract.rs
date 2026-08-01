use hyper::Method;
use rustrest::{App, HostPattern, Request, Response, RouteErrorKind, RouteMatchErrorKind, Router};

fn handler(_req: Request) -> Response {
    Response::send("ok")
}

#[test]
fn public_route_error_kinds_support_forward_compatible_matches() {
    let registration = match RouteErrorKind::MissingLeadingSlash {
        RouteErrorKind::MissingLeadingSlash => "missing-leading-slash",
        _ => "future-registration-error",
    };
    assert_eq!(registration, "missing-leading-slash");

    let request = match RouteMatchErrorKind::TooManySegments {
        RouteMatchErrorKind::InvalidPathEncoding => "invalid-path-encoding",
        RouteMatchErrorKind::TooManySegments => "too-many-segments",
        _ => "future-matching-error",
    };
    assert_eq!(request, "too-many-segments");
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
fn encoded_static_route_shapes_do_not_collide_with_placeholders_or_separators() {
    let mut router = Router::new();
    router.get("/:id", handler).unwrap();
    router.get("/%3A", handler).unwrap();
    router.get("/*rest", handler).unwrap();
    router.get("/%2A", handler).unwrap();
    router.get("/a%2Fb", handler).unwrap();
    router.get("/a/b", handler).unwrap();

    let literal_colon = router
        .resolve(&Method::GET, "/%3A", None)
        .unwrap()
        .expect("the encoded colon literal should resolve");
    assert_eq!(literal_colon.pattern, "/%3A");
    assert!(literal_colon.params.is_empty());

    let parameter = router
        .resolve(&Method::GET, "/value", None)
        .unwrap()
        .expect("the parameter route should still resolve");
    assert_eq!(parameter.pattern, "/:id");
    assert_eq!(
        parameter.params.get("id").map(String::as_str),
        Some("value")
    );

    let literal_star = router
        .resolve(&Method::GET, "/%2A", None)
        .unwrap()
        .expect("the encoded star literal should resolve");
    assert_eq!(literal_star.pattern, "/%2A");
    assert!(literal_star.params.is_empty());

    let wildcard = router
        .resolve(&Method::GET, "/other/more", None)
        .unwrap()
        .expect("the wildcard route should still resolve");
    assert_eq!(wildcard.pattern, "/*rest");
    assert_eq!(
        wildcard.params.get("rest").map(String::as_str),
        Some("other/more")
    );

    let encoded_slash = router
        .resolve(&Method::GET, "/a%2Fb", None)
        .unwrap()
        .expect("the encoded slash literal should remain one segment");
    assert_eq!(encoded_slash.pattern, "/a%2Fb");
    assert!(encoded_slash.params.is_empty());

    let path_separator = router
        .resolve(&Method::GET, "/a/b", None)
        .unwrap()
        .expect("the two-segment literal should resolve independently");
    assert_eq!(path_separator.pattern, "/a/b");
    assert!(path_separator.params.is_empty());
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

    for control in ["/users/%00", "/users/%0D%0A", "/users/%7F"] {
        let error = match router.resolve(&Method::GET, control, None) {
            Err(error) => error,
            Ok(_) => panic!("expected decoded control character to be rejected"),
        };
        assert_eq!(error.kind(), RouteMatchErrorKind::InvalidPathEncoding);
    }
}

#[test]
fn percent_decoding_precedes_trie_matching_and_happens_once() {
    let mut router = Router::new();
    // Register the parameter first to prove that the encoded static segment
    // still follows normal static-over-parameter precedence.
    router.get("/users/:id", handler).unwrap();
    router.get("/users/me", handler).unwrap();
    router.get("/items/:id", handler).unwrap();
    router.get("/café", handler).unwrap();

    let encoded_static = router
        .resolve(&Method::GET, "/users/%6De", None)
        .unwrap()
        .expect("encoded static path should match");
    assert!(encoded_static.params.is_empty());

    let encoded_unicode = router
        .resolve(&Method::GET, "/caf%C3%A9", None)
        .unwrap()
        .expect("encoded UTF-8 static path should match");
    assert!(encoded_unicode.params.is_empty());

    let encoded_slash = router
        .resolve(&Method::GET, "/items/%2F", None)
        .unwrap()
        .expect("encoded slash should remain one parameter segment");
    assert_eq!(
        encoded_slash.params.get("id").map(String::as_str),
        Some("/")
    );

    let encoded_percent = router
        .resolve(&Method::GET, "/items/%252F", None)
        .unwrap()
        .expect("double-encoded slash should match once-decoded");
    assert_eq!(
        encoded_percent.params.get("id").map(String::as_str),
        Some("%2F")
    );
}

#[test]
fn static_segments_reject_invalid_percent_encoding_and_utf8() {
    let mut router = Router::new();
    router.get("/users/me", handler).unwrap();

    for path in ["/users/%ZZ", "/users/%C3%28"] {
        let error = match router.resolve(&Method::GET, path, None) {
            Err(error) => error,
            Ok(_) => panic!("expected invalid path encoding for {path}"),
        };
        assert_eq!(error.kind(), RouteMatchErrorKind::InvalidPathEncoding);
    }
}

#[test]
fn custom_method_and_host_constraints_are_routed() {
    let mut router = Router::new();
    let purge = Method::from_bytes(b"PURGE").unwrap();
    router
        .route(purge.clone(), "/cache/:id", handler)
        .unwrap()
        .host("api.example.com")
        .unwrap();

    assert!(
        router
            .resolve(&purge, "/cache/7", Some("api.example.com"))
            .unwrap()
            .is_some()
    );
    assert!(
        router
            .resolve(&purge, "/cache/7", Some("www.example.com"))
            .unwrap()
            .is_none()
    );

    let error = router
        .route(Method::CONNECT, "/tunnel/:id", handler)
        .unwrap_err();
    assert_eq!(error.kind(), RouteErrorKind::UnsupportedMethod);

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

    let error = app.static_files("/assets", ".").unwrap_err();
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
