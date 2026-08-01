#![no_main]

use http::Method;
use libfuzzer_sys::fuzz_target;
use rustrest::{Request, Response, RoutePattern, Router};

fn handler(_request: Request) -> Response {
    Response::send("ok")
}

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let (path, host) = text
        .split_once('\0')
        .map_or((text.as_ref(), None), |(path, host)| {
            (path, (!host.is_empty()).then_some(host))
        });
    let method = match data.first().copied().unwrap_or_default() % 6 {
        0 => Method::GET,
        1 => Method::POST,
        2 => Method::PUT,
        3 => Method::DELETE,
        4 => Method::PATCH,
        _ => Method::OPTIONS,
    };

    let mut router = Router::new();
    router.get("/users/me", handler).unwrap();
    router.get("/users/:id", handler).unwrap();
    router.post("/files/*path", handler).unwrap();
    router.all("/fallback/*rest", handler).unwrap();

    let first = router.resolve(&method, path, host);
    let second = router.resolve(&method, path, host);
    assert_eq!(first.is_ok(), second.is_ok());
    if let (Ok(first), Ok(second)) = (first, second) {
        assert_eq!(
            first.as_ref().map(|matched| (&matched.pattern, &matched.params)),
            second
                .as_ref()
                .map(|matched| (&matched.pattern, &matched.params))
        );
    }

    let _ = RoutePattern::parse(path);
});
