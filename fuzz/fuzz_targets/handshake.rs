#![no_main]

use libfuzzer_sys::fuzz_target;
use rustrest::{Request, Response};

fuzz_target!(|data: &[u8]| {
    let text = String::from_utf8_lossy(data);
    let mut parts = text.split('\n');
    let key = parts.next().unwrap_or_default();
    let version = parts.next().unwrap_or_default();
    let origin = parts.next().unwrap_or_default();
    let request = Request::builder()
        .method("GET")
        .path("/ws")
        .header("host", "localhost")
        .header("upgrade", "websocket")
        .header("connection", "Upgrade")
        .header("sec-websocket-key", key)
        .header("sec-websocket-version", version)
        .header("origin", origin)
        .build();
    let _ = Response::websocket(&request);
});
