#![no_main]

use std::sync::Arc;

use libfuzzer_sys::fuzz_target;
use rustrest::{InMemoryWsBroker, WsHub};
use serde::Deserialize;

#[derive(Deserialize)]
struct Envelope {
    route: String,
    room: String,
    payload: Vec<u8>,
}

fuzz_target!(|data: &[u8]| {
    let data = &data[..data.len().min(64 * 1024)];
    let Ok(mut envelope) = serde_json::from_slice::<Envelope>(data) else {
        return;
    };
    envelope.payload.truncate(64 * 1024);
    let broker = Arc::new(InMemoryWsBroker::new(8));
    let Ok(hub) = WsHub::builder().broker(broker).build() else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _ = runtime.block_on(
        hub.route(envelope.route)
            .to(envelope.room)
            .send_binary(envelope.payload),
    );
});
