#![no_main]

use libfuzzer_sys::fuzz_target;
use rustrest::WebSocketEvent;
use serde_json::Value;

fuzz_target!(|data: &[u8]| {
    if let Ok(event) = serde_json::from_slice::<WebSocketEvent<Value>>(data) {
        let _ = serde_json::to_vec(&event);
    }
});
