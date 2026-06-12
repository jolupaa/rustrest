#![no_main]

use libfuzzer_sys::fuzz_target;
use rustrest::WsHub;

fuzz_target!(|data: &[u8]| {
    let data = &data[..data.len().min(1024)];
    let room = String::from_utf8_lossy(data).into_owned();
    let Ok(hub) = WsHub::builder().build() else {
        return;
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let _ = runtime.block_on(hub.route("/chat/:channel").to(room).send_text("x"));
});
