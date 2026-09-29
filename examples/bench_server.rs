//! Benchmark target: GET `/` (text) and GET `/users/:id` (path param),
//! optionally behind three global middlewares.
//!
//! cargo run --release --example bench_server -- [--addr 127.0.0.1:3000] \
//!     [--layers none|noop|builtin]
//!
//! noop:    three pass-through `next(req).await` middlewares (framework cost).
//! builtin: request_id() + cors() + rate_limit(u32::MAX, 60s).

use std::time::Duration;

use rustrest::{App, Next, Request, Response, middleware};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let (mut addr, mut layers) = ("127.0.0.1:3000".to_string(), "none".to_string());
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().expect("falta el valor de la opcion");
        match flag.as_str() {
            "--addr" => addr = value,
            "--layers" => layers = value,
            other => panic!("Opcion desconocida: {other}"),
        }
    }

    let mut app = App::new();
    match layers.as_str() {
        "none" => {}
        "noop" => {
            for _ in 0..3 {
                app.layer(|req: Request, next: Next| async move { next(req).await });
            }
        }
        "builtin" => {
            app.layer(middleware::request_id());
            app.layer(middleware::cors());
            app.layer(middleware::rate_limit(u32::MAX, Duration::from_secs(60)));
        }
        other => panic!("Capas desconocidas: {other}"),
    }
    app.get("/", |_req: Request| Response::send("Hello from RustRest"))
        .unwrap();
    app.get("/users/:id", |req: Request| {
        Response::send(&format!("user {}", req.param("id").unwrap_or("?")))
    })
    .unwrap();

    println!("Escuchando en http://{addr} (capas={layers})");
    app.listen(addr).await
}
