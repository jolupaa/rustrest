#![forbid(unsafe_code)]

mod api;
mod users;

use rustrest::{App, Next, Request, Response};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();

    // Global middleware (onion model): runs before and after the handler.
    app.layer(|req: Request, next: Next| async move {
        println!("--> {} {}", req.method, req.path);
        let res = next(req).await;
        println!("<-- {} ({})", res.status, res.content_type);
        res
    });

    // Root route (synchronous handler).
    app.get("/", |_req: Request| {
        Response::send("Hello from my Rust framework")
    });

    // Routes and sub-routes organized in files: `api` mounts `users`.
    // Result: /api/users, /api/users/:id, ...
    app.mount("/api", api::router());

    app.listen("127.0.0.1:3000").await
}
