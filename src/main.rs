#![forbid(unsafe_code)]

mod api;
mod users;

use rustrest::{App, Next, Request, Response, middleware};

use std::collections::HashMap;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();

    app.layer(middleware::cors());

    // Global middleware (onion model): runs before and after the handler.
    app.layer(|req: Request, next: Next| async move {
        println!("--> {} {}", req.method, req.path);
        let res = next(req).await;
        println!("<-- {} ({})", res.status, res.content_type);
        res
    });

    // Root route (synchronous handler).
    app.get("/", |mut req: Request| async move {
        if let Some(user) = req.params.get("user") {
            Response::send(format!("Hola {}", user).as_str())
        } else {
            let body: HashMap<String, String> = req.json().await.unwrap_or_default();
            Response::send(body.get("Hola").unwrap_or(&"tonto".to_string()).as_str())
        }
    })
    .unwrap();

    // Routes and sub-routes organized in files: `api` mounts `users`.
    // Result: /api/users, /api/users/:id, ...
    app.mount("/api", api::router()).unwrap();

    app.listen("127.0.0.1:3000").await
}
