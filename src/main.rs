use rustrest::api;
use rustrest::app::{App, Next, Request, Response};

#[tokio::main]
async fn main() {
    let mut app = App::new();

    // Middleware global (modelo onion): corre antes y después del handler.
    app.layer(|req: Request, next: Next| async move {
        println!("--> {} {}", req.method, req.path);
        let res = next(req).await;
        println!("<-- {} ({})", res.status, res.content_type);
        res
    });

    // Ruta raíz (handler síncrono).
    app.get("/", |_req: Request| {
        Response::send("Hola desde mi framework en Rust")
    });

    // Rutas y subrutas organizadas en archivos: `api` monta `users`.
    // Resultado: /api/users, /api/users/:id, ...
    app.mount("/api", api::router());

    app.listen("127.0.0.1:3000").await;
}
