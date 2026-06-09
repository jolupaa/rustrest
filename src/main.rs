mod app;

use app::{App, Request, Response};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct User {
    id: u32,
    name: String,
}

#[tokio::main]
async fn main() {
    let mut app = App::new();

    // Handler SÍNCRONO: `|req| Response`.
    app.get("/", |_req: Request| {
        Response::send("Hola desde mi framework en Rust")
    });

    // Handler ASÍNCRONO: `|req| async { Response }`. Aquí podrías `await`
    // una consulta a base de datos, una llamada HTTP, etc.
    app.get("/users", |_req: Request| async move {
        let users = vec![
            User {
                id: 1,
                name: "Ada".to_string(),
            },
            User {
                id: 2,
                name: "Linus".to_string(),
            },
        ];
        Response::json(&users)
    });

    // Crear: acceso al body JSON -> struct.
    app.post("/users", |req: Request| match req.json::<User>() {
        Ok(user) => Response::json(&user),
        Err(_) => Response::bad_request(),
    });

    // PATH PARAM: `:id` se captura y se lee con `req.param("id")`.
    app.get("/users/:id", |req: Request| {
        let id = req.param("id").unwrap_or("?");
        Response::send(&format!("Usuario solicitado: {}", id))
    });

    // PUT: path param + body. Toma el `id` de la ruta y el resto del body.
    app.put("/users/:id", |req: Request| {
        let id = req.param("id").unwrap_or("0").to_string();
        match req.json::<User>() {
            Ok(mut user) => {
                user.id = id.parse().unwrap_or(user.id);
                Response::json(&user)
            }
            Err(_) => Response::bad_request(),
        }
    });

    // DELETE: solo necesita el path param.
    app.delete("/users/:id", |req: Request| {
        let id = req.param("id").unwrap_or("?");
        Response::send(&format!("Usuario {} eliminado", id))
    });

    app.listen("127.0.0.1:3000").await;
}
