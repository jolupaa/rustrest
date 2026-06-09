//! Users routes, defined as a standalone `Router` so they can live in their
//! own file and be mounted wherever needed.

use rustrest::{Request, Response, Router};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct User {
    id: u32,
    name: String,
}

/// Builds the users router. Paths here are relative to wherever it is mounted.
pub fn router() -> Router {
    let mut router = Router::new();

    // GET / -> list (async handler)
    router.get("/", |_req: Request| async move {
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

    // GET /:id -> one (path param)
    router.get("/:id", |req: Request| {
        let id = req.param("id").unwrap_or("?");
        Response::send(&format!("Requested user: {}", id))
    });

    // POST / -> create (JSON body)
    router.post("/", |req: Request| match req.json::<User>() {
        Ok(user) => Response::json(&user),
        Err(_) => Response::bad_request(),
    });

    // PUT /:id -> update (path param + body)
    router.put("/:id", |req: Request| {
        let id = req.param("id").unwrap_or("0").to_string();
        match req.json::<User>() {
            Ok(mut user) => {
                user.id = id.parse().unwrap_or(user.id);
                Response::json(&user)
            }
            Err(_) => Response::bad_request(),
        }
    });

    // DELETE /:id -> delete (path param)
    router.delete("/:id", |req: Request| {
        let id = req.param("id").unwrap_or("?");
        Response::send(&format!("User {} deleted", id))
    });

    router
}
