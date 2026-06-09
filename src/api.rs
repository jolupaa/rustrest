//! API router: aggregates resource routers (sub-routes) into one mountable
//! unit. Demonstrates nesting routers across files, plus a router-scoped
//! middleware that only runs for routes under this router's mount point.

use crate::app::{Next, Request, Router};
use crate::users;

/// Builds the `/api` router by mounting each resource under its sub-path.
pub fn router() -> Router {
    let mut router = Router::new();

    // Scoped middleware: runs only for routes under wherever this router is
    // mounted (in `main`, under `/api`) — not for top-level routes like `/`.
    router.layer(|req: Request, next: Next| async move {
        println!("    [api] {} {}", req.method, req.path);
        next(req).await
    });

    router.mount("/users", users::router()); // -> <mount>/users, <mount>/users/:id, ...
    router
}
