//! Minimal OpenAPI 3.0 generation from the registered routes, plus the
//! Swagger UI page served by `App::serve_docs`. Path parameters are derived
//! from `:param` / `*wildcard` placeholders (typed as strings); request/
//! response schemas are not introspected.

use base64::Engine;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use super::RouteInfo;

const SWAGGER_UI_VERSION: &str = "5.17.14";
const OPENAPI_OPERATIONS: &[&str] = &[
    "get", "put", "post", "delete", "options", "head", "patch", "trace",
];

/// Builds an OpenAPI 3.0.3 document for the given routes. `all()` routes
/// and extension methods are preserved under `x-rustrest-custom-methods`
/// because OpenAPI Path Items only permit a fixed operation set.
pub(crate) fn build_document(title: &str, version: &str, routes: &[RouteInfo]) -> Value {
    let mut paths = Map::new();
    for route in routes {
        let (path, params) = openapi_path(&route.path);

        let mut operation = Map::new();
        if let Some(summary) = &route.summary {
            operation.insert("summary".to_string(), json!(summary));
        }
        if let Some(description) = &route.description {
            operation.insert("description".to_string(), json!(description));
        }
        if !route.tags.is_empty() {
            operation.insert("tags".to_string(), json!(route.tags));
        }
        if !params.is_empty() {
            let params: Vec<Value> = params
                .iter()
                .map(|name| {
                    json!({
                        "name": name,
                        "in": "path",
                        "required": true,
                        "schema": { "type": "string" },
                    })
                })
                .collect();
            operation.insert("parameters".to_string(), json!(params));
        }
        operation.insert(
            "responses".to_string(),
            json!({ "200": { "description": "OK" } }),
        );

        let item = paths
            .entry(path)
            .or_insert_with(|| Value::Object(Map::new()));
        if let Value::Object(item) = item {
            let method = route.method.to_lowercase();
            if !OPENAPI_OPERATIONS.contains(&method.as_str()) {
                operation.insert("method".to_string(), json!(route.method));
                let custom = item
                    .entry("x-rustrest-custom-methods".to_string())
                    .or_insert_with(|| Value::Array(Vec::new()));
                if let Value::Array(custom) = custom {
                    custom.push(Value::Object(operation));
                }
                continue;
            }
            match item.entry(method) {
                serde_json::map::Entry::Vacant(entry) => {
                    entry.insert(Value::Object(operation));
                }
                serde_json::map::Entry::Occupied(mut entry) => {
                    // Host-specific routes can collapse to the same OpenAPI
                    // path/method because RouteInfo intentionally omits hosts.
                    // Preserve the first registered operation (matching router
                    // precedence) and report how many variants were omitted.
                    if let Value::Object(existing) = entry.get_mut() {
                        let count = existing
                            .get("x-rustrest-duplicate-routes")
                            .and_then(Value::as_u64)
                            .unwrap_or(1)
                            + 1;
                        existing.insert(
                            "x-rustrest-duplicate-routes".to_string(),
                            Value::from(count),
                        );
                    }
                }
            }
        }
    }

    json!({
        "openapi": "3.0.3",
        "info": { "title": title, "version": version },
        "paths": paths,
    })
}

/// Converts a route pattern (`/users/:id/files/*rest`) into an OpenAPI path
/// (`/users/{id}/files/{rest}`), returning the path parameter names.
fn openapi_path(pattern: &str) -> (String, Vec<String>) {
    let mut path = String::new();
    let mut params = Vec::new();
    for segment in pattern.split('/').filter(|s| !s.is_empty()) {
        path.push('/');
        match segment
            .strip_prefix(':')
            .or_else(|| segment.strip_prefix('*'))
        {
            Some(name) => {
                path.push('{');
                path.push_str(name);
                path.push('}');
                params.push(name.to_string());
            }
            None => path.push_str(segment),
        }
    }
    if path.is_empty() {
        path.push('/');
    }
    (path, params)
}

/// The Swagger UI page pointing at `spec_url`. CDN assets use an exact version
/// and a meta CSP authorizes only the generated initializer by its hash.
pub(crate) fn swagger_ui_html(title: &str, spec_url: &str) -> String {
    let title = escape_html_text(title);
    let spec_url = json_string_for_inline_script(spec_url);
    let initializer = format!(
        r##"window.addEventListener("load", () => {{
      SwaggerUIBundle({{ url: {spec_url}, dom_id: "#swagger-ui" }});
    }});"##
    );
    let initializer_hash =
        base64::engine::general_purpose::STANDARD.encode(Sha256::digest(initializer.as_bytes()));
    let stylesheet =
        format!("https://unpkg.com/swagger-ui-dist@{SWAGGER_UI_VERSION}/swagger-ui.css");
    let bundle =
        format!("https://unpkg.com/swagger-ui-dist@{SWAGGER_UI_VERSION}/swagger-ui-bundle.js");
    format!(
        r##"<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; base-uri 'none'; object-src 'none'; connect-src 'self'; img-src data: https:; style-src 'unsafe-inline' https://unpkg.com; script-src 'sha256-{initializer_hash}' https://unpkg.com" />
  <meta name="referrer" content="no-referrer" />
  <title>{title} — API docs</title>
  <link rel="stylesheet" href="{stylesheet}" crossorigin="anonymous" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="{bundle}" crossorigin="anonymous"></script>
  <script>{initializer}</script>
</body>
</html>
"##
    )
}

fn escape_html_text(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(character),
        }
    }
    escaped
}

fn json_string_for_inline_script(value: &str) -> String {
    let serialized = match serde_json::to_string(value) {
        Ok(value) => value,
        Err(_) => "\"\"".to_string(),
    };
    let mut escaped = String::with_capacity(serialized.len());
    for character in serialized.chars() {
        match character {
            '<' => escaped.push_str("\\u003C"),
            '>' => escaped.push_str("\\u003E"),
            '&' => escaped.push_str("\\u0026"),
            '\u{2028}' => escaped.push_str("\\u2028"),
            '\u{2029}' => escaped.push_str("\\u2029"),
            _ => escaped.push(character),
        }
    }
    escaped
}
