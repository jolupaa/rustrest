use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use rustrest::{App, HttpError, Request, Response};
use serde::Serialize;
use tokio::io::AsyncWriteExt;

const MAX_UPLOAD_BYTES: usize = 10 * 1024 * 1024;

#[derive(Serialize)]
struct UploadResult {
    bytes: u64,
    path: String,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut app = App::new();

    app.post("/upload", upload)
        .unwrap()
        .body_limit(MAX_UPLOAD_BYTES);

    println!("Servidor escuchando en http://127.0.0.1:3000");
    println!("Prueba: curl -X POST --data-binary @archivo.bin http://127.0.0.1:3000/upload");
    app.listen("127.0.0.1:3000").await
}

async fn upload(mut req: Request) -> Result<Response, HttpError> {
    let path = upload_path();
    let mut file = tokio::fs::File::create(&path).await.map_err(|err| {
        HttpError::internal_server_error("No se pudo crear el upload").with_source(err)
    })?;
    let mut stream = req.take_body_stream()?;
    let mut written = 0_u64;

    while let Some(chunk) = stream.next().await {
        let chunk = match chunk {
            Ok(chunk) => chunk,
            Err(error) => {
                remove_partial(&path).await;
                return Err(HttpError::body_read(error));
            }
        };
        written = match written
            .checked_add(chunk.len() as u64)
            .filter(|bytes| *bytes <= MAX_UPLOAD_BYTES as u64)
        {
            Some(written) => written,
            None => {
                remove_partial(&path).await;
                return Err(HttpError::payload_too_large_limit(MAX_UPLOAD_BYTES));
            }
        };
        if let Err(error) = file.write_all(&chunk).await {
            remove_partial(&path).await;
            return Err(
                HttpError::internal_server_error("No se pudo escribir el upload")
                    .with_source(error),
            );
        }
    }

    if let Err(error) = file.flush().await {
        remove_partial(&path).await;
        return Err(
            HttpError::internal_server_error("No se pudo finalizar el upload").with_source(error),
        );
    }

    Ok(Response::json(&UploadResult {
        bytes: written,
        path: path.display().to_string(),
    })
    .status(201))
}

fn upload_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustrest-upload-{}-{nanos}.bin",
        std::process::id()
    ))
}

async fn remove_partial(path: &Path) {
    let _ = tokio::fs::remove_file(path).await;
}
