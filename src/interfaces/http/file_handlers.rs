use crate::AppState;
use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use std::path::Path;
use tracing::{error, warn};

#[derive(Deserialize)]
pub struct FileRequest {
    path: String,
}

pub async fn get_file_handler(
    State(state): State<AppState>,
    Query(params): Query<FileRequest>,
) -> impl IntoResponse {
    let normalized = Path::new(&params.path)
        .components()
        .collect::<std::path::PathBuf>();

    let allowed = state.nfs_paths.iter().any(|root| {
        let root_path = Path::new(root);
        normalized.starts_with(root_path)
    });

    if !allowed {
        warn!("File access denied: {}", params.path);
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "Access denied"})),
        )
            .into_response();
    }

    match tokio::fs::read(&normalized).await {
        Ok(data) => {
            let mime = mime_for_path(&normalized);
            let headers = [(header::CONTENT_TYPE, mime)];
            (headers, data).into_response()
        }
        Err(e) => {
            error!("File read error {}: {}", params.path, e);
            (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "File not found"})),
            )
                .into_response()
        }
    }
}

fn mime_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|e| e.to_str()) {
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("svg") => "image/svg+xml",
        Some("md") => "text/markdown; charset=utf-8",
        Some("txt") | Some("log") => "text/plain; charset=utf-8",
        Some("html") | Some("htm") => "text/html; charset=utf-8",
        Some("json") => "application/json",
        Some("csv") => "text/csv; charset=utf-8",
        Some("yaml") | Some("yml") => "application/x-yaml",
        Some("xml") => "application/xml",
        _ => "application/octet-stream",
    }
}
