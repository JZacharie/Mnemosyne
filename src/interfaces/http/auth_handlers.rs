use crate::application::use_cases::auth::AuthUseCase;
use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

#[derive(Deserialize)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Serialize)]
pub struct LoginResponse {
    pub token: String,
    pub username: String,
}

pub async fn login_handler(
    State(auth_use_case): State<Arc<AuthUseCase>>,
    Json(payload): Json<LoginRequest>,
) -> Response {
    match auth_use_case
        .login(&payload.username, &payload.password)
        .await
    {
        Ok(user) => {
            info!("User {} logged in successfully", user.username);
            // In a real app, generate a JWT token here
            let token = "dummy-jwt-token".to_string();
            Json(LoginResponse {
                token,
                username: user.username,
            })
            .into_response()
        }
        Err(e) => {
            error!("Login failed for user {}: {}", payload.username, e);
            (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": e.to_string() })),
            )
                .into_response()
        }
    }
}
