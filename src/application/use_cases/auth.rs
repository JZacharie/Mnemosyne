use crate::domain::ports::{UserRepository, AuditRepository};
use crate::domain::entities::{User, AuditLog};
use std::sync::Arc;
use anyhow::{Result, anyhow};
use uuid::Uuid;
use chrono::Utc;
use tracing::info;

pub struct AuthUseCase {
    user_repository: Arc<dyn UserRepository>,
    audit_repository: Arc<dyn AuditRepository>,
}

impl AuthUseCase {
    pub fn new(
        user_repository: Arc<dyn UserRepository>,
        audit_repository: Arc<dyn AuditRepository>,
    ) -> Self {
        Self {
            user_repository,
            audit_repository,
        }
    }

    pub async fn login(&self, username: &str, password_attempt: &str) -> Result<User> {
        info!("Login attempt for user: {}", username);
        
        let user = self.user_repository.get_by_username(username).await?
            .ok_or_else(|| anyhow!("User not found"))?;

        // In a real app, verify password hash using bcrypt
        // For now, simple check
        if password_attempt == "password" { // Placeholder
             self.audit_repository.log(AuditLog {
                id: Uuid::new_v4(),
                user_id: user.id,
                action: "LOGIN_SUCCESS".to_string(),
                resource: "auth".to_string(),
                timestamp: Utc::now(),
                metadata: None,
            }).await?;
            
            Ok(user)
        } else {
            self.audit_repository.log(AuditLog {
                id: Uuid::new_v4(),
                user_id: user.id,
                action: "LOGIN_FAILURE".to_string(),
                resource: "auth".to_string(),
                timestamp: Utc::now(),
                metadata: None,
            }).await?;
            
            Err(anyhow!("Invalid password"))
        }
    }
}
