use crate::domain::entities::{AuditLog, User};
use crate::domain::ports::{AuditRepository, UserRepository};
use anyhow::{anyhow, Result};
use chrono::Utc;
use std::sync::Arc;
use tracing::info;
use uuid::Uuid;

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

        let user = self
            .user_repository
            .get_by_username(username)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        // In a real app, verify password hash using bcrypt
        // For now, simple check
        if password_attempt == "password" {
            // Placeholder
            self.audit_repository
                .log(AuditLog {
                    id: Uuid::new_v4(),
                    user_id: user.id,
                    action: "LOGIN_SUCCESS".to_string(),
                    resource: "auth".to_string(),
                    timestamp: Utc::now(),
                    metadata: None,
                })
                .await?;

            Ok(user)
        } else {
            self.audit_repository
                .log(AuditLog {
                    id: Uuid::new_v4(),
                    user_id: user.id,
                    action: "LOGIN_FAILURE".to_string(),
                    resource: "auth".to_string(),
                    timestamp: Utc::now(),
                    metadata: None,
                })
                .await?;

            Err(anyhow!("Invalid password"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{AuditLog, User};
    use crate::domain::ports::{AuditRepository, UserRepository};
    use anyhow::Result;
    use async_trait::async_trait;
    use mockall::mock;
    use uuid::Uuid;

    mock! {
        pub UserRepositoryImpl {}
        #[async_trait]
        impl UserRepository for UserRepositoryImpl {
            async fn get_by_username(&self, username: &str) -> Result<Option<User>>;
            async fn create(&self, user: User) -> Result<User>;
        }
    }

    mock! {
        pub AuditRepositoryImpl {}
        #[async_trait]
        impl AuditRepository for AuditRepositoryImpl {
            async fn log(&self, entry: AuditLog) -> Result<()>;
            async fn get_by_user(&self, user_id: Uuid) -> Result<Vec<AuditLog>>;
        }
    }

    #[tokio::test]
    async fn test_login_success() {
        let mut mock_ur = MockUserRepositoryImpl::new();
        let mut mock_ar = MockAuditRepositoryImpl::new();

        let user_id = Uuid::new_v4();
        let test_user = User {
            id: user_id,
            username: "testuser".to_string(),
            password_hash: "password".to_string(),
            email: "test@example.com".to_string(),
            created_at: Utc::now(),
        };

        let user_clone = test_user.clone();
        mock_ur
            .expect_get_by_username()
            .with(mockall::predicate::eq("testuser"))
            .returning(move |_| Ok(Some(user_clone.clone())));

        mock_ar
            .expect_log()
            .withf(move |entry| entry.action == "LOGIN_SUCCESS" && entry.user_id == user_id)
            .returning(|_| Ok(()));

        let use_case = AuthUseCase::new(Arc::new(mock_ur), Arc::new(mock_ar));
        let result = use_case.login("testuser", "password").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().username, "testuser");
    }

    #[tokio::test]
    async fn test_login_invalid_password() {
        let mut mock_ur = MockUserRepositoryImpl::new();
        let mut mock_ar = MockAuditRepositoryImpl::new();

        let user_id = Uuid::new_v4();
        let test_user = User {
            id: user_id,
            username: "testuser".to_string(),
            password_hash: "password".to_string(),
            email: "test@example.com".to_string(),
            created_at: Utc::now(),
        };

        let user_clone = test_user.clone();
        mock_ur
            .expect_get_by_username()
            .with(mockall::predicate::eq("testuser"))
            .returning(move |_| Ok(Some(user_clone.clone())));

        mock_ar
            .expect_log()
            .withf(move |entry| entry.action == "LOGIN_FAILURE" && entry.user_id == user_id)
            .returning(|_| Ok(()));

        let use_case = AuthUseCase::new(Arc::new(mock_ur), Arc::new(mock_ar));
        let result = use_case.login("testuser", "wrong_password").await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Invalid password");
    }
}
