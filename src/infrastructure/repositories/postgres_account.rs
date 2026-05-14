use crate::domain::ports::{UserRepository, AuditRepository};
use crate::domain::entities::{User, AuditLog};
use async_trait::async_trait;
use anyhow::Result;
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub struct PostgresAccountRepository {
    pool: PgPool,
}

impl PostgresAccountRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserRepository for PostgresAccountRepository {
    async fn get_by_username(&self, username: &str) -> Result<Option<User>> {
        let row = sqlx::query(
            "SELECT id, username, password_hash, email, created_at FROM users WHERE username = $1"
        )
        .bind(username)
        .fetch_optional(&self.pool)
        .await?;
        
        match row {
            Some(row) => Ok(Some(User {
                id: row.get("id"),
                username: row.get("username"),
                password_hash: row.get("password_hash"),
                email: row.get("email"),
                created_at: row.get("created_at"),
            })),
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    async fn create(&self, user: User) -> Result<User> {
        sqlx::query(
            "INSERT INTO users (id, username, password_hash, email, created_at) VALUES ($1, $2, $3, $4, $5)"
        )
        .bind(user.id)
        .bind(&user.username)
        .bind(&user.password_hash)
        .bind(&user.email)
        .bind(user.created_at)
        .execute(&self.pool)
        .await?;
        
        Ok(user)
    }
}

#[async_trait]
impl AuditRepository for PostgresAccountRepository {
    async fn log(&self, entry: AuditLog) -> Result<()> {
        sqlx::query(
            "INSERT INTO audit_logs (id, user_id, action, resource, timestamp, metadata) VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(entry.id)
        .bind(entry.user_id)
        .bind(&entry.action)
        .bind(&entry.resource)
        .bind(entry.timestamp)
        .bind(entry.metadata)
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }

    #[allow(dead_code)]
    async fn get_by_user(&self, user_id: Uuid) -> Result<Vec<AuditLog>> {
        let rows = sqlx::query(
            "SELECT id, user_id, action, resource, timestamp, metadata FROM audit_logs WHERE user_id = $1 ORDER BY timestamp DESC"
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        
        let logs = rows.into_iter().map(|row| AuditLog {
            id: row.get("id"),
            user_id: row.get("user_id"),
            action: row.get("action"),
            resource: row.get("resource"),
            timestamp: row.get("timestamp"),
            metadata: row.get("metadata"),
        }).collect();
        
        Ok(logs)
    }
}
