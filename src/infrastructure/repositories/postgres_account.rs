use crate::domain::entities::{AuditLog, User, PipelineRun};
use crate::domain::ports::{AuditRepository, UserRepository, PipelineRepository};
use anyhow::Result;
use async_trait::async_trait;
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
            "SELECT id, username, password_hash, email, created_at FROM users WHERE username = $1",
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

        let logs = rows
            .into_iter()
            .map(|row| AuditLog {
                id: row.get("id"),
                user_id: row.get("user_id"),
                action: row.get("action"),
                resource: row.get("resource"),
                timestamp: row.get("timestamp"),
                metadata: row.get("metadata"),
            })
            .collect();

        Ok(logs)
    }
}

#[async_trait]
impl PipelineRepository for PostgresAccountRepository {
    async fn create_run(&self, run: PipelineRun) -> Result<()> {
        sqlx::query(
            "INSERT INTO pipeline_runs (id, file_path, file_name, file_size, status, current_step, ocr_status, error_message, chunks_count, extracted_text, chunks, started_at, completed_at, parameters) 
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)"
        )
        .bind(run.id)
        .bind(&run.file_path)
        .bind(&run.file_name)
        .bind(run.file_size)
        .bind(&run.status)
        .bind(&run.current_step)
        .bind(&run.ocr_status)
        .bind(run.error_message)
        .bind(run.chunks_count)
        .bind(run.extracted_text)
        .bind(run.chunks)
        .bind(run.started_at)
        .bind(run.completed_at)
        .bind(run.parameters)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn update_run(&self, run: PipelineRun) -> Result<()> {
        sqlx::query(
            "UPDATE pipeline_runs 
             SET status = $1, current_step = $2, ocr_status = $3, error_message = $4, chunks_count = $5, extracted_text = $6, chunks = $7, completed_at = $8, parameters = $9
             WHERE id = $10"
        )
        .bind(&run.status)
        .bind(&run.current_step)
        .bind(&run.ocr_status)
        .bind(run.error_message)
        .bind(run.chunks_count)
        .bind(run.extracted_text)
        .bind(run.chunks)
        .bind(run.completed_at)
        .bind(run.parameters)
        .bind(run.id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_run(&self, id: Uuid) -> Result<Option<PipelineRun>> {
        let row = sqlx::query(
            "SELECT id, file_path, file_name, file_size, status, current_step, ocr_status, error_message, chunks_count, extracted_text, chunks, started_at, completed_at, parameters 
             FROM pipeline_runs WHERE id = $1"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(PipelineRun {
                id: row.get("id"),
                file_path: row.get("file_path"),
                file_name: row.get("file_name"),
                file_size: row.get("file_size"),
                status: row.get("status"),
                current_step: row.get("current_step"),
                ocr_status: row.get("ocr_status"),
                error_message: row.get("error_message"),
                chunks_count: row.get("chunks_count"),
                extracted_text: row.get("extracted_text"),
                chunks: row.get("chunks"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                parameters: row.get("parameters"),
            })),
            None => Ok(None),
        }
    }

    async fn get_run_by_file_path(&self, file_path: &str) -> Result<Option<PipelineRun>> {
        let row = sqlx::query(
            "SELECT id, file_path, file_name, file_size, status, current_step, ocr_status, error_message, chunks_count, extracted_text, chunks, started_at, completed_at, parameters 
             FROM pipeline_runs WHERE file_path = $1"
        )
        .bind(file_path)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(PipelineRun {
                id: row.get("id"),
                file_path: row.get("file_path"),
                file_name: row.get("file_name"),
                file_size: row.get("file_size"),
                status: row.get("status"),
                current_step: row.get("current_step"),
                ocr_status: row.get("ocr_status"),
                error_message: row.get("error_message"),
                chunks_count: row.get("chunks_count"),
                extracted_text: row.get("extracted_text"),
                chunks: row.get("chunks"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                parameters: row.get("parameters"),
            })),
            None => Ok(None),
        }
    }

    async fn list_runs(&self) -> Result<Vec<PipelineRun>> {
        let rows = sqlx::query(
            "SELECT id, file_path, file_name, file_size, status, current_step, ocr_status, error_message, chunks_count, extracted_text, chunks, started_at, completed_at, parameters 
             FROM pipeline_runs ORDER BY started_at DESC"
        )
        .fetch_all(&self.pool)
        .await?;

        let runs = rows
            .into_iter()
            .map(|row| PipelineRun {
                id: row.get("id"),
                file_path: row.get("file_path"),
                file_name: row.get("file_name"),
                file_size: row.get("file_size"),
                status: row.get("status"),
                current_step: row.get("current_step"),
                ocr_status: row.get("ocr_status"),
                error_message: row.get("error_message"),
                chunks_count: row.get("chunks_count"),
                extracted_text: row.get("extracted_text"),
                chunks: row.get("chunks"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                parameters: row.get("parameters"),
            })
            .collect();

        Ok(runs)
    }
}
