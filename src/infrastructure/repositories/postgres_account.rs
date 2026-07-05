use crate::domain::entities::{AuditLog, PipelineRun, User};
use crate::domain::ports::{AuditRepository, PipelineRepository, UserRepository};
use anyhow::Result;
use async_trait::async_trait;
use serde_json::Value;
use sqlx::{postgres::PgRow, PgPool, Row};
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

fn row_to_run(row: &PgRow) -> PipelineRun {
    PipelineRun {
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

        Ok(row.as_ref().map(row_to_run))
    }

    async fn get_run_by_file_path(&self, file_path: &str) -> Result<Option<PipelineRun>> {
        let row = sqlx::query(
            "SELECT id, file_path, file_name, file_size, status, current_step, ocr_status, error_message, chunks_count, extracted_text, chunks, started_at, completed_at, parameters 
             FROM pipeline_runs WHERE file_path = $1"
        )
        .bind(file_path)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.as_ref().map(row_to_run))
    }

    async fn list_runs(&self) -> Result<Vec<PipelineRun>> {
        let rows = sqlx::query(
            "SELECT id, file_path, file_name, file_size, status, current_step, ocr_status, error_message, chunks_count, extracted_text, chunks, started_at, completed_at, parameters 
             FROM pipeline_runs ORDER BY started_at DESC"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.iter().map(row_to_run).collect())
    }

    async fn get_indexing_stats(&self) -> Result<Value> {
        let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_runs")
            .fetch_one(&self.pool)
            .await?;

        let completed: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'COMPLETED'")
                .fetch_one(&self.pool)
                .await?;

        let failed: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'FAILED'")
                .fetch_one(&self.pool)
                .await?;

        let in_progress: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'IN_PROGRESS'")
                .fetch_one(&self.pool)
                .await?;

        let total_chunks: Option<i64> = sqlx::query_scalar(
            "SELECT SUM(chunks_count) FROM pipeline_runs WHERE status = 'COMPLETED'",
        )
        .fetch_one(&self.pool)
        .await?;

        let total_file_size: Option<i64> = sqlx::query_scalar(
            "SELECT SUM(file_size) FROM pipeline_runs WHERE status = 'COMPLETED'",
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "total_files": total,
            "completed_files": completed,
            "failed_files": failed,
            "in_progress_files": in_progress,
            "total_chunks": total_chunks.unwrap_or(0),
            "total_file_size_bytes": total_file_size.unwrap_or(0),
        }))
    }

    async fn log_search(
        &self,
        id: Uuid,
        query: &str,
        results_count: i32,
        duration_ms: i32,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO search_logs (id, query, results_count, search_duration_ms) VALUES ($1, $2, $3, $4)"
        )
        .bind(id)
        .bind(query)
        .bind(results_count)
        .bind(duration_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_usage_stats(&self) -> Result<Value> {
        let total_searches: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM search_logs")
            .fetch_one(&self.pool)
            .await?;

        let searches_today: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM search_logs WHERE created_at >= CURRENT_DATE")
                .fetch_one(&self.pool)
                .await?;

        let searches_this_week: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM search_logs WHERE created_at >= date_trunc('week', CURRENT_DATE)",
        )
        .fetch_one(&self.pool)
        .await?;

        let avg_duration: Option<f64> =
            sqlx::query_scalar("SELECT AVG(search_duration_ms) FROM search_logs")
                .fetch_one(&self.pool)
                .await?;

        let total_results: Option<i64> =
            sqlx::query_scalar("SELECT SUM(results_count) FROM search_logs")
                .fetch_one(&self.pool)
                .await?;

        let zero_result_searches: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM search_logs WHERE results_count = 0")
                .fetch_one(&self.pool)
                .await?;

        let top_queries: Vec<Value> = sqlx::query(
            "SELECT query, COUNT(*) as freq FROM search_logs GROUP BY query ORDER BY freq DESC LIMIT 10"
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            serde_json::json!({
                "query": row.get::<String, _>("query"),
                "count": row.get::<i64, _>("freq"),
            })
        })
        .collect();

        Ok(serde_json::json!({
            "total_searches": total_searches,
            "searches_today": searches_today,
            "searches_this_week": searches_this_week,
            "average_duration_ms": avg_duration.unwrap_or(0.0),
            "total_results_returned": total_results.unwrap_or(0),
            "zero_result_searches": zero_result_searches,
            "top_queries": top_queries,
        }))
    }
}
