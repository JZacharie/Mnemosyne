use crate::domain::entities::DocumentChunk;
use crate::domain::ports::VectorStore;
use anyhow::Result;
use async_trait::async_trait;
use sqlx::PgPool;
use tracing::info;

pub struct PostgresVectorStore {
    pool: PgPool,
}

impl PostgresVectorStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl VectorStore for PostgresVectorStore {
    async fn save_chunks(&self, chunks: Vec<DocumentChunk>, collection_name: &str) -> Result<()> {
        info!(
            "Saving {} chunks to collection {}",
            chunks.len(),
            collection_name
        );

        for chunk in chunks {
            let metadata_json = serde_json::to_value(&chunk.metadata)?;
            let embedding = chunk.embedding.unwrap_or_default();

            // Note: pgvector specific syntax might need raw sql or specific bind handling
            // Here we use standard sqlx::query
            sqlx::query(
                "INSERT INTO langchain_pg_embedding (content, embedding, cmetadata, collection_id) 
                 VALUES ($1, $2, $3, (SELECT uuid FROM langchain_pg_collection WHERE name = $4 LIMIT 1))"
            )
            .bind(&chunk.content)
            .bind(&embedding)
            .bind(&metadata_json)
            .bind(collection_name)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn search(
        &self,
        _query_text: &str,
        _query_vector: Vec<f32>,
        _limit: usize,
        _collection_name: &str,
    ) -> Result<Vec<DocumentChunk>> {
        // Simple placeholder for now, as we are migrating to Qdrant for performance
        Ok(vec![])
    }
}
