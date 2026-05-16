use crate::domain::entities::{DocumentChunk, DocumentMetadata};
use crate::domain::ports::VectorStore;
use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::qdrant::{
    Condition, CreateCollectionBuilder, Distance, FieldType, Filter, PointStruct,
    QueryPointsBuilder, UpsertPointsBuilder, Value as QdrantValue, VectorParamsBuilder,
};
use qdrant_client::Qdrant;
use std::collections::HashMap;
use tracing::{debug, info};

pub struct QdrantVectorStore {
    client: Qdrant,
}

impl QdrantVectorStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Qdrant::from_url(url)
            .build()?;
        Ok(Self { client })
    }

    async fn ensure_collection(&self, collection_name: &str, vector_size: u64) -> Result<()> {
        if !self.client.collection_exists(collection_name).await? {
            info!("Creating Qdrant collection: {}", collection_name);
            self.client
                .create_collection(
                    CreateCollectionBuilder::new(collection_name)
                        .vectors_config(VectorParamsBuilder::new(vector_size, Distance::Cosine))
                        .shard_number(3)
                        .replication_factor(2)
                        .on_disk_payload(true),
                )
                .await?;

            // Create full-text payload index for hybrid search
            self.client
                .create_field_index(
                    qdrant_client::qdrant::CreateFieldIndexCollectionBuilder::new(
                        collection_name,
                        "content",
                        FieldType::Text,
                    ),
                )
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl VectorStore for QdrantVectorStore {
    async fn save_chunks(&self, chunks: Vec<DocumentChunk>, collection_name: &str) -> Result<()> {
        if chunks.is_empty() {
            return Ok(());
        }

        let vector_size = chunks[0]
            .embedding
            .as_ref()
            .map(|v| v.len() as u64)
            .unwrap_or(1024);
        self.ensure_collection(collection_name, vector_size).await?;

        let mut points = Vec::new();
        for chunk in chunks {
            if let Some(embedding) = chunk.embedding {
                let mut payload: HashMap<String, QdrantValue> = HashMap::new();
                payload.insert("content".to_string(), chunk.content.into());
                payload.insert("source_path".to_string(), chunk.metadata.source_path.into());
                payload.insert("file_name".to_string(), chunk.metadata.file_name.into());
                payload.insert("pvc_name".to_string(), chunk.metadata.pvc_name.into());
                payload.insert(
                    "file_size".to_string(),
                    (chunk.metadata.file_size as i64).into(),
                );
                payload.insert(
                    "last_modified".to_string(),
                    chunk.metadata.last_modified.into(),
                );
                payload.insert(
                    "creation_date".to_string(),
                    chunk.metadata.creation_date.into(),
                );
                payload.insert("file_hash".to_string(), chunk.metadata.file_hash.into());
                payload.insert("folder_tags".to_string(), chunk.metadata.folder_tags.into());

                points.push(PointStruct::new(
                    uuid::Uuid::new_v4().to_string(),
                    embedding,
                    payload,
                ));
            }
        }

        if !points.is_empty() {
            self.client
                .upsert_points(UpsertPointsBuilder::new(collection_name, points))
                .await?;
        }

        Ok(())
    }

    async fn search(
        &self,
        query_text: &str,
        query_vector: Vec<f32>,
        limit: usize,
        collection_name: &str,
    ) -> Result<Vec<DocumentChunk>> {
        let vector_size = query_vector.len() as u64;
        self.ensure_collection(collection_name, vector_size).await?;

        debug!(
            "Hybrid search on collection {} with limit {}",
            collection_name, limit
        );

        // Hybrid Search: Vector Search narrowed by Full-Text filtering
        let request = QueryPointsBuilder::new(collection_name)
            .query(query_vector)
            .filter(Filter::must(vec![Condition::matches_text("content", query_text)]))
            .limit(limit as u64)
            .with_payload(true)
            .build();

        let search_result = self.client.query(request).await?;

        let mut chunks = Vec::new();
        for point in search_result.result {
            let payload = point.payload;

            let content = payload
                .get("content")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();

            let metadata = DocumentMetadata {
                source_path: payload
                    .get("source_path")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                file_name: payload
                    .get("file_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                pvc_name: payload
                    .get("pvc_name")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                file_size: payload
                    .get("file_size")
                    .and_then(|v| v.as_integer())
                    .unwrap_or(0) as u64,
                last_modified: payload
                    .get("last_modified")
                    .and_then(|v| v.as_integer())
                    .unwrap_or(0),
                creation_date: payload
                    .get("creation_date")
                    .and_then(|v| v.as_integer())
                    .unwrap_or(0),
                file_hash: payload
                    .get("file_hash")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default(),
                folder_tags: payload
                    .get("folder_tags")
                    .and_then(|v| v.as_list())
                    .map(|list| {
                        list.iter()
                            .filter_map(|v| v.as_str())
                            .map(|s| s.to_string())
                            .collect()
                    })
                    .unwrap_or_default(),
            };

            chunks.push(DocumentChunk {
                content,
                metadata,
                embedding: None,
                score: Some(point.score),
            });
        }

        Ok(chunks)
    }
}
