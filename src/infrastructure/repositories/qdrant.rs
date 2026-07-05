use crate::domain::entities::{DocumentChunk, DocumentMetadata};
use crate::domain::ports::VectorStore;
use anyhow::Result;
use async_trait::async_trait;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, Distance, FieldType, PointStruct, QueryPointsBuilder,
    UpsertPointsBuilder, Value as QdrantValue, VectorParamsBuilder,
};
use qdrant_client::Qdrant;
use std::collections::HashMap;
use tracing::{debug, info};

fn get_str(payload: &HashMap<String, QdrantValue>, key: &str) -> String {
    payload
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default()
}

fn get_i64(payload: &HashMap<String, QdrantValue>, key: &str) -> i64 {
    payload.get(key).and_then(|v| v.as_integer()).unwrap_or(0)
}

fn get_str_list(payload: &HashMap<String, QdrantValue>, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(|v| v.as_list())
        .map(|list| {
            list.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn get_optional_str(payload: &HashMap<String, QdrantValue>, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn get_optional_str_list(payload: &HashMap<String, QdrantValue>, key: &str) -> Option<Vec<String>> {
    payload.get(key).and_then(|v| v.as_list()).map(|list| {
        list.iter()
            .filter_map(|v| v.as_str())
            .map(|s| s.to_string())
            .collect()
    })
}

pub struct QdrantVectorStore {
    client: Qdrant,
}

impl QdrantVectorStore {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Qdrant::from_url(url).build()?;
        Ok(Self { client })
    }

    async fn ensure_collection(&self, collection_name: &str, vector_size: u64) -> Result<()> {
        if !self.client.collection_exists(collection_name).await? {
            info!("Creating Qdrant collection: {}", collection_name);
            self.client
                .create_collection(
                    CreateCollectionBuilder::new(collection_name)
                        .vectors_config(VectorParamsBuilder::new(vector_size, Distance::Cosine))
                        .shard_number(1)
                        .replication_factor(1)
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

                if let Some(ref tags) = chunk.metadata.inferred_tags {
                    payload.insert("inferred_tags".to_string(), tags.clone().into());
                }
                if let Some(ref summary) = chunk.metadata.document_summary {
                    payload.insert("document_summary".to_string(), summary.clone().into());
                }
                if let Some(ref entities) = chunk.metadata.detected_entities {
                    payload.insert("detected_entities".to_string(), entities.clone().into());
                }

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
        _query_text: &str,
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

        // Vector Search candidate retrieval for cross-encoder reranking
        let request = QueryPointsBuilder::new(collection_name)
            .query(query_vector)
            .limit(limit as u64)
            .with_payload(true)
            .build();

        let search_result = self.client.query(request).await?;

        let mut chunks = Vec::new();
        for point in search_result.result {
            let payload = point.payload;

            let metadata = DocumentMetadata {
                source_path: get_str(&payload, "source_path"),
                file_name: get_str(&payload, "file_name"),
                pvc_name: get_str(&payload, "pvc_name"),
                file_size: get_i64(&payload, "file_size") as u64,
                last_modified: get_i64(&payload, "last_modified"),
                creation_date: get_i64(&payload, "creation_date"),
                file_hash: get_str(&payload, "file_hash"),
                folder_tags: get_str_list(&payload, "folder_tags"),
                inferred_tags: get_optional_str_list(&payload, "inferred_tags"),
                document_summary: get_optional_str(&payload, "document_summary"),
                detected_entities: get_optional_str_list(&payload, "detected_entities"),
            };

            chunks.push(DocumentChunk {
                content: get_str(&payload, "content"),
                metadata,
                embedding: None,
                score: Some(point.score),
            });
        }

        Ok(chunks)
    }

    async fn health_check(&self) -> Result<()> {
        // Check health of Qdrant client
        self.client.health_check().await?;
        Ok(())
    }

    async fn get_collection_info(&self, collection_name: &str) -> Result<serde_json::Value> {
        let exists = self.client.collection_exists(collection_name).await?;
        if !exists {
            return Ok(serde_json::json!({
                "exists": false,
                "collection_name": collection_name,
            }));
        }

        let response = self.client.collection_info(collection_name).await?;
        let info = response.result.unwrap_or_default();

        let status_str = info.status().as_str_name().to_string();

        Ok(serde_json::json!({
            "exists": true,
            "collection_name": collection_name,
            "points_count": info.points_count(),
            "indexed_vectors_count": info.indexed_vectors_count(),
            "segments_count": info.segments_count,
            "status": status_str,
        }))
    }
}
