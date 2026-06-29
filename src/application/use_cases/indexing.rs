use crate::domain::entities::{DocumentChunk, PipelineRun};
use crate::domain::ports::{EmbeddingService, FileScanner, VectorStore, PipelineRepository};
use anyhow::Result;
use std::sync::Arc;
use tracing::{error, info};
use futures::StreamExt;
use uuid::Uuid;
use chrono::Utc;

#[derive(Clone)]
pub struct IndexingUseCase {
    file_scanner: Arc<dyn FileScanner>,
    embedding_service: Arc<dyn EmbeddingService>,
    vector_store: Arc<dyn VectorStore>,
    pipeline_repo: Arc<dyn PipelineRepository>,
}

impl IndexingUseCase {
    pub fn new(
        file_scanner: Arc<dyn FileScanner>,
        embedding_service: Arc<dyn EmbeddingService>,
        vector_store: Arc<dyn VectorStore>,
        pipeline_repo: Arc<dyn PipelineRepository>,
    ) -> Self {
        Self {
            file_scanner,
            embedding_service,
            vector_store,
            pipeline_repo,
        }
    }

    pub async fn execute(&self, path: &str, collection_name: &str) -> Result<()> {
        info!("Starting indexing process for path: {}", path);

        let file_paths = self.file_scanner.scan_directory(path).await?;
        info!("Found {} files to process", file_paths.len());

        let concurrency = 8; // Process 8 files in parallel
        let collection_name = collection_name.to_string();

        futures::stream::iter(file_paths)
            .map(|file_path| {
                let this = self.clone();
                let col_name = collection_name.clone();
                async move {
                    match this.process_file(&file_path, &col_name).await {
                        Ok(_) => info!("Successfully indexed: {}", file_path),
                        Err(e) => error!("Failed to index {}: {}", file_path, e),
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<()>>()
            .await;

        Ok(())
    }

    pub async fn process_file(&self, file_path: &str, collection_name: &str) -> Result<()> {
        self.process_file_with_params(file_path, collection_name, 1000, 0, None).await
    }

    pub async fn process_file_with_params(
        &self,
        file_path: &str,
        collection_name: &str,
        chunk_size: usize,
        chunk_overlap: usize,
        custom_text: Option<String>,
    ) -> Result<()> {
        let file_name = std::path::Path::new(file_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();

        let metadata = std::fs::metadata(file_path);
        let file_size = metadata.map(|m| m.len() as i64).unwrap_or(0);

        // Check if a run already exists for this file
        let run_id = match self.pipeline_repo.get_run_by_file_path(file_path).await {
            Ok(Some(existing)) => existing.id,
            _ => Uuid::new_v4(),
        };

        let parameters = serde_json::json!({
            "chunk_size": chunk_size,
            "chunk_overlap": chunk_overlap,
        });

        let mut run = PipelineRun {
            id: run_id,
            file_path: file_path.to_string(),
            file_name,
            file_size,
            status: "IN_PROGRESS".to_string(),
            current_step: "PARSING".to_string(),
            ocr_status: "NONE".to_string(),
            error_message: None,
            chunks_count: None,
            extracted_text: None,
            chunks: None,
            started_at: Utc::now(),
            completed_at: None,
            parameters: Some(parameters.clone()),
        };

        // Create or update starting run
        if let Ok(Some(_)) = self.pipeline_repo.get_run(run_id).await {
            let _ = self.pipeline_repo.update_run(run.clone()).await;
        } else {
            let _ = self.pipeline_repo.create_run(run.clone()).await;
        }

        match self.process_file_internal(file_path, collection_name, chunk_size, chunk_overlap, custom_text, &mut run).await {
            Ok(_) => {
                run.status = "COMPLETED".to_string();
                run.current_step = "COMPLETE".to_string();
                run.completed_at = Some(Utc::now());
                let _ = self.pipeline_repo.update_run(run).await;
                Ok(())
            }
            Err(e) => {
                let err_str = e.to_string();
                error!("Error during indexing for {}: {}", file_path, err_str);
                run.status = "FAILED".to_string();
                run.error_message = Some(err_str);
                run.completed_at = Some(Utc::now());
                let _ = self.pipeline_repo.update_run(run).await;
                Err(e)
            }
        }
    }

    async fn process_file_internal(
        &self,
        file_path: &str,
        collection_name: &str,
        chunk_size: usize,
        chunk_overlap: usize,
        custom_text: Option<String>,
        run: &mut PipelineRun,
    ) -> Result<()> {
        // Step 1: Content Extraction / OCR
        let doc = if let Some(text) = custom_text {
            // If custom text was provided for correction
            run.ocr_status = "NONE".to_string();
            let mut temp_doc = self.file_scanner.load_document(file_path).await?;
            temp_doc.content = text;
            temp_doc
        } else {
            let doc = self.file_scanner.load_document(file_path).await?;
            if file_path.to_lowercase().ends_with(".pdf") {
                if doc.content.contains("[OCR PENDING]")
                    || doc.content.contains("[ERROR]")
                    || doc.content.contains("[TIMEOUT]")
                {
                    run.ocr_status = "FAILED".to_string();
                } else {
                    run.ocr_status = "SUCCESS".to_string();
                }
            }
            doc
        };

        run.extracted_text = Some(doc.content.clone());
        run.current_step = "CHUNKING".to_string();
        let _ = self.pipeline_repo.update_run(run.clone()).await;

        // Step 2: Chunking logic
        let content_chars: Vec<char> = doc.content.chars().collect();
        let mut chunks_content = Vec::new();
        
        let mut start = 0;
        while start < content_chars.len() {
            let end = std::cmp::min(start + chunk_size, content_chars.len());
            let chunk_str: String = content_chars[start..end].iter().collect();
            chunks_content.push(chunk_str);
            if end == content_chars.len() {
                break;
            }
            // Advance by chunk_size - chunk_overlap
            let step = if chunk_size > chunk_overlap {
                chunk_size - chunk_overlap
            } else {
                1
            };
            start += step;
        }

        run.chunks_count = Some(chunks_content.len() as i32);
        run.chunks = Some(serde_json::to_value(&chunks_content).unwrap_or(serde_json::Value::Null));
        run.current_step = "EMBEDDING".to_string();
        let _ = self.pipeline_repo.update_run(run.clone()).await;

        // Step 3: Embeddings
        let mut embeddings = Vec::new();
        for batch in chunks_content.chunks(32) {
            let batch_embeddings = self
                .embedding_service
                .generate_embeddings(batch.to_vec())
                .await?;
            embeddings.extend(batch_embeddings);
        }

        run.current_step = "STORING".to_string();
        let _ = self.pipeline_repo.update_run(run.clone()).await;

        // Step 4: Save to vector database
        let doc_chunks: Vec<DocumentChunk> = chunks_content
            .into_iter()
            .zip(embeddings)
            .map(|(content, embedding)| DocumentChunk {
                content,
                metadata: doc.metadata.clone(),
                embedding: Some(embedding),
                score: None,
            })
            .collect();

        self.vector_store
            .save_chunks(doc_chunks, collection_name)
            .await?;

        Ok(())
    }
}
