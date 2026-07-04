use crate::domain::entities::{DocumentChunk, PipelineRun};
use crate::domain::ports::{
    EmbeddingService, FileScanner, LLMService, PipelineRepository, VectorStore,
};
use anyhow::Result;
use chrono::Utc;
use futures::StreamExt;
use serde::Deserialize;
use std::sync::Arc;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Clone)]
pub struct IndexingUseCase {
    file_scanner: Arc<dyn FileScanner>,
    embedding_service: Arc<dyn EmbeddingService>,
    vector_store: Arc<dyn VectorStore>,
    pipeline_repo: Arc<dyn PipelineRepository>,
    llm_service: Arc<dyn LLMService>,
}

impl IndexingUseCase {
    pub fn new(
        file_scanner: Arc<dyn FileScanner>,
        embedding_service: Arc<dyn EmbeddingService>,
        vector_store: Arc<dyn VectorStore>,
        pipeline_repo: Arc<dyn PipelineRepository>,
        llm_service: Arc<dyn LLMService>,
    ) -> Self {
        Self {
            file_scanner,
            embedding_service,
            vector_store,
            pipeline_repo,
            llm_service,
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
        self.process_file_with_params(file_path, collection_name, 1000, 0, None)
            .await
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

        match self
            .process_file_internal(
                file_path,
                collection_name,
                chunk_size,
                chunk_overlap,
                custom_text,
                &mut run,
            )
            .await
        {
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

        let mut doc = doc;
        let mut doc_context = String::new();

        if !doc.content.trim().is_empty() {
            let truncated_content = if doc.content.len() > 10000 {
                &doc.content[..10000]
            } else {
                &doc.content
            };

            // Call LLM for document summary (context wrapper)
            let summary_system = "You are a precise context summarizer.";
            let summary_user = format!(
                "Provide a brief 1-2 sentence summary of this document. It will prefix search chunks to provide context.\n\nDocument text:\n{}",
                truncated_content
            );
            if let Ok(sum) = self
                .llm_service
                .generate_text(summary_system, &summary_user)
                .await
            {
                doc_context = sum;
            }

            // Call LLM for structured metadata JSON
            let meta_system = "You are a metadata extractor. Output ONLY raw JSON matching the requested schema. No markdown formatting, no prefix/suffix.";
            let meta_user = format!(
                "Extract metadata from this document as JSON with these keys:\n- inferred_tags (list of strings for topics/categories)\n- document_summary (string summary, max 3 sentences)\n- detected_entities (list of key entities mentioned)\n\nDocument text:\n{}",
                truncated_content
            );
            if let Ok(meta_raw) = self
                .llm_service
                .generate_text(meta_system, &meta_user)
                .await
            {
                if let Some(parsed) = parse_llm_metadata(&meta_raw) {
                    doc.metadata.inferred_tags = parsed.inferred_tags;
                    doc.metadata.document_summary = parsed.document_summary;
                    doc.metadata.detected_entities = parsed.detected_entities;
                }
            }
        }

        run.extracted_text = Some(doc.content.clone());
        run.current_step = "CHUNKING".to_string();
        let _ = self.pipeline_repo.update_run(run.clone()).await;

        // Step 2: Chunking logic
        let chunks_content = split_text(&doc.content, chunk_size, chunk_overlap);

        // Prepend context summary to each chunk
        let enriched_chunks = enrich_chunks_with_context(
            chunks_content.clone(),
            &doc.metadata.file_name,
            &doc_context,
        );

        run.chunks_count = Some(chunks_content.len() as i32);
        run.chunks = Some(serde_json::to_value(&chunks_content).unwrap_or(serde_json::Value::Null));
        run.current_step = "EMBEDDING".to_string();
        let _ = self.pipeline_repo.update_run(run.clone()).await;

        // Step 3: Embeddings
        let mut embeddings = Vec::new();
        for batch in enriched_chunks.chunks(32) {
            let batch_embeddings = self
                .embedding_service
                .generate_embeddings(batch.to_vec())
                .await?;
            embeddings.extend(batch_embeddings);
        }

        run.current_step = "STORING".to_string();
        let _ = self.pipeline_repo.update_run(run.clone()).await;

        // Step 4: Save to vector database
        let doc_chunks: Vec<DocumentChunk> = enriched_chunks
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

#[derive(Deserialize)]
struct LlmMetadata {
    inferred_tags: Option<Vec<String>>,
    document_summary: Option<String>,
    detected_entities: Option<Vec<String>>,
}

fn enrich_chunks_with_context(chunks: Vec<String>, file_name: &str, context: &str) -> Vec<String> {
    if context.is_empty() {
        return chunks;
    }
    chunks
        .iter()
        .map(|chunk| {
            format!(
                "Document: {}\nContext: {}\nChunk Content: {}",
                file_name, context, chunk
            )
        })
        .collect()
}

fn parse_llm_metadata(raw: &str) -> Option<LlmMetadata> {
    serde_json::from_str::<LlmMetadata>(raw).ok().or_else(|| {
        let clean = raw.replace("```json", "").replace("```", "");
        serde_json::from_str::<LlmMetadata>(&clean).ok()
    })
}

fn split_text(text: &str, chunk_size: usize, chunk_overlap: usize) -> Vec<String> {
    let separators = vec!["\n\n", "\n", " ", ""];
    let mut final_chunks = Vec::new();

    fn split_recursive(
        text: &str,
        separators: &[&str],
        chunk_size: usize,
        chunk_overlap: usize,
        chunks: &mut Vec<String>,
    ) {
        if text.chars().count() <= chunk_size {
            if !text.trim().is_empty() {
                chunks.push(text.to_string());
            }
            return;
        }

        let (separator, remaining_separators) =
            if let Some((&first, rest)) = separators.split_first() {
                (first, rest)
            } else {
                // Fallback to hard character splitting
                let chars: Vec<char> = text.chars().collect();
                let mut start = 0;
                while start < chars.len() {
                    let end = std::cmp::min(start + chunk_size, chars.len());
                    let s: String = chars[start..end].iter().collect();
                    if !s.trim().is_empty() {
                        chunks.push(s);
                    }
                    if end == chars.len() {
                        break;
                    }
                    let step = if chunk_size > chunk_overlap {
                        chunk_size - chunk_overlap
                    } else {
                        1
                    };
                    start += step;
                }
                return;
            };

        // Split by the current separator
        let splits: Vec<String> = if separator.is_empty() {
            text.chars().map(|c| c.to_string()).collect()
        } else {
            text.split(separator).map(|s| s.to_string()).collect()
        };

        let mut current_doc = Vec::new();
        let mut total_len = 0;

        for split in splits {
            let split_len = split.chars().count();
            let sep_len = if total_len > 0 {
                separator.chars().count()
            } else {
                0
            };

            if total_len + split_len + sep_len <= chunk_size {
                current_doc.push(split);
                total_len += split_len + sep_len;
            } else {
                if !current_doc.is_empty() {
                    let chunk_str = current_doc.join(separator);
                    chunks.push(chunk_str);

                    // Rebuild current_doc with overlap elements
                    let mut overlap_doc = Vec::new();
                    let mut overlap_len = 0;
                    for item in current_doc.iter().rev() {
                        let item_len = item.chars().count();
                        let o_sep_len = if overlap_len > 0 {
                            separator.chars().count()
                        } else {
                            0
                        };
                        if overlap_len + item_len + o_sep_len <= chunk_overlap {
                            overlap_doc.insert(0, item.clone());
                            overlap_len += item_len + o_sep_len;
                        } else {
                            break;
                        }
                    }
                    current_doc = overlap_doc;
                    total_len = overlap_len;
                }

                if split_len > chunk_size {
                    split_recursive(
                        &split,
                        remaining_separators,
                        chunk_size,
                        chunk_overlap,
                        chunks,
                    );
                } else {
                    let sep_len = if total_len > 0 {
                        separator.chars().count()
                    } else {
                        0
                    };
                    current_doc.push(split);
                    total_len += split_len + sep_len;
                }
            }
        }

        if !current_doc.is_empty() {
            let chunk_str = current_doc.join(separator);
            if !chunk_str.trim().is_empty() {
                chunks.push(chunk_str);
            }
        }
    }

    split_recursive(
        text,
        &separators,
        chunk_size,
        chunk_overlap,
        &mut final_chunks,
    );
    final_chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- split_text tests ---

    #[test]
    fn test_split_text_simple() {
        let text = "Hello world from Mnemosyne RAG";
        let chunks = split_text(text, 10, 2);
        assert!(!chunks.is_empty());
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 10);
        }
    }

    #[test]
    fn test_split_text_paragraphs() {
        let text = "Paragraph 1\n\nParagraph 2 is longer and will split\n\nParagraph 3";
        let chunks = split_text(text, 20, 5);
        assert!(chunks.len() >= 3);
        assert_eq!(chunks[0], "Paragraph 1");
    }

    #[test]
    fn test_split_text_respects_sentence_boundary() {
        let text = "Sentence one. Sentence two. Sentence three.";
        let chunks = split_text(text, 15, 0);
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 15);
        }
    }

    #[test]
    fn test_split_text_empty() {
        let chunks = split_text("", 10, 0);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_text_whitespace_only() {
        let chunks = split_text("   \n  \n   ", 10, 0);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_split_text_small_content() {
        let text = "Small";
        let chunks = split_text(text, 100, 0);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], "Small");
    }

    #[test]
    fn test_split_text_with_overlap() {
        let text = "aaa bbb ccc ddd eee";
        let chunks = split_text(text, 8, 4);
        assert!(chunks.len() >= 3);
        if chunks.len() >= 2 {
            let overlap = &chunks[1];
            assert!(!overlap.is_empty());
        }
    }

    #[test]
    fn test_split_text_no_overlap_exact() {
        let text = "aaaa bbbb cccc";
        let chunks = split_text(text, 5, 0);
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 5);
        }
    }

    #[test]
    fn test_split_text_unicode() {
        let text = "éèêë àâäùûü öôœ";
        let chunks = split_text(text, 6, 0);
        assert!(!chunks.is_empty());
        for chunk in &chunks {
            assert!(chunk.chars().count() <= 6);
        }
    }

    #[test]
    fn test_split_text_large_chunk_exact() {
        let text = "A".repeat(100);
        let chunks = split_text(&text, 50, 0);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 50);
        assert_eq!(chunks[1].len(), 50);
    }

    // --- enrich_chunks_with_context tests ---

    #[test]
    fn test_enrich_chunks_with_context() {
        let chunks = vec!["chunk1".to_string(), "chunk2".to_string()];
        let result = enrich_chunks_with_context(chunks, "doc.md", "summary text");
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            "Document: doc.md\nContext: summary text\nChunk Content: chunk1"
        );
        assert_eq!(
            result[1],
            "Document: doc.md\nContext: summary text\nChunk Content: chunk2"
        );
    }

    #[test]
    fn test_enrich_chunks_empty_context() {
        let chunks = vec!["chunk1".to_string()];
        let result = enrich_chunks_with_context(chunks.clone(), "doc.md", "");
        assert_eq!(result, chunks);
    }

    #[test]
    fn test_enrich_chunks_empty_chunks() {
        let result = enrich_chunks_with_context(vec![], "doc.md", "context");
        assert!(result.is_empty());
    }

    #[test]
    fn test_enrich_chunks_special_chars() {
        let chunks = vec!["line1\nline2".to_string()];
        let result =
            enrich_chunks_with_context(chunks, "fichier spécial.md", "contexte avec émoji");
        assert_eq!(
            result[0],
            "Document: fichier spécial.md\nContext: contexte avec émoji\nChunk Content: line1\nline2"
        );
    }

    // --- parse_llm_metadata tests ---

    #[test]
    fn test_parse_llm_metadata_valid() {
        let raw = r#"{"inferred_tags":["rust","ai"],"document_summary":"A summary.","detected_entities":["entity1"]}"#;
        let parsed = parse_llm_metadata(raw).unwrap();
        assert_eq!(
            parsed.inferred_tags,
            Some(vec!["rust".to_string(), "ai".to_string()])
        );
        assert_eq!(parsed.document_summary, Some("A summary.".to_string()));
        assert_eq!(parsed.detected_entities, Some(vec!["entity1".to_string()]));
    }

    #[test]
    fn test_parse_llm_metadata_markdown_fenced() {
        let raw = "```json\n{\"inferred_tags\":[\"tag1\"],\"document_summary\":\"Sum\",\"detected_entities\":[]}\n```";
        let parsed = parse_llm_metadata(raw).unwrap();
        assert_eq!(parsed.inferred_tags, Some(vec!["tag1".to_string()]));
        assert_eq!(parsed.document_summary, Some("Sum".to_string()));
    }

    #[test]
    fn test_parse_llm_metadata_partial_fields() {
        let raw = r#"{"inferred_tags":["test"]}"#;
        let parsed = parse_llm_metadata(raw).unwrap();
        assert_eq!(parsed.inferred_tags, Some(vec!["test".to_string()]));
        assert!(parsed.document_summary.is_none());
        assert!(parsed.detected_entities.is_none());
    }

    #[test]
    fn test_parse_llm_metadata_empty_object() {
        let parsed = parse_llm_metadata("{}").unwrap();
        assert!(parsed.inferred_tags.is_none());
        assert!(parsed.document_summary.is_none());
        assert!(parsed.detected_entities.is_none());
    }

    #[test]
    fn test_parse_llm_metadata_invalid_json() {
        let parsed = parse_llm_metadata("not json at all");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_llm_metadata_empty_string() {
        let parsed = parse_llm_metadata("");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_parse_llm_metadata_fallback_plain_fence() {
        let raw = "```\n{\"inferred_tags\":[\"plain\"],\"document_summary\":\"No lang hint\"}\n```";
        let parsed = parse_llm_metadata(raw).unwrap();
        assert_eq!(parsed.inferred_tags, Some(vec!["plain".to_string()]));
        assert_eq!(parsed.document_summary, Some("No lang hint".to_string()));
    }
}
