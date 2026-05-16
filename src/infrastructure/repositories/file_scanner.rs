use crate::domain::entities::{Document, DocumentMetadata};
use crate::domain::ports::FileScanner;
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::fs;
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

pub struct LocalFileScanner {
    pvc_name: String,
}

impl LocalFileScanner {
    pub fn new(pvc_name: String) -> Self {
        Self { pvc_name }
    }
}

#[async_trait]
impl FileScanner for LocalFileScanner {
    async fn scan_directory(&self, path: &str) -> Result<Vec<String>> {
        let mut files = Vec::new();
        for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                if let Some(ext) = entry.path().extension() {
                    let ext_str = ext.to_string_lossy().to_lowercase();
                    if ["pdf", "md", "txt", "log"].contains(&ext_str.as_str()) {
                        files.push(entry.path().to_string_lossy().to_string());
                    }
                }
            }
        }
        Ok(files)
    }

    async fn load_document(&self, file_path: &str) -> Result<Document> {
        let path = std::path::Path::new(file_path);
        let content = if path.extension().map(|e| e == "pdf").unwrap_or(false) {
            pdf_extract::extract_text(file_path)
                .map_err(|e| anyhow!("Failed to extract PDF text from {}: {}", file_path, e))?
        } else {
            fs::read_to_string(file_path)?
        };

        let metadata = fs::metadata(file_path)?;
        let last_modified = metadata.modified()?.duration_since(UNIX_EPOCH)?.as_secs() as i64;

        Ok(Document {
            content,
            metadata: DocumentMetadata {
                source_path: file_path.to_string(),
                file_name: path
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string(),
                pvc_name: self.pvc_name.clone(),
                file_size: metadata.len(),
                last_modified,
            },
        })
    }
}
