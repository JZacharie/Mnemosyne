use crate::domain::ports::FileScanner;
use crate::domain::entities::{Document, DocumentMetadata};
use async_trait::async_trait;
use anyhow::{Result, anyhow};
use walkdir::WalkDir;
use std::fs;
use std::time::UNIX_EPOCH;

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
            // In a real app, we would use a PDF library like `pdf-extract` or `lopdf`
            // For now, let's assume we can read it as text or return an error if not implemented
            return Err(anyhow!("PDF loading not yet implemented in Rust version"));
        } else {
            fs::read_to_string(file_path)?
        };

        let metadata = fs::metadata(file_path)?;
        let last_modified = metadata.modified()?
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64;

        Ok(Document {
            content,
            metadata: DocumentMetadata {
                source_path: file_path.to_string(),
                file_name: path.file_name().unwrap_or_default().to_string_lossy().to_string(),
                pvc_name: self.pvc_name.clone(),
                file_size: metadata.len(),
                last_modified,
            },
        })
    }
}
