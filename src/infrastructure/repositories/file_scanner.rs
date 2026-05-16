use crate::domain::entities::{Document, DocumentMetadata};
use crate::domain::ports::FileScanner;
use anyhow::Result;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
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

        // 1. Calculate Hash & Read Bytes
        let file_bytes = fs::read(file_path)?;
        let mut hasher = Sha256::new();
        hasher.update(&file_bytes);
        let file_hash = hex::encode(hasher.finalize());

        // 2. Load Content
        let content = if path.extension().map(|e| e == "pdf").unwrap_or(false) {
            match pdf_extract::extract_text(file_path) {
                Ok(text) if !text.trim().is_empty() => text,
                _ => {
                    // TODO: Implement actual OCR with tesseract
                    format!("[OCR PENDING] Image-based PDF detected: {}", file_path)
                }
            }
        } else {
            String::from_utf8_lossy(&file_bytes).to_string()
        };

        // 3. Metadata extraction
        let metadata = fs::metadata(file_path)?;
        let last_modified = metadata.modified()?.duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let creation_date = metadata
            .created()
            .unwrap_or(metadata.modified()?)
            .duration_since(UNIX_EPOCH)?
            .as_secs() as i64;

        // 4. Folder Tags (Hierarchical location)
        let folder_tags: Vec<String> = path
            .parent()
            .map(|p| {
                p.components()
                    .map(|c| c.as_os_str().to_string_lossy().to_string())
                    .filter(|s| !s.is_empty() && s != "/" && s != ".")
                    .collect()
            })
            .unwrap_or_default();

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
                creation_date,
                file_hash,
                folder_tags,
            },
        })
    }
}
