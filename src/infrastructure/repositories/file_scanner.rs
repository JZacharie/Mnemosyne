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
        let path_str = path.to_string();
        tokio::task::spawn_blocking(move || {
            let mut files = Vec::new();
            let walker = WalkDir::new(&path_str).into_iter().filter_entry(|e| {
                let name = e.file_name().to_string_lossy().to_lowercase();
                if e.depth() == 1
                    && [
                        "photos",
                        "backup photo",
                        "photoprism",
                        "3d",
                        "videos de familles",
                        "dcim2",
                        "apps",
                    ]
                    .contains(&name.as_str())
                {
                    return false;
                }
                ![
                    "node_modules",
                    "vendor",
                    "target",
                    ".git",
                    ".cache",
                    "bundle",
                    "tmp",
                    "temp",
                    "dist",
                    ".github",
                    "07_development",
                    "git",
                    ".terraform",
                    "gems",
                    "lib",
                    "bin",
                    "obj",
                    "build",
                    "deps",
                    "packages",
                    "add-ons",
                    "licenses",
                    "test",
                    "tests",
                ]
                .contains(&name.as_str())
            });

            for entry in walker.filter_map(|e| e.ok()) {
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
        })
        .await?
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
            extract_pdf_text(file_path).await
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
                inferred_tags: None,
                document_summary: None,
                detected_entities: None,
            },
        })
    }
}

async fn extract_pdf_text(file_path: &str) -> String {
    let fp = file_path.to_string();
    let handle = tokio::task::spawn_blocking(move || -> Result<String, pdf_oxide::Error> {
        let doc = pdf_oxide::PdfDocument::open(&fp)?;
        let mut full_text = String::new();
        for i in 0..doc.page_count()? {
            if let Ok(page_text) = doc.extract_text(i) {
                full_text.push_str(&page_text);
                full_text.push('\n');
            }
        }
        Ok(full_text)
    });

    match tokio::time::timeout(std::time::Duration::from_secs(30), handle).await {
        Ok(Ok(Ok(text))) if !text.trim().is_empty() => text,
        Ok(Ok(Ok(_))) => {
            format!(
                "[OCR PENDING] Empty or image-based PDF detected: {}",
                file_path
            )
        }
        Ok(Ok(Err(e))) => {
            tracing::warn!("Failed to extract PDF {}: {}", file_path, e);
            format!("[ERROR] PDF extraction failed: {}", file_path)
        }
        Ok(Err(e)) => {
            tracing::error!("Blocking task panicked for {}: {}", file_path, e);
            format!("[ERROR] PDF extraction panicked: {}", file_path)
        }
        Err(_) => {
            tracing::warn!("PDF extraction timed out for {}", file_path);
            format!("[TIMEOUT] PDF extraction too slow: {}", file_path)
        }
    }
}
