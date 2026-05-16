# Mnemosyne 🧠

Mnemosyne is a high-performance, production-grade **RAG (Retrieval-Augmented Generation) Indexer** and **Search Engine** written in **Rust**. Designed for large-scale data lakes and high-concurrency environments, it provides a state-of-the-art hybrid search experience by combining dense semantic vectors with sparse keyword filtering.

## 🚀 Key Features

-   **Parallel Indexing Engine**: Leverages Rust's `Tokio` and `futures` to process multiple files in parallel (default concurrency: 8), saturating high-performance vector databases.
-   **Hybrid Search Strategy**: Combines **Semantic Vector Search** (dense) with **Full-Text Filtering** (sparse) using Qdrant's Query API and Reciprocal Rank Fusion (RRF) logic.
-   **Advanced Reranking**: Integrated with **TEI (Text Embeddings Inference)** for multi-stage retrieval, ensuring the most relevant chunks are always at the top.
-   **Rich Metadata Extraction**:
    *   **Data Integrity**: Automatic **SHA256 hashing** for deduplication and change tracking.
    *   **Contextual Tagging**: Hierarchical folder-based tagging to preserve document structure.
    *   **Temporal Tracking**: Creation and modification date indexing for time-aware retrieval.
-   **Production Infrastructure**:
    *   **Qdrant Native**: Optimized for Qdrant clusters with on-disk payload storage and high-performance collection settings.
    *   **PDF Intelligence**: Robust extraction using `pdf-extract` with `spawn_blocking` pools to prevent thread starvation.
    *   **Observability**: Structured JSON logging via `tracing` with silenced noisy libraries for clean Kubernetes logs.

## 🏗️ Architecture

Mnemosyne follows the **Hexagonal Architecture** (Ports & Adapters) for maximum modularity:

-   **Domain**: Core entities (`Document`, `Chunk`) and ports (traits for `VectorStore`, `EmbeddingService`).
-   **Application**: Parallelized use cases for indexing, auth, and hybrid retrieval.
-   **Infrastructure**:
    *   **Vector Store**: [Qdrant](https://qdrant.tech/) (Primary).
    *   **Embedding/Rerank**: [HuggingFace TEI](https://github.com/huggingface/text-embeddings-inference).
    *   **Database**: [PostgreSQL](https://www.postgresql.org/) (Account management & persistent metadata).
-   **Interfaces**: High-performance REST API built with [Axum](https://github.com/tokio-rs/axum).

## 🛠️ Configuration

Mnemosyne is configured via environment variables (12-Factor App compliant).

| Variable | Description | Default |
| :--- | :--- | :--- |
| `DATABASE_URL` | PostgreSQL connection string | **Required** |
| `QDRANT_URL` | Qdrant REST/gRPC endpoint | `http://qdrant.qdrant.svc.cluster.local:6333` |
| `TEI_EMBEDDER_URL` | TEI Embedding Service endpoint | `http://mnemosyne-tei-embedder` |
| `TEI_RERANKER_URL` | TEI Reranking Service endpoint | `http://mnemosyne-tei-reranker` |
| `NFS_PATH` | Path(s) to scan for documents (comma-separated) | `/data/nfs` |
| `COLLECTION_NAME` | Qdrant collection name | `mnemosyne_docs` |
| `EMBEDDING_MODEL` | Model ID for tracking/metadata | `BAAI/bge-m3` |
| `HTTP_PORT` | Port for the API server | `8080` |
| `ONESHOT` | If `true`, exit after indexing is complete | `false` |
| `RUST_LOG` | Logging level (`info`, `debug`, `error`) | `info` |

## 📦 Deployment

### Kubernetes (ArgoCD / Helm)

The modern deployment uses **Helm** and **GitOps**. Manifests are integrated into the `helmscharts` repository.

```bash
# Example manual installation
helm install mnemosyne ./kubernetes/helm \
  --set config.qdrantUrl="http://qdrant:6333"
```

### Auditing Tooling

Includes a utility job for identifying non-indexed files within the NFS share:
```bash
kubectl apply -f kubernetes/check-index-job.yaml
```

## 🛠️ Development

### Prerequisites
-   Rust 1.80+
-   Tesseract OCR dev headers (`libtesseract-dev`, `libleptonica-dev`)

### Setup & Run
```bash
# Build
cargo build --release

# Run Indexing + Server
cargo run -- --paths /mnt/data --database-url $DB_URL
```

## 📜 Legacy Version
The original Python implementation and migration notes are preserved in the `python-version/` and `docs/migration/` directories.

---
**Mnemosyne** — *Because knowledge is only as powerful as your ability to find it.*
