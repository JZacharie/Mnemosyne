# Mnemosyne 🧠

Mnemosyne is a modern, high-performance RAG indexer written in **Rust**. It follows the **12-Factor App** methodology and uses **Hexagonal Architecture** for maximum maintainability and testability.

## 🏗️ Architecture

Mnemosyne is built using **Ports & Adapters** (Hexagonal Architecture):

- **Domain**: Core business logic and entities.
- **Application**: Use cases orchestration (e.g., `IndexingUseCase`).
- **Infrastructure**: Technical implementations for:
  - **Vector Store**: PostgreSQL with `pgvector` and `pgvectorscale`.
  - **Embedding Service**: LiteLLM / OpenAI API.
  - **File Scanner**: Local filesystem / PVC scanning.

## 🚀 Features

- **Performance**: Built with Rust and Tokio for high-concurrency indexing.
- **12-Factor Compliant**: Configuration via environment variables, stateless execution, and structured logging.
- **PgVectorScale Support**: Optimized for Timescale's vector extension.
- **Multi-PVC Indexing**: Supports mounting and scanning multiple PVCs.
- **CI/CD Ready**: Includes GitHub Actions and Helm charts for GitOps/ArgoCD.

## 🛠️ Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `LITELLM_URL` | LiteLLM API endpoint | `http://litellm:4000` |
| `LITELLM_API_KEY` | LiteLLM API Key | Required |
| `NFS_PATH` | Path to scan for documents | `/data/nfs` |
| `COLLECTION_NAME` | PGVector collection name | `mnemosyne_docs` |
| `EMBEDDING_MODEL` | Model to use for embeddings | `zembed-132k` |

## 📦 Deployment

### Helm

The Helm chart is located in `deploy/helm/mnemosyne`.

```bash
helm install mnemosyne ./deploy/helm/mnemosyne \
  --set secrets.existingSecret=my-secrets
```

### GitOps (ArgoCD)

Mnemosyne is designed to be deployed via ArgoCD. Use the provided Helm chart and point it to your configuration.

## 🛠️ Development

### Setup

```bash
cargo build
```

### Running

```bash
cargo run -- --paths /path/to/docs
```

## 📜 Legacy Version

The original Python version is preserved in the `python-version/` directory.
