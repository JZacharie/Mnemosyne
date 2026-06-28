CREATE TABLE IF NOT EXISTS pipeline_runs (
    id UUID PRIMARY KEY,
    file_path VARCHAR(1024) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL,
    status VARCHAR(50) NOT NULL, -- PENDING, IN_PROGRESS, COMPLETED, FAILED
    current_step VARCHAR(50) NOT NULL, -- SCANNING, PARSING, CHUNKING, EMBEDDING, STORING, COMPLETE
    ocr_status VARCHAR(50) NOT NULL, -- NONE, SUCCESS, FAILED
    error_message TEXT,
    chunks_count INT,
    extracted_text TEXT,
    chunks JSONB, -- list of strings
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    parameters JSONB -- { "chunk_size": 1000, "chunk_overlap": 0 }
);
