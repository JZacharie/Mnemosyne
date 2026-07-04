CREATE TABLE IF NOT EXISTS search_logs (
    id UUID PRIMARY KEY,
    query TEXT NOT NULL,
    results_count INT NOT NULL DEFAULT 0,
    search_duration_ms INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_search_logs_created_at ON search_logs(created_at);
