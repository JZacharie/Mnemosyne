# Stage 1: Chef
FROM lukemathwalker/cargo-chef:latest-rust-1.88-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Install system dependencies BEFORE building deps (for linking)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libtesseract-dev \
    libleptonica-dev \
    pkg-config \
    clang \
    && rm -rf /var/lib/apt/lists/*
# Build dependencies - this is the caching layer
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release --bin mnemosyne

# Stage 2: Runtime
FROM debian:bookworm-slim
WORKDIR /app

# Install runtime dependencies only (no -dev packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    tesseract-ocr \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/mnemosyne /usr/local/bin/mnemosyne

# Create a non-root user
RUN useradd -m mnemosyne
USER mnemosyne

ENTRYPOINT ["mnemosyne"]
