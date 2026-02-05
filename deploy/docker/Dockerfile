# Varpulis CEP Engine - Production Dockerfile
# Multi-stage build for minimal image size

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM rust:1.85-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    cmake \
    libclang-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates/varpulis-core/Cargo.toml crates/varpulis-core/
COPY crates/varpulis-parser/Cargo.toml crates/varpulis-parser/
COPY crates/varpulis-runtime/Cargo.toml crates/varpulis-runtime/
COPY crates/varpulis-cli/Cargo.toml crates/varpulis-cli/
COPY crates/varpulis-lsp/Cargo.toml crates/varpulis-lsp/
COPY crates/varpulis-zdd/Cargo.toml crates/varpulis-zdd/
COPY crates/varpulis-cluster/Cargo.toml crates/varpulis-cluster/

# Create dummy source files to cache dependencies
RUN mkdir -p crates/varpulis-core/src && echo "pub fn dummy() {}" > crates/varpulis-core/src/lib.rs && \
    mkdir -p crates/varpulis-parser/src && echo "pub fn dummy() {}" > crates/varpulis-parser/src/lib.rs && \
    mkdir -p crates/varpulis-runtime/src && echo "pub fn dummy() {}" > crates/varpulis-runtime/src/lib.rs && \
    mkdir -p crates/varpulis-cli/src && echo "fn main() {}" > crates/varpulis-cli/src/main.rs && \
    mkdir -p crates/varpulis-lsp/src && echo "fn main() {}" > crates/varpulis-lsp/src/main.rs && \
    mkdir -p crates/varpulis-zdd/src && echo "pub fn dummy() {}" > crates/varpulis-zdd/src/lib.rs && \
    mkdir -p crates/varpulis-cluster/src && echo "pub fn dummy() {}" > crates/varpulis-cluster/src/lib.rs

# Build dependencies (this layer is cached)
RUN cargo build --release -p varpulis-cli 2>/dev/null || true

# Copy actual source code
COPY crates/ crates/

# Touch source files to ensure rebuild
RUN touch crates/*/src/*.rs crates/*/src/**/*.rs 2>/dev/null || true

# Build release binary with optional features
ARG FEATURES=""
RUN if [ -n "$FEATURES" ]; then \
        cargo build --release -p varpulis-cli --features "$FEATURES"; \
    else \
        cargo build --release -p varpulis-cli; \
    fi

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    libssl3 \
    libcurl4 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 1000 -g users varpulis

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/target/release/varpulis /usr/local/bin/varpulis

# Create directories for queries and data
RUN mkdir -p /app/queries /app/data /app/state && \
    chown -R varpulis:users /app

# Switch to non-root user
USER varpulis

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9000/health || exit 1

# Default ports
# 9000 - HTTP/WebSocket API + health endpoints
# 9090 - Prometheus metrics
EXPOSE 9000 9090

# Default entrypoint
ENTRYPOINT ["varpulis"]

# Default command (can be overridden)
CMD ["--help"]
