# Varpulis CEP Engine - Release Docker Image
# This Dockerfile is used by the release workflow with pre-built binaries

FROM debian:bookworm-slim

# Build argument for version
ARG VERSION=dev

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libcurl4 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -u 1000 -g users varpulis

WORKDIR /app

# Copy pre-built binary from release workflow
COPY docker-build/varpulis-linux-x86_64 /usr/local/bin/varpulis
RUN chmod +x /usr/local/bin/varpulis

# Create directories for queries and data
RUN mkdir -p /app/queries /app/data /app/state && \
    chown -R varpulis:users /app

# Add version label
LABEL org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.title="Varpulis CEP Engine" \
      org.opencontainers.image.description="High-performance Complex Event Processing engine" \
      org.opencontainers.image.source="https://github.com/cpo/cep"

# Switch to non-root user
USER varpulis

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default ports
# 8080 - HTTP webhook input / health endpoints
# 9090 - Prometheus metrics
EXPOSE 8080 9090

# Default entrypoint
ENTRYPOINT ["varpulis"]

# Default command (can be overridden)
CMD ["--help"]
