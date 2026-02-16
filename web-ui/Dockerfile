# ============================================
# Stage 1: Build the Vue.js application
# ============================================
FROM node:22-alpine AS builder

WORKDIR /app

# Copy package files first for better caching
COPY package.json package-lock.json* ./

# Install dependencies
RUN npm ci --prefer-offline --no-audit

# Copy source files
COPY . .

# Optional: pre-populate API key at build time (for demos)
ARG VITE_API_KEY=""
ENV VITE_API_KEY=$VITE_API_KEY

# Build the application
RUN npm run build

# ============================================
# Stage 2: Serve with Nginx
# ============================================
FROM nginx:1.27-alpine

# Install curl for health checks
RUN apk add --no-cache curl

# Copy built files from builder stage
COPY --from=builder /app/dist /usr/share/nginx/html

# Copy nginx configuration
COPY nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Create non-root user for security
RUN adduser -D -H -u 1000 -s /sbin/nologin www-user && \
    chown -R www-user:www-user /var/cache/nginx && \
    chown -R www-user:www-user /var/log/nginx && \
    chown -R www-user:www-user /etc/nginx/conf.d && \
    touch /var/run/nginx.pid && \
    chown -R www-user:www-user /var/run/nginx.pid

# Expose port 8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Run as non-root user
USER www-user

# Start nginx
CMD ["nginx", "-g", "daemon off;"]
