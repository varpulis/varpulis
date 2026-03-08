# ============================================
# Stage 1: Build the Vue.js public site
# ============================================
FROM node:22-alpine AS builder

WORKDIR /app/public-site

# Copy package files first for better caching
COPY public-site/package.json public-site/package-lock.json* ./

# Install dependencies
RUN npm ci --prefer-offline --no-audit

# Copy public-site source
COPY public-site/ .

# Copy shared web-ui source (imported via @ alias → ../web-ui/src)
COPY web-ui/src/ /app/web-ui/src/

# Symlink node_modules to /app/ so TypeScript can resolve modules
# from web-ui source files (it walks up: /app/web-ui/src → /app/)
RUN ln -s /app/public-site/node_modules /app/node_modules

# Optional: pre-populate API key at build time (for demos)
ARG VITE_API_KEY=""
ENV VITE_API_KEY=$VITE_API_KEY

# Version: defaults to package.json version, can be overridden
ARG VITE_APP_VERSION=""
ENV VITE_APP_VERSION=$VITE_APP_VERSION

# Build the application
RUN npm run build

# ============================================
# Stage 2: Serve with Nginx
# ============================================
FROM nginx:1.27-alpine

# Install curl for health checks
RUN apk add --no-cache curl

# Copy built files from builder stage
COPY --from=builder /app/public-site/dist /usr/share/nginx/html

# Copy nginx configuration
COPY public-site/nginx/nginx.conf /etc/nginx/conf.d/default.conf

# Create non-root user for security
RUN adduser -D -H -u 1000 -s /sbin/nologin www-user && \
    chown -R www-user:www-user /var/cache/nginx && \
    chown -R www-user:www-user /var/log/nginx && \
    chown -R www-user:www-user /etc/nginx/conf.d && \
    touch /var/run/nginx.pid && \
    chown -R www-user:www-user /var/run/nginx.pid

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

USER www-user

CMD ["nginx", "-g", "daemon off;"]
