# Varpulis Stream Builder - Visual Pipeline Editor
# Multi-stage build for minimal image size

# =============================================================================
# Stage 1: Build
# =============================================================================
FROM node:24-alpine AS builder

WORKDIR /app

# Copy manifests first for layer caching
COPY package.json package-lock.json ./
RUN npm ci

# Copy source and build
COPY . .
RUN npm run build

# =============================================================================
# Stage 2: Runtime
# =============================================================================
FROM nginx:1.27-alpine AS runtime

RUN adduser -D -H -u 1000 -g www-user www-user

COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx-demo.conf /etc/nginx/conf.d/default.conf

RUN chown -R www-user:www-user /var/cache/nginx /var/log/nginx /usr/share/nginx/html && \
    touch /var/run/nginx.pid && chown www-user:www-user /var/run/nginx.pid

RUN apk add --no-cache curl

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
