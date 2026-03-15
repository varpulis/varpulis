FROM node:24-alpine AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
# Docs content under /docs/ path
COPY --from=build /app/.vitepress/dist /usr/share/nginx/html/docs
# SEO files at root (served via www.varpulis-cep.com/sitemap.xml)
COPY --from=build /app/.vitepress/dist/sitemap.xml /usr/share/nginx/html/sitemap.xml
COPY --from=build /app/.vitepress/dist/robots.txt /usr/share/nginx/html/robots.txt
EXPOSE 80
