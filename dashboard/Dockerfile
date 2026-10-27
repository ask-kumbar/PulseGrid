FROM node:24-alpine AS builder
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./

# Vite substitutes these at build time. Keep API calls on the UI's origin.
ENV VITE_QUERY_API_URL=/api
RUN npm run build

FROM nginx:1.28-alpine AS runtime
COPY dashboard/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 80
