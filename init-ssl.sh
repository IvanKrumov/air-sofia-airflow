#!/bin/bash
# SSL Setup Script for airsofia.eu
# Run this ONCE on the server after docker compose up -d

set -e

DOMAIN="airsofia.eu"
EMAIL="${1:?Usage: ./init-ssl.sh your-email@example.com}"

echo "=== Step 1: Start with HTTP-only Nginx config ==="
cp nginx/nginx-initial.conf nginx/nginx.conf
docker compose up -d nginx webapp postgres

echo "=== Step 2: Wait for Nginx to be ready ==="
sleep 5

echo "=== Step 3: Request SSL certificate from Let's Encrypt ==="
docker compose run --rm --entrypoint "" certbot \
    certbot certonly \
    --webroot \
    --webroot-path=/var/www/certbot \
    --email "$EMAIL" \
    --agree-tos \
    --no-eff-email \
    -d "$DOMAIN" \
    -d "www.$DOMAIN"

echo "=== Step 4: Switch to HTTPS Nginx config ==="
cat > nginx/nginx.conf << 'NGINXEOF'
events {
    worker_connections 1024;
}

http {
    include       /etc/nginx/mime.types;
    default_type  application/octet-stream;

    upstream flask_app {
        server webapp:5000;
    }

    # Redirect HTTP to HTTPS
    server {
        listen 80;
        server_name airsofia.eu www.airsofia.eu;

        location /.well-known/acme-challenge/ {
            root /var/www/certbot;
        }

        location / {
            return 301 https://$host$request_uri;
        }
    }

    # HTTPS server
    server {
        listen 443 ssl;
        server_name airsofia.eu www.airsofia.eu;

        ssl_certificate /etc/letsencrypt/live/airsofia.eu/fullchain.pem;
        ssl_certificate_key /etc/letsencrypt/live/airsofia.eu/privkey.pem;
        ssl_protocols TLSv1.2 TLSv1.3;
        ssl_ciphers HIGH:!aNULL:!MD5;

        location / {
            proxy_pass http://flask_app;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_read_timeout 120s;
            proxy_buffer_size 128k;
            proxy_buffers 4 256k;
        }
    }
}
NGINXEOF

echo "=== Step 5: Reload Nginx with SSL ==="
docker compose exec nginx nginx -s reload

echo "=== Done! ==="
echo "https://$DOMAIN is now live with SSL"
echo "Certificate will auto-renew via the certbot container"
