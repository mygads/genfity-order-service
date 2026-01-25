# Genfity Order Services (Go)

Single Go app (one port) to offload high-traffic APIs and realtime updates from Next.js.

## Service Components (single process)

- **order-api**: REST APIs for high-frequency order/customer endpoints.
- **order-ws**: WebSocket push server (client polling removal).
- **order-worker**: RabbitMQ consumer/translator that publishes Next-compatible email/notification/webpush jobs (optional when `RABBITMQ_URL` is set).

## Environment Variables

- `APP_ENV` (default: `development`)
- `DATABASE_URL` (required)
- `JWT_SECRET` (required)
- `JWT_EXPIRY` (default: 3600)
- `JWT_REFRESH_EXPIRY` (default: 604800)
- `ORDER_TRACKING_TOKEN_SECRET` (default: `dev-insecure-tracking-secret`)
- `HTTP_ADDR` (default: `:8086`) — API + WS share the same port
- `CORS_ALLOWED_ORIGINS` (comma-separated)
- `WS_MERCHANT_POLL_INTERVAL` (default: `5s`)
- `WS_CUSTOMER_POLL_INTERVAL` (default: `5s`)
- `WS_GROUP_ORDER_POLL_INTERVAL` (default: `5s`)
- `RABBITMQ_URL` (optional, enables order-worker)
- `RABBITMQ_WORKER_MODE` (default: `daemon`) — `daemon` runs a background consumer

### Local `.env` (optional)

This service reads environment variables from the process and will auto-load a local `.env` file (if present). You can still override values via your shell or tooling (for example `set -a; source .env; set +a` on bash, or `Get-Content .env | ForEach-Object { $name,$value = $_ -split '=',2; [Environment]::SetEnvironmentVariable($name,$value) }` on PowerShell).

Example file: [.env.example](.env.example)

Minimal local example:

```
APP_ENV=development
DATABASE_URL=postgres://user:pass@localhost:5432/genfity
JWT_SECRET=your-secret
ORDER_TRACKING_TOKEN_SECRET=dev-insecure-tracking-secret
HTTP_ADDR=:8086
CORS_ALLOWED_ORIGINS=http://localhost:3000
```

## Run Locally

From this folder:

- `go run .` (single app: API + WS on one port; worker starts only if `RABBITMQ_URL` is set)

Legacy split binaries under `cmd/` are disabled by default (build tag `legacy`).

## Deploy (Server or Docker)

Recommended options:

1. **systemd service** (server): set environment in the unit file or an env file, then run `ExecStart=/path/to/order-services`.
2. **Docker / Compose**: pass environment via `environment:` or `env_file:` and expose port `HTTP_ADDR`.

Both approaches require the same environment variables listed above.

### Docker (single container)

Build:

```
docker build -t genfity-order-services .
```

Run:

```
docker run --env-file .env -p 8086:8086 genfity-order-services
```

## Endpoints (order-api)

Merchant:
- `GET /api/merchant/orders`
- `GET /api/merchant/orders/active`
- `GET /api/merchant/orders/analytics`
- `GET /api/merchant/orders/stats`
- `GET /api/merchant/orders/resolve?orderNumber=...`
- `GET /api/merchant/orders/{orderId}`
- `PATCH /api/merchant/orders/{orderId}`
- `DELETE /api/merchant/orders/{orderId}`
- `PUT /api/merchant/orders/{orderId}/status`
- `PUT /api/merchant/orders/{orderId}/admin-note`
- `POST /api/merchant/orders/{orderId}/payment`
- `POST /api/merchant/orders/{orderId}/cancel`
- `PUT /api/merchant/orders/{orderId}/delivery/assign`
- `POST /api/merchant/orders/{orderId}/cod/confirm`
- `GET /api/merchant/orders/{orderId}/tracking-token`
- `GET /api/merchant/orders/{orderId}/receipt-html`
- `GET /api/merchant/orders/{orderId}/receipt`
- `GET /api/merchant/orders/pos/history`
- `GET /api/merchant/orders/pos/voucher-templates`
- `POST /api/merchant/orders/pos/validate-voucher`
- `POST /api/merchant/orders/pos/validate-voucher-template`
- `POST /api/merchant/orders/pos/refund`
- `POST /api/merchant/orders/pos/payment`
- `POST /api/merchant/orders/pos`
- `GET /api/merchant/orders/pos/{orderId}`
- `PUT /api/merchant/orders/pos/{orderId}`
- `GET /api/merchant/reservations`
- `GET /api/merchant/reservations/active`
- `GET /api/merchant/reservations/count`
- `GET /api/merchant/reservations/{reservationId}/preorder`
- `PUT /api/merchant/reservations/{reservationId}/accept`
- `PUT /api/merchant/reservations/{reservationId}/cancel`
- `GET /api/merchant/customers/search`
- `GET /api/merchant/customer-display/state`
- `PUT /api/merchant/customer-display/state`
- `GET /api/merchant/customer-display/sessions`

Public:
- `POST /api/public/orders`
- `GET /api/public/orders/{orderNumber}`
- `GET /api/public/orders/{orderNumber}/wait-time`
- `GET /api/public/orders/{orderNumber}/group-details`
- `GET /api/public/orders/{orderNumber}/feedback`
- `POST /api/public/orders/{orderNumber}/feedback`
- `GET /api/public/geocode/forward`
- `GET /api/public/geocode/reverse`
- `POST /api/public/vouchers/validate`
- `POST /api/public/reservations`
- `POST /api/public/group-order`
- `GET /api/public/group-order/{code}`
- `DELETE /api/public/group-order/{code}`
- `POST /api/public/group-order/{code}/join`
- `DELETE /api/public/group-order/{code}/leave`
- `PUT /api/public/group-order/{code}/cart`
- `DELETE /api/public/group-order/{code}/kick`
- `POST /api/public/group-order/{code}/transfer-host`
- `POST /api/public/group-order/{code}/submit`
- `GET /api/public/menu/{merchantCode}`
- `GET /api/public/merchants/{code}`
- `GET /api/public/merchants/{code}/categories`
- `GET /api/public/merchants/{code}/status`
- `GET /api/public/merchants/{code}/stock-stream`
- `GET /api/public/merchants/{code}/available-times`
- `POST /api/public/merchants/{code}/delivery/quote`
- `GET /api/public/merchants/{code}/menus`
- `GET /api/public/merchants/{code}/menus/{id}`
- `GET /api/public/merchants/{code}/menus/{id}/addons`
- `GET /api/public/merchants/{code}/menus/search`
- `GET /api/public/merchants/{code}/recommendations`

## WebSocket Endpoints (order-ws)

WebSocket endpoints are served on the same host/port as `HTTP_ADDR`.

- `GET /ws/merchant/orders?token=Bearer%20<accessToken>`
- `GET /ws/merchant/customer-display?token=Bearer%20<accessToken>`
- `GET /ws/public/order?orderNumber=...&token=...`
- `GET /ws/public/group-order?code=...`

Payloads are small refresh signals. Clients should refetch via REST when events arrive.
