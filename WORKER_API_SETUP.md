# Cloudflare Worker API Setup (Recommended)

If Pages Functions API is unstable in your environment, use a dedicated Worker API.

This app supports a custom API base URL from Settings:

- `Cloudflare Worker API URL`
- `Cloudflare API Key (optional)`

Use the steps below to deploy a standalone API Worker and point the app to it.

## 1) Create Worker in Cloudflare

1. Cloudflare Dashboard -> Workers & Pages -> Create application -> Worker.
2. Name it (example): `jrshop-api`.
3. Replace generated code with this repository's `worker.js` content.

## 2) Bind D1 database

In Worker settings -> Bindings:

- Add binding type: **D1 Database**
- Variable name: **`DB`**
- Select your existing D1 database (or create one)

`DB` name is mandatory because `worker.js` expects `env.DB`.

## 3) Deploy Worker

Deploy from dashboard or CLI. After deploy, get Worker URL:

- `https://jrshop-api.<your-subdomain>.workers.dev`

## 4) Validate API health

Open:

- `https://jrshop-api.<your-subdomain>.workers.dev/api/test`

Expected JSON:

- `"ok": true`
- `"hasDbBinding": true`
- `"dbReady": true`

If `ok` is false, fix D1 binding first.

## 5) Point app to Worker API

In app Settings:

- Cloudflare Worker API URL -> `https://jrshop-api.<your-subdomain>.workers.dev`
- Cloudflare API Key -> leave empty (unless you enforce auth)
- Save

Then hard refresh and test `Push to Cloudflare`.

## 6) Optional: lock CORS to your domain

Current worker uses `Access-Control-Allow-Origin: *`.
You can restrict it later to your production domain after everything is stable.

## 7) Security and reliability env variables (recommended)

Set these Worker variables for the hardened API mode:

- Authentication
  - `JWT_AUTH_SECRET` (preferred)
  - `API_AUTH_TOKEN` (optional admin fallback token)
  - `SETUP_ENABLED=false` in production
- Default admin bootstrap
  - `DEFAULT_ADMIN_EMAIL`
  - `DEFAULT_ADMIN_PASSWORD`
- Logistics provider
  - `HUALEI_BASE_URL`
  - `HUALEI_USERNAME`
  - `HUALEI_PASSWORD`
  - `HUALEI_LABEL_BASE_URL` (optional)
- Rate/replay protection
  - `API_RATE_LIMIT_WINDOW_MS`
  - `API_RATE_LIMIT_MAX`
  - `REPLAY_MAX_SKEW_MS`
  - `REPLAY_CACHE_MS`
- Secret rotation health
  - `SECRET_ROTATED_AT` (epoch ms)
  - `SECRET_MAX_AGE_MS`

New operator endpoints:

- `GET /api/auth/health` (public): auth mode + secret rotation status
- `GET /api/ops/health` (Admin): rate/replay/auth metrics + outbox/audit summary

