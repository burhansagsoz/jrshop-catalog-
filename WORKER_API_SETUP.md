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

