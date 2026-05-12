# JRSHOP Catalog — AI Agent Guide

## Quick Start

**Tech Stack**: Vanilla JS SPA + Cloudflare Worker + D1 (SQLite) + Firebase (Firestore/Storage/Auth)

**Critical Architecture**: Server-first sync model with idempotent operations, role-based access control, and offline-first fallbacks.

---

## 1. Key Files & Entry Points

| File | Purpose | When to Edit |
|------|---------|--------------|
| `index.html` | Main SPA (all UI, theme, styles, inline scripts) | Adding pages, fixing UI, modifying state listeners |
| `js/state.js` | Centralized pub/sub state hub | Adding state keys, debugging state sync issues |
| `js/api.js` | API client wrapper for `/api/*` endpoints | Adding API calls, error handling |
| `js/auth.js` | Session management, role checks, Firebase auth | Authentication flows, permission issues |
| `worker.js` | Cloudflare Worker backend (legacy) | Adding endpoints, validating role-gated ops |
| `pages/*.html` | Page fragments (catalog, orders, users, etc.) | Feature development per page |
| `firestore.rules` | Role-based Firestore security rules | Adjusting Reseller/Admin/Staff permissions |
| `wrangler.toml` | Worker config + D1 database binding | Database setup, deployments |

---

## 2. Architecture Overview

### **Data Flow**

```
Frontend (index.html + js/)
  ↓ (writeLocalJson + saveOrders)
  ↓
POST /api/data  or  POST /api/ops
  ↓
Cloudflare Worker (worker.js)
  ↓
D1 (SQLite) or Firestore
  ↓
Frontend (loadFromCloudSilent periodic sync)
```

### **State Management Pattern**

- All app state managed via `State` object (pub/sub)
- State keys prefixed with `jb_*` → persist to server via `writeLocalJson(key, value)`
- Subscribe: `State.on('orders', callback)` — fires when state changes
- Update: Direct assignment + `State.broadcast()` or use save functions

### **Sync Model**

1. **Server-First**: All writes go to `/api/data` first; fallback to localStorage if offline
2. **Idempotent Ops**: Each mutation tagged with `opId` (prevents duplicates on retry)
3. **Mutation Markers**: On success, clear with `clearLocalOrderMutation()` (fixes "pending" spam)
4. **Periodic Sync**: `loadFromCloudSilent()` (~30s) auto-retries failed ops in `jb_sync_fallback`

---

## 3. Common Workflows

### **Adding a New Order Action**

1. **Define action handler** in `pages/pg-orders.html` or appropriate page
2. **Use `advanceStatus()` or `saveOrders()`**:
   ```js
   await advanceStatus(orderId, newStatus);
   ```
3. **State will auto-broadcast** → UI re-renders via listeners
4. **Fallback handled**: If offline/error, queued in `jb_sync_fallback`

### **Adding a New State Key**

1. Add to `hydrateSharedStateServerFirst()` keys list in `js/state.js`
2. Call `State.set('jb_new_key', value)` to initialize
3. Use `saveOrders()` or `writeLocalJson('jb_new_key', value)` to persist
4. Subscribe: `State.on('jb_new_key', callback)`

### **Adding a New API Endpoint**

1. Extend `worker.js` with route handler:
   ```js
   if (req.pathname === '/api/myendpoint') {
     return handleMyEndpoint(req);
   }
   ```
2. Add to `js/api.js` wrapper:
   ```js
   API.myEndpoint = async (data) => {
     return fetch('/api/myendpoint', { 
       method: 'POST', body: JSON.stringify(data) 
     }).then(r => r.json());
   };
   ```
3. Call from UI: `await API.myEndpoint(data)`

### **Debugging Sync Issues**

1. **Enable debug logs** in browser console:
   ```js
   localStorage.setItem('jb_debug_logs', '1');
   location.reload();
   ```
2. **Check logs**:
   - `[Remote] Loading data...` — fetching from server
   - `[Remote] Response status: ...` — server response
   - `Skip authoritative replace due to pending local mutations` — mutation marker issue (now fixed)
3. **Check fallback queue**: `State.get('jb_sync_fallback')` should empty after ~30s
4. **Worker logs**: Cloudflare dashboard → Workers → `jrshop-api` logs

---

## 4. Role-Based Access & Security

### **Role Model**

- **Admin**: Full read/write to all Firestore collections
- **Staff**: Full read/write (same as Admin for most operations)
- **Reseller**: Read all collections, but write-restricted:
  - `orders`: can only modify where `resellerId == auth.uid` or `resellerEmail == auth.email`
  - `catalog_orders`: reseller-scoped similarly
  - App data keys (`jb_*`): Admin/Staff only

### **Role Resolution**

1. Check Firebase custom claim: `auth.currentUser?.customClaims?.role`
2. Fallback to Firestore: `user_roles/{uid}` or `user_roles/{email-lowercase}`
3. Role synced on login via `hydrateSharedStateServerFirst()`

### **Permission Checking**

- Use `auth.isAdmin()`, `auth.isStaff()`, `auth.isReseller()` helpers
- Server-side validation in `worker.js` (client checks are UI only)
- Firestore rules enforce: deploy with `firebase deploy --only firestore:rules`

---

## 5. Image & Media Handling

### **Safe Image Rendering**

Before rendering any order/product image:

```js
// 1. Check if URL is safe
if (!isUsableOrderImage(url)) return null;

// 2. Get normalized URL
const renderUrl = getRenderableImageUrl(url);

// 3. Use onerror handler to cache broken images
<img src={renderUrl} onerror="handleBrokenImage(this)" />
```

### **Why This Matters**

- **Broken cache** (`_brokenImageCache`) prevents repeated 404 requests
- **Image variants** cached (relative, absolute, origin-prefixed) to cover retry patterns
- **Allowed sources**: `https:`, `data:`, `blob:`, `/api/image/:id`, **not** arbitrary HTTP/relative URLs

---

## 6. Important Conventions & Gotchas

### ⚠️ **Critical Changes**

1. **Orders not in localStorage** — migrated to remote-first; only metadata cached locally. Full snapshots from `/api/orders`
2. **Mutation markers fix** — pending status now properly cleared after cloud ACK (`clearLocalOrderMutation()`)
3. **Image 404 spam fixed** — broken URLs cached to prevent repeated requests
4. **Reseller scoping** — write restrictions enforced server-side; validation in `worker.js`

### **Conventions**

- **State keys**: Always `jb_*` prefix (e.g., `jb_orders`, `jb_users`, `jb_catalog`)
- **Timestamps**: Include in metadata to track sync state
- **Error messages**: Show via toast (auto-dismiss) + console logs
- **Debug logs**: Gated by `localStorage['jb_debug_logs']` (off by default)

---

## 7. Deployment & Testing

### **Dev Workflow**

```bash
# Frontend: edit index.html or pages/
# Worker: edit worker.js or js/api.js
wrangler dev                  # Local Cloudflare Worker
firebase serve --only hosting # Local Firebase Hosting (separate terminal)
```

### **Validation**

- **Frontend loaded**: Open http://localhost:8787 (or Firebase Hosting URL)
- **Worker alive**: `GET /api/test` should return `{ ok: true, hasDbBinding: true, dbReady: true }`
- **Auth working**: Sign in via Firebase UI
- **Sync working**: Make a change, verify it persists across page reload

### **Deploy**

```bash
# Deploy Worker + D1
wrangler publish

# Deploy rules + static frontend
firebase deploy

# Deploy rules only
firebase deploy --only firestore:rules,storage
```

---

## 8. Smoke Tests (Before Deploy)

- [ ] Login with valid credentials
- [ ] Load order list (badge should show count)
- [ ] Mark product "arrived" (status updates + persists)
- [ ] Advance order status (server ACK visible + no "pending" spam)
- [ ] Add/edit user (if Admin)
- [ ] View catalog/suppliers (if Staff/Reseller)
- [ ] Missing images show placeholder (no repeated 404s)
- [ ] Offline: make changes (should queue), go online (auto-sync)

---

## 9. Documentation & References

See detailed docs for architecture & implementation:

- [README.md](README.md) — Overview, Firebase rules, debug logging, broken images
- [SYSTEM.md](SYSTEM.md) — Detailed architecture, endpoints, sync model, deploy checklist (in Turkish)
- [FIREBASE_SECURITY.md](FIREBASE_SECURITY.md) — Role model, Firestore collections, auth flow
- [CHANGELOG_LOCAL_SYNC_TESTING.md](CHANGELOG_LOCAL_SYNC_TESTING.md) — Recent sync & notification fixes
- [WORKER_API_SETUP.md](WORKER_API_SETUP.md) — Worker endpoints & D1 integration

---

## 10. Quick Debug Checklist

| Problem | Diagnosis | Fix |
|---------|-----------|-----|
| Orders not loading | `loadFromCloudSilent()` error? | Check network, Worker logs, Firebase rules |
| Data stuck "pending" | Mutation marker not cleared | Should auto-clear now; if not, check `clearLocalOrderMutation()` logic |
| Missing images | URLs broken or not in `kv_store` | Enable debug logs, check `getRenderableImageUrl()`, confirm `/api/image/:id` returns 200 |
| Permission denied | Firestore rule rejection | Check user role in Firebase, verify `user_roles/{uid}` exists |
| Offline changes lost | Fallback queue not retrying | Ensure `jb_sync_fallback` populated; `loadFromCloudSilent()` should retry |
| Worker not responding | D1 binding missing | Check `wrangler.toml`: binding `DB` points to `jrshop-db` |

---

## 11. Helpful One-Liners

```js
// Check app state
State.all()

// Enable debug logs
localStorage.setItem('jb_debug_logs', '1'); location.reload();

// Check sync queue
State.get('jb_sync_fallback')

// Check user role
auth.getRole()

// Manual sync attempt
loadFromCloudSilent()

// Clear broken image cache
localStorage.removeItem('jb_broken_images'); location.reload();

// View Firestore auth token
console.log(await auth.getFirebaseUser()?.getIdTokenResult())
```

---

**Last Updated**: 2026-05-12  
**Agent Focus**: Productivity-first; link to detailed docs for deep dives.
