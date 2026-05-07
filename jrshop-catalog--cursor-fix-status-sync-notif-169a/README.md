# jrshop-catalog-

## Firebase security and deployment artifacts

This repository now includes first-phase Firebase security files:

- `firestore.rules` - role-based Firestore access policy
- `storage.rules` - media upload/download policy for authenticated users
- `firebase.json` - points Firebase CLI to these rules
- `FIREBASE_SECURITY.md` - deployment and role model guide

Apply rules with Firebase CLI:

```bash
firebase deploy --only firestore:rules,storage
```

## Local debug logging (new)

To reduce console noise in normal usage, some verbose logs now run only in debug mode.

### What changed

- `[Users] merged from ...`
- `[Remote] Loading data...`
- `[Remote] Response status: ...`
- `[Remote] Loaded ... orders`

These logs are now gated by `localStorage["jb_debug_logs"]`.

### How to enable debug logs

Run in browser console:

```js
localStorage.setItem('jb_debug_logs','1');
location.reload();
```

### How to disable debug logs

Run in browser console:

```js
localStorage.removeItem('jb_debug_logs');
location.reload();
```

## Local sync guard fix (new)

A local sync issue was fixed where order mutations could stay marked as "pending" after cloud ACK.

### Symptom

- Repeated warning spam:
  - `Skip authoritative replace due to pending local mutations`

### Fix

- On successful ACK, mutation markers are now cleared with `clearLocalOrderMutation(...)`
  instead of being re-marked as pending.

## Broken image handling (new)

To prevent repeated 404 requests and console spam for missing images, client-side broken-image caching was added.

### What changed

- Added local cache key: `jb_broken_images`
- Added helpers:
  - `persistBrokenImageCache()`
  - `normalizeImageUrlForRender(url)`
  - `getRenderableImageUrl(url)`
  - `handleBrokenImage(el)`
- Updated image render points to:
  - skip URLs already marked as broken
  - use `onerror="handleBrokenImage(this)"`
  - show placeholder when image is unavailable

### Why this helps

- Stops retrying the same missing `/api/image/...` URLs on every render/navigation
- Reduces network noise and console spam
- Keeps UI responsive by showing placeholders for broken assets

### Important note for local testing

- If backup data includes only `data:image/...` images, those render directly.
- If backup data includes `/api/image/...` references, local DB must also include `kv_store` `img_*` keys.
- Missing `img_*` keys means some images will not render locally (expected behavior).

### Reset broken-image cache (optional)

Run in browser console:

```js
localStorage.removeItem('jb_broken_images');
location.reload();
```

## Session lock hotfix (temporary)

Session lock was temporarily disabled to prevent users from losing in-progress changes after re-entering password.

### What changed

- Added `SESSION_LOCK_ENABLED=false` in `index.html`
- `resetSessionTimer()` now returns immediately when session lock is disabled
- Session activity listeners are not attached when lock is disabled
- `sessionUnlock()` safely no-ops when lock is disabled

### Why this was needed

- Customer reported: when session lock appears and user unlocks/relogs, recent changes can disappear
- Temporary mitigation is to disable lock flow until state persistence is fully hardened

### Fastest way to test this fix

1. Hard refresh (`Ctrl+F5`)
2. Edit an existing order (e.g. note/status) and save
3. Wait longer than old timeout threshold (or keep app open and idle)
4. Verify:
   - no session lock overlay appears
   - saved changes still exist after refresh

### Extra quick validation (console)

Run:

```js
typeof SESSION_LOCK_ENABLED !== 'undefined' ? SESSION_LOCK_ENABLED : 'missing'
```

Expected result: `false`