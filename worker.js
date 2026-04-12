// Legacy Cloudflare Worker (not used in Firebase-only runtime)

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Client-Timestamp, X-Idempotency-Key, X-Request-Id',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
}

const ORDER_OP_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const ORDER_BULK_CHUNK_SIZE = 80;
const ORDER_MAX_BULK_ROWS = 500;
const ORDER_OP_PRUNE_INTERVAL_MS = 5 * 60 * 1000;
const DEFAULT_ADMIN_EMAIL = 'joobuyadmin@gmail.com';
let _orderTablesEnsuredAt = 0;
let _lastOrderOpsPruneAt = 0;
let _securityWarnedMissingAuthToken = false;
let _securityTablesEnsuredAt = 0;
let _lastSecurityMemoryPruneAt = 0;
let _lastSecurityDbPruneAt = 0;
const _rateLimitBuckets = new Map();
const _replayProtectionBuckets = new Map();

function safeParseJson(raw, fallback = null) {
  if (raw === null || raw === undefined) return fallback;
  if (typeof raw === 'object') return raw;
  try {
    return JSON.parse(String(raw));
  } catch {
    return fallback;
  }
}

function normalizeOpId(value, fallback = '') {
  const normalized = String(value || '').trim();
  if (normalized) return normalized.slice(0, 190);
  return String(fallback || '').trim().slice(0, 190);
}

function buildFallbackOpId(opType, orderId, v, ts) {
  return `${String(opType || 'op')}:${String(orderId || 'na')}:${Number(v) || 0}:${Number(ts) || 0}`;
}

function logWorker(level, event, details = {}) {
  const payload = {
    event,
    ts: Date.now(),
    ...details
  };
  const line = `[worker] ${JSON.stringify(payload)}`;
  if (level === 'error') console.error(line);
  else if (level === 'warn') console.warn(line);
  else console.log(line);
}

function isRetryableD1Error(err) {
  const text = String(err && err.message ? err.message : err || '').toLowerCase();
  return (
    text.includes('database is locked') ||
    text.includes('database busy') ||
    text.includes('too many requests') ||
    text.includes('temporarily unavailable') ||
    text.includes('network')
  );
}

async function waitMs(ms) {
  await new Promise((resolve) => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

async function withD1Retry(fn, meta = {}) {
  const maxAttempts = Math.max(1, Number(meta.maxAttempts) || 3);
  let attempt = 0;
  while (attempt < maxAttempts) {
    attempt++;
    try {
      const result = await fn();
      if (attempt > 1) {
        logWorker('warn', 'd1_retry_recovered', {
          op: String(meta.op || 'd1_query'),
          attempt,
          maxAttempts,
          orderId: meta.orderId ? String(meta.orderId) : undefined,
          opId: meta.opId ? String(meta.opId) : undefined
        });
      }
      return result;
    } catch (err) {
      const retryable = isRetryableD1Error(err);
      if (!retryable || attempt >= maxAttempts) {
        logWorker('error', 'd1_query_failed', {
          op: String(meta.op || 'd1_query'),
          attempt,
          maxAttempts,
          retryable,
          message: String(err && err.message ? err.message : err || 'unknown'),
          orderId: meta.orderId ? String(meta.orderId) : undefined,
          opId: meta.opId ? String(meta.opId) : undefined
        });
        throw err;
      }
      const delay = Math.min(500, 40 * Math.pow(2, attempt - 1));
      logWorker('warn', 'd1_retry_scheduled', {
        op: String(meta.op || 'd1_query'),
        attempt,
        maxAttempts,
        delayMs: delay,
        message: String(err && err.message ? err.message : err || 'unknown')
      });
      await waitMs(delay);
    }
  }
  throw new Error('d1_retry_exhausted');
}

async function executeD1Statements(db, statements) {
  const list = Array.isArray(statements) ? statements.filter(Boolean) : [];
  if (!list.length) return [];
  if (typeof db.batch === 'function') {
    return await db.batch(list);
  }
  const out = [];
  for (const stmt of list) {
    if (!stmt || typeof stmt.run !== 'function') continue;
    out.push(await stmt.run());
  }
  return out;
}

async function ensureOrderTables(db) {
  const now = Date.now();
  if (now - _orderTablesEnsuredAt < 15000) return;
  await db.prepare("CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, ts INTEGER NOT NULL, data TEXT NOT NULL)").run();
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS order_ops (
      op_id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL,
      op_type TEXT NOT NULL,
      req_v INTEGER NOT NULL,
      req_ts INTEGER NOT NULL,
      ack_v INTEGER NOT NULL,
      status TEXT NOT NULL,
      response TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )
  `).run();
  _orderTablesEnsuredAt = now;
}

async function readOrderOpRecord(db, opId) {
  const id = normalizeOpId(opId);
  if (!id) return null;
  try {
    return await db.prepare('SELECT * FROM order_ops WHERE op_id=?').bind(id).first();
  } catch (_e) {
    return null;
  }
}

async function storeOrderOpRecord(db, row) {
  const opId = normalizeOpId(row && row.opId);
  if (!opId) return;
  const now = Date.now();
  const response = JSON.stringify((row && row.response) || {});
  await db.prepare(`
    INSERT INTO order_ops (op_id,order_id,op_type,req_v,req_ts,ack_v,status,response,created_at)
    VALUES (?,?,?,?,?,?,?,?,?)
    ON CONFLICT(op_id) DO UPDATE SET
      ack_v=excluded.ack_v,
      status=excluded.status,
      response=excluded.response
  `).bind(
    opId,
    String((row && row.orderId) || ''),
    String((row && row.opType) || 'upsert'),
    Number((row && row.reqV) || 0),
    Number((row && row.reqTs) || now),
    Number((row && row.ackV) || 0),
    String((row && row.status) || 'applied'),
    response,
    now
  ).run();
}

async function maybePruneOrderOps(db) {
  const now = Date.now();
  if ((now - _lastOrderOpsPruneAt) < ORDER_OP_PRUNE_INTERVAL_MS) return;
  _lastOrderOpsPruneAt = now;
  try {
    await db.prepare('DELETE FROM order_ops WHERE created_at < ?').bind(now - ORDER_OP_RETENTION_MS).run();
  } catch (_e) {}
}

function parseOrderMutationPayload(payload) {
  const src = payload && typeof payload === 'object' ? payload : {};
  const hasNestedOrder = src.order && typeof src.order === 'object';
  const order = hasNestedOrder ? src.order : (src && typeof src === 'object' ? src : null);
  return {
    order,
    opId: src.opId || src.requestId || (order && (order.opId || order.lastOpId)) || '',
    requestId: src.requestId || '',
    v: Number(src.v),
    ts: Number(src.ts),
  };
}

function validateOrderPayload(order) {
  if (!order || typeof order !== 'object') return 'order missing';
  const id = String(order.id || '').trim();
  if (!id) return 'order id missing';
  if (order.products !== undefined && !Array.isArray(order.products)) return 'order products invalid';
  return '';
}

function toBoundedInt(raw, fallback, min, max) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

function isWriteMethod(method) {
  const m = String(method || '').toUpperCase();
  return m === 'POST' || m === 'PUT' || m === 'PATCH' || m === 'DELETE';
}

function getClientIp(request) {
  const cfIp = String(request.headers.get('CF-Connecting-IP') || '').trim();
  if (cfIp) return cfIp;
  const forwarded = String(request.headers.get('X-Forwarded-For') || '').trim();
  if (!forwarded) return 'unknown';
  return String(forwarded.split(',')[0] || '').trim() || 'unknown';
}

function timingSafeEqual(a, b) {
  const left = String(a || '');
  const right = String(b || '');
  const maxLen = Math.max(left.length, right.length);
  let diff = left.length === right.length ? 0 : 1;
  for (let i = 0; i < maxLen; i++) {
    const l = i < left.length ? left.charCodeAt(i) : 0;
    const r = i < right.length ? right.charCodeAt(i) : 0;
    diff |= (l ^ r);
  }
  return diff === 0;
}

function parseBearerToken(headerValue) {
  const raw = String(headerValue || '').trim();
  if (!raw) return '';
  if (/^bearer\s+/i.test(raw)) return raw.replace(/^bearer\s+/i, '').trim();
  return raw;
}

function getApiAuthSecret(env) {
  return String(env.API_AUTH_TOKEN || env.CLOUD_API_KEY || env.WORKER_SHARED_SECRET || '').trim();
}

function getJwtAuthSecret(env) {
  return String(env.JWT_AUTH_SECRET || env.JWT_SECRET || '').trim();
}

function isSetupEnabled(env) {
  const raw = String(env && env.SETUP_ENABLED !== undefined ? env.SETUP_ENABLED : 'false').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

function getSecretRotationHealth(env) {
  const rotatedAtRaw = Number(
    env.SECRET_ROTATED_AT ||
    env.JWT_SECRET_ROTATED_AT ||
    env.API_SECRET_ROTATED_AT ||
    0
  );
  const rotatedAt = Number.isFinite(rotatedAtRaw) && rotatedAtRaw > 0 ? Math.floor(rotatedAtRaw) : 0;
  const maxAgeMs = toBoundedInt(
    env.SECRET_MAX_AGE_MS,
    90 * 24 * 60 * 60 * 1000,
    24 * 60 * 60 * 1000,
    365 * 24 * 60 * 60 * 1000
  );
  const ageMs = rotatedAt > 0 ? Math.max(0, Date.now() - rotatedAt) : 0;
  return {
    configured: rotatedAt > 0,
    rotatedAt,
    ageMs,
    maxAgeMs,
    stale: rotatedAt > 0 ? ageMs > maxAgeMs : false
  };
}

function getRequestIdFromRequest(request) {
  const fromHeaders = String(
    request.headers.get('X-Request-Id') ||
    request.headers.get('X-Idempotency-Key') ||
    ''
  ).trim();
  if (fromHeaders) return fromHeaders.slice(0, 190);
  return `rq_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function isPublicApiRoute(path, method) {
  const m = String(method || '').toUpperCase();
  if (path === '/api/test') return true;
  if (path.startsWith('/api/image/') && m === 'GET') return true;
  if (path.startsWith('/api/catalog-order/') && m === 'GET') return true;
  if (path === '/api/auth/health' && m === 'GET') return true;
  return false;
}

function getRequiredRolesForRoute(path, method) {
  const m = String(method || '').toUpperCase();
  if (path === '/api/setup' && m === 'GET') return ['Admin'];
  if (path === '/api/ops/health' && m === 'GET') return ['Admin'];
  if (path === '/api/backup' && m === 'GET') return ['Admin'];
  if (path === '/api/backup/import' && m === 'POST') return ['Admin'];
  if (path.startsWith('/api/logistics/') && m !== 'GET') return ['Admin', 'Staff'];
  if (path === '/api/push/send' && m === 'POST') return ['Admin', 'Staff'];
  if (path === '/api/catalog-page' && m === 'POST') return ['Admin', 'Staff'];
  return null;
}

function roleAllowed(requiredRoles, role) {
  if (!Array.isArray(requiredRoles) || !requiredRoles.length) return true;
  const normalized = String(role || '').trim().toLowerCase();
  if (!normalized) return false;
  return requiredRoles.some((r) => String(r || '').trim().toLowerCase() === normalized);
}

function shouldApplyReplayGuard(path, method) {
  if (!isWriteMethod(method)) return false;
  if (path === '/api/order' || path.startsWith('/api/order/')) return false;
  return path.startsWith('/api/');
}

function base64UrlToBytes(segment) {
  const normalized = String(segment || '').replace(/-/g, '+').replace(/_/g, '/');
  const padded = normalized + '='.repeat((4 - (normalized.length % 4 || 4)) % 4);
  const raw = atob(padded);
  const bytes = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) bytes[i] = raw.charCodeAt(i);
  return bytes;
}

function decodeBase64UrlJson(segment) {
  try {
    const decoded = new TextDecoder().decode(base64UrlToBytes(segment));
    return JSON.parse(decoded);
  } catch {
    return null;
  }
}

async function verifyJwtToken(token, secret) {
  const parts = String(token || '').split('.');
  if (parts.length !== 3) return { ok: false, error: 'jwt_format_invalid' };
  const [headerPart, payloadPart, signaturePart] = parts;
  const header = decodeBase64UrlJson(headerPart);
  const payload = decodeBase64UrlJson(payloadPart);
  if (!header || !payload) return { ok: false, error: 'jwt_decode_failed' };
  if (String(header.alg || '').toUpperCase() !== 'HS256') {
    return { ok: false, error: 'jwt_alg_unsupported' };
  }
  const nowSec = Math.floor(Date.now() / 1000);
  if (payload.nbf && Number(payload.nbf) > nowSec) {
    return { ok: false, error: 'jwt_not_active' };
  }
  if (payload.exp && Number(payload.exp) <= nowSec) {
    return { ok: false, error: 'jwt_expired' };
  }
  try {
    const key = await crypto.subtle.importKey(
      'raw',
      new TextEncoder().encode(String(secret || '')),
      { name: 'HMAC', hash: 'SHA-256' },
      false,
      ['verify']
    );
    const verified = await crypto.subtle.verify(
      'HMAC',
      key,
      base64UrlToBytes(signaturePart),
      new TextEncoder().encode(`${headerPart}.${payloadPart}`)
    );
    if (!verified) return { ok: false, error: 'jwt_signature_invalid' };
    return { ok: true, payload, header };
  } catch (_e) {
    return { ok: false, error: 'jwt_verify_failed' };
  }
}

function getRoleFromClaims(claims) {
  if (!claims || typeof claims !== 'object') return '';
  const role = claims.role || claims.r || '';
  if (role) return String(role);
  if (Array.isArray(claims.roles) && claims.roles.length) return String(claims.roles[0] || '');
  return '';
}

function buildAnonymousContext(request) {
  return {
    requestId: getRequestIdFromRequest(request),
    ip: getClientIp(request),
    authMode: 'open',
    isAuthenticated: false,
    role: 'Guest',
    actorId: 'anonymous'
  };
}

async function authenticateRequest(request, env) {
  const context = buildAnonymousContext(request);
  const bearer = parseBearerToken(request.headers.get('Authorization'));
  const sharedSecret = getApiAuthSecret(env);
  const jwtSecret = getJwtAuthSecret(env);

  if (jwtSecret) {
    if (!bearer) return { ok: false, error: 'missing_token', context };
    if (sharedSecret && timingSafeEqual(bearer, sharedSecret)) {
      return {
        ok: true,
        context: {
          ...context,
          authMode: 'shared_token',
          isAuthenticated: true,
          role: 'Admin',
          actorId: 'shared_token_admin'
        }
      };
    }
    const jwt = await verifyJwtToken(bearer, jwtSecret);
    if (!jwt.ok) return { ok: false, error: jwt.error, context };
    const claims = jwt.payload || {};
    const role = getRoleFromClaims(claims) || 'Reseller';
    const actorId = String(claims.sub || claims.uid || claims.email || claims.userId || 'jwt_user');
    return {
      ok: true,
      context: {
        ...context,
        authMode: 'jwt',
        isAuthenticated: true,
        role,
        actorId,
        claims
      }
    };
  }

  if (sharedSecret) {
    if (!bearer || !timingSafeEqual(bearer, sharedSecret)) {
      return { ok: false, error: 'shared_token_invalid', context };
    }
    return {
      ok: true,
      context: {
        ...context,
        authMode: 'shared_token',
        isAuthenticated: true,
        role: 'Admin',
        actorId: 'shared_token_admin'
      }
    };
  }

  return { ok: true, context };
}

function pruneSecurityState(now) {
  if ((now - _lastSecurityMemoryPruneAt) < 60000) return;
  _lastSecurityMemoryPruneAt = now;
  for (const [key, bucket] of _rateLimitBuckets.entries()) {
    if (!bucket || Number(bucket.resetAt) <= now) _rateLimitBuckets.delete(key);
  }
  for (const [key, entry] of _replayProtectionBuckets.entries()) {
    if (!entry || Number(entry.expiresAt) <= now) _replayProtectionBuckets.delete(key);
  }
}

async function ensureSecurityTables(db) {
  const now = Date.now();
  if ((now - _securityTablesEnsuredAt) < 15000) return;
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS api_rate_limits (
      bucket_key TEXT PRIMARY KEY,
      window_start INTEGER NOT NULL,
      hits INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `).run();
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS api_replay_keys (
      replay_key TEXT PRIMARY KEY,
      created_at INTEGER NOT NULL,
      expires_at INTEGER NOT NULL
    )
  `).run();
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS api_metrics (
      metric_key TEXT PRIMARY KEY,
      count INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `).run();
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS order_audit_events (
      event_id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      actor_id TEXT NOT NULL,
      actor_role TEXT NOT NULL,
      request_id TEXT NOT NULL,
      payload TEXT NOT NULL,
      created_at INTEGER NOT NULL
    )
  `).run();
  _securityTablesEnsuredAt = now;
}

async function maybePruneSecurityTables(db) {
  const now = Date.now();
  if ((now - _lastSecurityDbPruneAt) < 60000) return;
  _lastSecurityDbPruneAt = now;
  try {
    await db.prepare('DELETE FROM api_replay_keys WHERE expires_at < ?').bind(now).run();
    await db.prepare('DELETE FROM api_rate_limits WHERE updated_at < ?').bind(now - (24 * 60 * 60 * 1000)).run();
    await db.prepare('DELETE FROM api_metrics WHERE updated_at < ?').bind(now - (14 * 24 * 60 * 60 * 1000)).run();
    await db.prepare('DELETE FROM order_audit_events WHERE created_at < ?').bind(now - (30 * 24 * 60 * 60 * 1000)).run();
  } catch (_e) {}
}

async function bumpSecurityMetric(db, name, delta = 1) {
  if (!db || !name) return;
  try {
    const minuteBucket = Math.floor(Date.now() / 60000);
    const metricKey = `${String(name)}:${minuteBucket}`;
    await db.prepare(`
      INSERT INTO api_metrics (metric_key,count,updated_at)
      VALUES (?,?,?)
      ON CONFLICT(metric_key) DO UPDATE SET
        count=count+excluded.count,
        updated_at=excluded.updated_at
    `).bind(metricKey, Math.max(1, Number(delta) || 1), Date.now()).run();
  } catch (_e) {}
}

async function enforceRateLimit(request, env, path, method, context = {}) {
  if (!isWriteMethod(method)) return null;
  const now = Date.now();
  const windowMs = toBoundedInt(env.API_RATE_LIMIT_WINDOW_MS, 60000, 1000, 10 * 60 * 1000);
  const maxHits = toBoundedInt(env.API_RATE_LIMIT_MAX, 240, 10, 10000);
  const ip = context.ip || getClientIp(request);
  const db = env && env.DB ? env.DB : null;

  if (db) {
    await ensureSecurityTables(db);
    await maybePruneSecurityTables(db);
    const windowStart = Math.floor(now / windowMs) * windowMs;
    const bucketKey = `${ip}:write:${windowStart}`;
    let hits = 1;
    const row = await db.prepare('SELECT hits FROM api_rate_limits WHERE bucket_key=?').bind(bucketKey).first();
    if (row && row.hits !== undefined) {
      hits = Math.max(0, Number(row.hits) || 0) + 1;
      await db.prepare('UPDATE api_rate_limits SET hits=?, updated_at=? WHERE bucket_key=?')
        .bind(hits, now, bucketKey)
        .run();
    } else {
      await db.prepare('INSERT INTO api_rate_limits (bucket_key,window_start,hits,updated_at) VALUES (?,?,?,?)')
        .bind(bucketKey, windowStart, hits, now)
        .run();
    }
    if (hits <= maxHits) return null;
    const retryAfterMs = Math.max(0, (windowStart + windowMs) - now);
    await bumpSecurityMetric(db, 'rate_limit_blocked', 1);
    logWorker('warn', 'rate_limit_blocked', {
      path,
      method: String(method || '').toUpperCase(),
      ip,
      retryAfterMs,
      maxHits,
      windowMs
    });
    return json({ error: 'rate_limited', retryAfterMs, limit: maxHits, windowMs }, 429);
  }

  pruneSecurityState(now);
  const key = `${ip}:write`;
  const prev = _rateLimitBuckets.get(key);
  const bucket = (!prev || Number(prev.resetAt) <= now) ? { count: 0, resetAt: now + windowMs } : prev;
  bucket.count += 1;
  _rateLimitBuckets.set(key, bucket);
  if (bucket.count <= maxHits) return null;
  const retryAfterMs = Math.max(0, Number(bucket.resetAt) - now);
  logWorker('warn', 'rate_limit_blocked', { path, method: String(method || '').toUpperCase(), ip, retryAfterMs, maxHits, windowMs });
  return json({ error: 'rate_limited', retryAfterMs, limit: maxHits, windowMs }, 429);
}

async function enforceReplayGuard(request, env, path, method, context = {}) {
  if (!shouldApplyReplayGuard(path, method)) return null;
  const now = Date.now();
  const skewMs = toBoundedInt(env.REPLAY_MAX_SKEW_MS, 5 * 60 * 1000, 60 * 1000, 60 * 60 * 1000);
  const tsRaw = String(
    request.headers.get('X-Client-Timestamp') ||
    request.headers.get('X-Request-Timestamp') ||
    ''
  ).trim();
  if (tsRaw) {
    const ts = Number(tsRaw);
    if (!Number.isFinite(ts) || ts <= 0) {
      return json({ error: 'invalid_client_timestamp' }, 400);
    }
    if (Math.abs(now - ts) > skewMs) {
      return json({
        error: 'stale_request',
        maxSkewMs: skewMs
      }, 409);
    }
  }

  const rawKey = String(request.headers.get('X-Idempotency-Key') || request.headers.get('X-Request-Id') || '').trim();
  if (!rawKey) return null;
  const replayKey = rawKey.slice(0, 190);
  const replayTtlMs = toBoundedInt(env.REPLAY_CACHE_MS, 10 * 60 * 1000, 60 * 1000, 24 * 60 * 60 * 1000);
  const ip = context.ip || getClientIp(request);
  const key = `${ip}:${String(method || '').toUpperCase()}:${path}:${replayKey}`;
  const db = env && env.DB ? env.DB : null;

  if (db) {
    await ensureSecurityTables(db);
    await maybePruneSecurityTables(db);
    const knownRow = await db.prepare('SELECT expires_at FROM api_replay_keys WHERE replay_key=?').bind(key).first();
    if (knownRow && Number(knownRow.expires_at) > now) {
      await bumpSecurityMetric(db, 'replay_blocked', 1);
      logWorker('warn', 'replay_blocked', {
        path,
        method: String(method || '').toUpperCase(),
        ip,
        replayKey
      });
      return json({ error: 'replay_detected', replay: true }, 409);
    }
    await db.prepare(`
      INSERT INTO api_replay_keys (replay_key,created_at,expires_at)
      VALUES (?,?,?)
      ON CONFLICT(replay_key) DO UPDATE SET
        created_at=excluded.created_at,
        expires_at=excluded.expires_at
    `).bind(key, now, now + replayTtlMs).run();
    return null;
  }

  pruneSecurityState(now);
  const known = _replayProtectionBuckets.get(key);
  if (known && Number(known.expiresAt) > now) {
    logWorker('warn', 'replay_blocked', {
      path,
      method: String(method || '').toUpperCase(),
      ip,
      replayKey
    });
    return json({ error: 'replay_detected', replay: true }, 409);
  }
  _replayProtectionBuckets.set(key, { expiresAt: now + replayTtlMs });
  return null;
}

async function enforceEndpointSecurity(request, env, path, method) {
  const context = buildAnonymousContext(request);
  if (!path.startsWith('/api/')) return { response: null, context };
  if (isPublicApiRoute(path, method)) return { response: null, context };
  if (path === '/api/setup' && String(method || '').toUpperCase() === 'GET' && !isSetupEnabled(env)) {
    const db = env && env.DB ? env.DB : null;
    if (db) await bumpSecurityMetric(db, 'setup_blocked', 1);
    return { response: json({ error: 'setup_disabled' }, 403), context };
  }

  const auth = await authenticateRequest(request, env);
  const db = env && env.DB ? env.DB : null;
  if (!auth.ok) {
    if (db) await bumpSecurityMetric(db, 'auth_rejected', 1);
    logWorker('warn', 'auth_rejected', {
      path,
      method: String(method || '').toUpperCase(),
      ip: context.ip,
      reason: auth.error
    });
    return { response: json({ error: 'unauthorized' }, 401), context };
  }

  const authContext = auth.context || context;
  const requiredRoles = getRequiredRolesForRoute(path, method);
  if (requiredRoles && authContext.authMode === 'open') {
    logWorker('error', 'role_guard_auth_not_configured', {
      path,
      method: String(method || '').toUpperCase(),
      requiredRoles
    });
    return { response: json({ error: 'auth_required_for_role_protected_route' }, 503), context: authContext };
  }
  if (requiredRoles && !roleAllowed(requiredRoles, authContext.role)) {
    if (db) await bumpSecurityMetric(db, 'role_forbidden', 1);
    logWorker('warn', 'role_forbidden', {
      path,
      method: String(method || '').toUpperCase(),
      actorId: authContext.actorId,
      role: authContext.role,
      requiredRoles
    });
    return { response: json({ error: 'forbidden', requiredRoles }, 403), context: authContext };
  }

  if (authContext.authMode === 'open' && !_securityWarnedMissingAuthToken) {
    _securityWarnedMissingAuthToken = true;
    logWorker('warn', 'auth_secret_missing', {
      hint: 'Set JWT_AUTH_SECRET and/or API_AUTH_TOKEN in Worker env to enforce endpoint auth'
    });
  }
  const limited = await enforceRateLimit(request, env, path, method, authContext);
  if (limited) return { response: limited, context: authContext };
  const replay = await enforceReplayGuard(request, env, path, method, authContext);
  if (replay) return { response: replay, context: authContext };
  if (db) await bumpSecurityMetric(db, 'auth_ok', 1);
  return { response: null, context: authContext };
}

function buildDefaultAdminUser(env) {
  const email = String(env.DEFAULT_ADMIN_EMAIL || DEFAULT_ADMIN_EMAIL).trim();
  const password = String(env.DEFAULT_ADMIN_PASSWORD || '').trim();
  if (!email || !password) return null;
  return {
    id: 'u1',
    email,
    password,
    role: 'Admin',
    dname: 'Admin'
  };
}

function getLogisticsConfig(env) {
  return {
    baseUrl: String(env.HUALEI_BASE_URL || '').trim().replace(/\/+$/, ''),
    username: String(env.HUALEI_USERNAME || '').trim(),
    password: String(env.HUALEI_PASSWORD || '').trim(),
    labelBaseUrl: String(env.HUALEI_LABEL_BASE_URL || '').trim().replace(/\/+$/, '')
  };
}

function resolveLogisticsLabelBase(config) {
  if (config && config.labelBaseUrl) return config.labelBaseUrl;
  const base = String(config && config.baseUrl || '').trim();
  if (!base) return '';
  if (/:8082$/i.test(base)) return base.replace(/:8082$/i, ':8089');
  return base;
}

function getLogisticsConfigError(config, opts = {}) {
  if (!config || !config.baseUrl || !config.username || !config.password) {
    return 'logistics_not_configured';
  }
  if (opts.requireLabelBase) {
    const labelBase = resolveLogisticsLabelBase(config);
    if (!labelBase) return 'logistics_label_not_configured';
  }
  return '';
}

function normalizeUsersForStore(raw) {
  let list = raw;
  if (list === null || list === undefined) list = [];
  if (!Array.isArray(list) && list && typeof list === 'object') {
    list = Object.values(list).filter(v => v && typeof v === 'object');
  }
  if (!Array.isArray(list)) list = [];
  return list
    .filter(u => u && typeof u === 'object')
    .map(u => ({
      ...u,
      id: String(u.id || ('u' + Date.now() + Math.random().toString(36).slice(2, 7))),
      email: String(u.email || '').trim(),
      password: String(u.password || u.pw || u.pass || u.userPassword || ''),
      role: String(u.role || 'Reseller'),
      dname: String(u.dname || '').trim(),
    }))
    .filter(u => u.email && u.password);
}

function userIdentityKey(u) {
  const email = String(u && u.email || '').trim().toLowerCase();
  if (email) return 'em:' + email;
  return 'id:' + String(u && u.id || '');
}

function mergeUsersConservative(existingRaw, incomingRaw) {
  const existing = normalizeUsersForStore(existingRaw);
  const incoming = normalizeUsersForStore(incomingRaw);
  const byKey = new Map();
  for (const u of existing) {
    byKey.set(userIdentityKey(u), u);
  }
  for (const u of incoming) {
    const key = userIdentityKey(u);
    const prev = byKey.get(key) || {};
    byKey.set(key, { ...prev, ...u });
  }
  const merged = normalizeUsersForStore([...byKey.values()]);
  // Safety: ignore suspicious admin-only snapshots when we already have richer user data.
  if (
    existing.length >= 2 &&
    incoming.length === 1 &&
    String(incoming[0] && incoming[0].email || '').toLowerCase() === String(DEFAULT_ADMIN_EMAIL).toLowerCase()
  ) {
    return existing;
  }
  return merged;
}

async function readUsersFromStore(db) {
  const row = await db.prepare("SELECT value FROM kv_store WHERE key='jb_users'").first();
  if (!row || !row.value) return [];
  try {
    return normalizeUsersForStore(JSON.parse(row.value));
  } catch {
    return [];
  }
}

export default {
  async fetch(request, env) {
    if (request.method === 'OPTIONS') return new Response(null, { headers: CORS });

    const url = new URL(request.url);
    const path = url.pathname;
    const method = request.method;

    // Public endpoints
    if (path === '/api/test') {
      const hasDbBinding = !!env.DB;
      let dbReady = false;
      if (hasDbBinding) {
        try {
          await env.DB.prepare('SELECT 1 as ok').first();
          dbReady = true;
        } catch (_e) {
          dbReady = false;
        }
      }
      return json({
        ok: hasDbBinding && dbReady,
        message: hasDbBinding
          ? (dbReady ? 'Worker + DB hazır' : 'DB sorgu hatası')
          : 'DB binding eksik (Functions > Bindings > D1 Database name: DB)',
        hasDbBinding,
        dbReady,
        ts: Date.now()
      }, hasDbBinding && dbReady ? 200 : 503);
    }
    if (path === '/api/auth/health' && method === 'GET') {
      const jwtEnabled = !!getJwtAuthSecret(env);
      const sharedEnabled = !!getApiAuthSecret(env);
      const rotation = getSecretRotationHealth(env);
      return json({
        ok: true,
        authMode: jwtEnabled ? 'jwt_or_shared' : (sharedEnabled ? 'shared_token' : 'open'),
        setupEnabled: isSetupEnabled(env),
        secrets: {
          jwtConfigured: jwtEnabled,
          sharedTokenConfigured: sharedEnabled,
          rotation
        },
        ts: Date.now()
      });
    }
    if (path === '/catalog' && method === 'GET') return await getCatalog(env);
    if (path.startsWith('/api/image/') && method === 'GET') return await getImage(env, path.replace('/api/image/', ''));

    const { response: securityFailure, context: authContext } = await enforceEndpointSecurity(request, env, path, method);
    if (securityFailure) return securityFailure;

    // Setup
    if (path === '/api/setup' && method === 'GET') {
      try {
        const db = env.DB;
        // Tablolar
        await db.prepare('CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL)').run();
        await db.prepare('CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, ts INTEGER NOT NULL, data TEXT NOT NULL)').run();
        // Admin kullanıcısı ekle (mevcut kullanıcı listesini EZME)
        const adminUser = buildDefaultAdminUser(env);
        const currentUsersRow = await db.prepare("SELECT value FROM kv_store WHERE key='jb_users'").first();
        let users = [];
        if (currentUsersRow && currentUsersRow.value) {
          try {
            users = JSON.parse(currentUsersRow.value) || [];
          } catch {
            users = [];
          }
        }
        if (!Array.isArray(users)) users = [];
        let adminCreated = false;
        if (adminUser) {
          const hasAdmin = users.some(u => u && String(u.email || '').toLowerCase() === String(adminUser.email).toLowerCase());
          if (!hasAdmin) {
            users.unshift(adminUser);
            adminCreated = true;
          }
        } else {
          logWorker('warn', 'setup_admin_skipped', {
            reason: 'DEFAULT_ADMIN_PASSWORD missing',
            hint: 'Set DEFAULT_ADMIN_PASSWORD in Worker env before running /api/setup'
          });
        }
        await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
          .bind('jb_users', JSON.stringify(users)).run();
        return json({
          ok: true,
          message: adminUser
            ? 'Setup tamam! Kullanıcı listesi korundu.'
            : 'Setup tamam! Admin bootstrap skipped (DEFAULT_ADMIN_PASSWORD missing).',
          usersCount: users.length,
          adminCreated
        });
      } catch(e) { return json({ error: e.message }, 500); }
    }

    try {
      const db = env.DB;

      // Data
      if (path === '/api/data' && method === 'GET') return await getData(db);
      if (path === '/api/data' && method === 'POST') {
        const body = await request.json();
        const keys = Object.keys(body);
        for(const key of keys){
          if(key === 'jb_orders') continue; // orders ayrı endpoint'te
          await syncKey(db, key, body[key]);
        }
        return json({ ok: true, saved: keys.length });
      }
      if (path === '/api/sync' && method === 'POST') {
        const { key, value } = await request.json();
        return await syncKey(db, key, value);
      }

      // Orders
      if (path === '/api/orders' && method === 'GET') {
        const sinceTs = Math.max(0, Number(url.searchParams.get('sinceTs')) || 0);
        const sinceV = Math.max(0, Number(url.searchParams.get('sinceV')) || 0);
        return await getOrders(db, authContext, { sinceTs, sinceV });
      }
      if ((path === '/api/backup/import' || path === '/api/orders/bulk') && method === 'POST') {
        const body = await request.json();
        return await saveOrdersBulk(db, body, authContext);
      }
      if (path === '/api/order' && method === 'POST') {
        const payload = await request.json();
        return await saveOrder(db, payload, authContext);
      }
      if (path === '/api/order-event' && method === 'POST') {
        const event = await request.json();
        return await saveOrderEvent(db, event, authContext);
      }
      if (path.startsWith('/api/order/') && method === 'DELETE') {
        const id = decodeURIComponent(path.split('/').pop() || '').trim();
        if (!id) return json({ error: 'id missing' }, 400);
        let req = {};
        try { req = await request.json(); } catch { req = {}; }
        const nowTs = Date.now();
        const requestedDeletedAt = Number(req && req.deletedAt) || nowTs;
        const requestedV = Number(req && req.v) || 0;
        const requestedOpId = String(req && req.opId || '').trim() || (`del_${id}_${requestedV || 0}_${requestedDeletedAt}`);

        await ensureOrderTables(db);
        const knownOp = await readOrderOpRecord(db, requestedOpId);
        if (knownOp) {
          const cached = safeParseJson(knownOp.response, null);
          if (cached && typeof cached === 'object') {
            return json({ ...cached, idempotent: true });
          }
        }

        const prevTableOrder = await readOrderFromTable(db, id);
        const prevKvOrder = await readOrderFromKvSnapshot(db, id);
        const prevOrder = pickFresherOrder(prevTableOrder, prevKvOrder);
        const prevV = Number(prevOrder && prevOrder.v) || 0;
        const prevTs = Number(prevOrder && (prevOrder.updatedAt || prevOrder.ts)) || 0;
        const prevDeleted = !!(prevOrder && prevOrder.deletedAt);

        const nextV = Math.max(1, requestedV, prevV + 1);
        const deletedAt = Math.max(requestedDeletedAt, prevTs, nowTs);

        // Idempotency/stale protection: keep the freshest tombstone.
        if (prevDeleted && (prevV > nextV || (prevV === nextV && prevTs >= deletedAt))) {
          const staleResponse = {
            ok: true,
            staleIgnored: true,
            ackV: prevV,
            deletedAt: prevTs,
            conflict: { type: 'stale_delete', serverV: prevV, serverTs: prevTs }
          };
          await storeOrderOpRecord(db, {
            opId: requestedOpId,
            orderId: id,
            opType: 'delete',
            reqV: nextV,
            reqTs: deletedAt,
            ackV: prevV || nextV,
            status: 'stale',
            response: staleResponse
          });
          await recordOrderAuditEvent(db, 'order_delete_stale', id, {
            opId: requestedOpId,
            reqV: nextV,
            reqTs: deletedAt,
            serverV: prevV,
            serverTs: prevTs
          }, authContext);
          return json(staleResponse);
        }

        const tombstone = {
          id,
          status: 'Deleted',
          deletedAt,
          updatedAt: deletedAt,
          ts: deletedAt,
          v: nextV,
          opId: requestedOpId,
          lastOpId: requestedOpId,
          products: []
        };
        const writeRes = await db.prepare(`
          INSERT INTO orders (id,ts,data) VALUES (?,?,?)
          ON CONFLICT(id) DO UPDATE SET data=excluded.data, ts=excluded.ts
          WHERE
            COALESCE(CAST(json_extract(orders.data,'$.v') AS INTEGER),0) < ?
            OR (
              COALESCE(CAST(json_extract(orders.data,'$.v') AS INTEGER),0) = ?
              AND COALESCE(
                CAST(json_extract(orders.data,'$.updatedAt') AS INTEGER),
                COALESCE(CAST(json_extract(orders.data,'$.ts') AS INTEGER),0)
              ) <= ?
            )
        `).bind(id, deletedAt, JSON.stringify(tombstone), nextV, nextV, deletedAt).run();
        const changed = Number(writeRes && writeRes.meta && writeRes.meta.changes) || 0;
        if (!changed) {
          const latest = pickFresherOrder(await readOrderFromTable(db, id), await readOrderFromKvSnapshot(db, id));
          const latestV = Number(latest && latest.v) || prevV || nextV;
          const latestTs = Number(latest && (latest.updatedAt || latest.ts)) || deletedAt;
          const staleResponse = {
            ok: true,
            staleIgnored: true,
            ackV: latestV,
            deletedAt: latestTs,
            conflict: { type: 'concurrent_delete_conflict', serverV: latestV, serverTs: latestTs }
          };
          await storeOrderOpRecord(db, {
            opId: requestedOpId,
            orderId: id,
            opType: 'delete',
            reqV: nextV,
            reqTs: deletedAt,
            ackV: latestV,
            status: 'stale',
            response: staleResponse
          });
          await recordOrderAuditEvent(db, 'order_delete_conflict', id, {
            opId: requestedOpId,
            reqV: nextV,
            reqTs: deletedAt,
            serverV: latestV,
            serverTs: latestTs
          }, authContext);
          return json(staleResponse);
        }
        const successResponse = { ok: true, deletedAt, ackV: nextV };
        await storeOrderOpRecord(db, {
          opId: requestedOpId,
          orderId: id,
          opType: 'delete',
          reqV: nextV,
          reqTs: deletedAt,
          ackV: nextV,
          status: 'applied',
          response: successResponse
        });
        await recordOrderAuditEvent(db, 'order_tombstone_applied', id, {
          opId: requestedOpId,
          ackV: nextV,
          deletedAt
        }, authContext);
        await maybePruneOrderOps(db);
        return json(successResponse);
      }

      // Images
      if (path === '/api/image' && method === 'POST') {
        const { data, type } = await request.json();
        return await saveImage(db, data, type || 'image/jpeg');
      }

      // Catalog
      if (path === '/api/catalog-page' && method === 'POST') {
        const { html } = await request.json();
        await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
          .bind('catalog_html', JSON.stringify(html)).run();
        return json({ ok: true });
      }
      if (path === '/api/catalog-order' && method === 'POST') {
        const order = await request.json();
        return await saveCatalogOrder(db, order);
      }
      if (path.startsWith('/api/catalog-order/') && method === 'GET') {
        const code = decodeURIComponent(path.replace('/api/catalog-order/', ''));
        return await getCatalogOrder(db, code);
      }

      // Backup
      if (path === '/api/backup' && method === 'GET') {
        const kv = await db.prepare('SELECT * FROM kv_store').all();
        const orders = await db.prepare('SELECT * FROM orders').all();
        return json({ ok: true, kv: kv.results, orders: orders.results, ts: Date.now() });
      }
      if (path === '/api/ops/health' && method === 'GET') {
        await ensureSecurityTables(db);
        await maybePruneSecurityTables(db);
        const sinceMs = Math.max(60 * 1000, toBoundedInt(url.searchParams.get('sinceMs'), 5 * 60 * 1000, 60 * 1000, 24 * 60 * 60 * 1000));
        const minTs = Date.now() - sinceMs;
        const secretRotation = getSecretRotationHealth(env);
        const stats = await db.prepare(`
          SELECT
            SUM(CASE WHEN metric_key LIKE 'auth_ok:%' THEN count ELSE 0 END) AS auth_ok,
            SUM(CASE WHEN metric_key LIKE 'auth_rejected:%' THEN count ELSE 0 END) AS auth_rejected,
            SUM(CASE WHEN metric_key LIKE 'rate_limit_blocked:%' THEN count ELSE 0 END) AS rate_limit_blocked,
            SUM(CASE WHEN metric_key LIKE 'replay_blocked:%' THEN count ELSE 0 END) AS replay_blocked,
            SUM(CASE WHEN metric_key LIKE 'setup_blocked:%' THEN count ELSE 0 END) AS setup_blocked
          FROM api_metrics
          WHERE updated_at >= ?
        `).bind(minTs).first();
        const outbox = await db.prepare(`
          SELECT
            COUNT(*) AS pending_count,
            MIN(created_at) AS oldest_created_at
          FROM order_ops
          WHERE status!='applied'
        `).first();
        const lastAudit = await db.prepare(`
          SELECT event_type,order_id,actor_role,created_at
          FROM order_audit_events
          ORDER BY created_at DESC
          LIMIT 1
        `).first();
        return json({
          ok: true,
          ts: Date.now(),
          authMode: getJwtAuthSecret(env) ? 'jwt_or_shared' : (getApiAuthSecret(env) ? 'shared_token' : 'open'),
          setupEnabled: isSetupEnabled(env),
          secretRotation,
          windowMs: sinceMs,
          metrics: {
            authOk: Number(stats && stats.auth_ok) || 0,
            authRejected: Number(stats && stats.auth_rejected) || 0,
            rateLimitBlocked: Number(stats && stats.rate_limit_blocked) || 0,
            replayBlocked: Number(stats && stats.replay_blocked) || 0,
            setupBlocked: Number(stats && stats.setup_blocked) || 0
          },
          outbox: {
            pending: Number(outbox && outbox.pending_count) || 0,
            oldestCreatedAt: Number(outbox && outbox.oldest_created_at) || 0
          },
          audit: lastAudit || null
        });
      }

      // Push
      if (path === '/api/push/subscribe' && method === 'POST') {
        const { userId, endpoint, auth: a, p256dh } = await request.json();
        await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
          .bind('push_' + userId, JSON.stringify({ endpoint, auth: a, p256dh })).run();
        return json({ ok: true });
      }
      if (path === '/api/push/send' && method === 'POST') {
        return json({ ok: true, queued: true });
      }

      // Anthropic
      if (path === '/api/anthropic' && method === 'POST') {
        const body = await request.text();
        const res = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': env.ANTHROPIC_KEY || '', 'anthropic-version': '2023-06-01' },
          body,
        });
        return json(await res.json(), res.status);
      }

      // Logistics
      if (path === '/api/logistics/auth' && method === 'POST') {
        const logistics = getLogisticsConfig(env);
        const configErr = getLogisticsConfigError(logistics);
        if (configErr) return json({ ok: false, error: configErr }, 503);
        try {
          const res = await fetch(`${logistics.baseUrl}/selectAuth.htm`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ username: logistics.username, password: logistics.password }),
          });
          const text = await res.text();
          let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }
          return json({ ok: true, data });
        } catch(e) { return json({ ok: false, error: e.message }); }
      }

      if (path === '/api/logistics/create' && method === 'POST') {
        const logistics = getLogisticsConfig(env);
        const configErr = getLogisticsConfigError(logistics);
        if (configErr) return json({ ok: false, error: configErr }, 503);
        const { order } = await request.json();
        try {
          const params = new URLSearchParams({
            username: logistics.username, password: logistics.password,
            receiverName: order.name || '',
            receiverPhone: order.phone || '',
            receiverAddress: [order.address, order.city, order.postcode].filter(Boolean).join(', '),
            receiverCountry: 'GB',
            receiverZip: order.postcode || '',
            goodsName: (order.products || []).map(p => p.name).join(', ').slice(0, 100) || 'General Goods',
            goodsQty: String((order.products || []).reduce((s, p) => s + (parseInt(p.qty) || 1), 0) || 1),
            goodsWeight: '0.5',
            referenceNo: order.ref || order.id,
          });
          const res = await fetch(`${logistics.baseUrl}/createOrderApi.htm`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params,
          });
          const text = await res.text();
          let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }
          const trackingNo = data.trackingNo || data.waybillNo || data.mailNo || data.no || '';
          return json({ ok: true, trackingNo, data });
        } catch(e) { return json({ ok: false, error: e.message }); }
      }

      if (path === '/api/logistics/label-url' && method === 'GET') {
        const logistics = getLogisticsConfig(env);
        const configErr = getLogisticsConfigError(logistics, { requireLabelBase: true });
        if (configErr) return json({ ok: false, error: configErr }, 503);
        const no = url.searchParams.get('no') || '';
        const labelBase = resolveLogisticsLabelBase(logistics);
        return Response.redirect(`${labelBase}/order/FastRpt/PDF_NEW.aspx?no=${encodeURIComponent(no)}`, 302);
      }

      if (path === '/api/logistics/track' && method === 'POST') {
        const logistics = getLogisticsConfig(env);
        const configErr = getLogisticsConfigError(logistics);
        if (configErr) return json({ ok: false, error: configErr }, 503);
        const { trackingNo } = await request.json();
        try {
          const res = await fetch(`${logistics.baseUrl}/trackOrder.htm?no=${encodeURIComponent(trackingNo)}&username=${encodeURIComponent(logistics.username)}&password=${encodeURIComponent(logistics.password)}`);
          const text = await res.text();
          let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }
          return json({ ok: true, data });
        } catch(e) { return json({ ok: false, error: e.message }); }
      }

      return json({ error: 'Not found' }, 404);
    } catch(e) {
      return json({ error: e.message }, 500);
    }
  }
};

async function getData(db) {
  const rows = await db.prepare('SELECT key, value FROM kv_store').all();
  const data = {};
  for (const row of rows.results) {
    try { data[row.key] = JSON.parse(row.value); } catch { data[row.key] = row.value; }
  }
  // If jb_users was accidentally overwritten by a tiny snapshot, recover from backup key.
  if (Array.isArray(data.jb_users_backup) && data.jb_users_backup.length > (Array.isArray(data.jb_users) ? data.jb_users.length : 0)) {
    data.jb_users = data.jb_users_backup;
  }
  return json({ data });
}

async function syncKey(db, key, value) {
  if (key === 'jb_users') {
    const existingUsers = await readUsersFromStore(db);
    const nextUsers = mergeUsersConservative(existingUsers, value);
    await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
      .bind('jb_users', JSON.stringify(nextUsers)).run();
    const backupRow = await db.prepare("SELECT value FROM kv_store WHERE key='jb_users_backup'").first();
    let backupUsers = [];
    if (backupRow && backupRow.value) {
      try { backupUsers = normalizeUsersForStore(JSON.parse(backupRow.value)); } catch { backupUsers = []; }
    }
    if (nextUsers.length >= backupUsers.length) {
      await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
        .bind('jb_users_backup', JSON.stringify(nextUsers)).run();
    }
    return json({ ok: true, users: nextUsers.length });
  }
  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind(key, JSON.stringify(value)).run();
  return json({ ok: true });
}

async function readOrderFromTable(db, id) {
  const row = await db.prepare('SELECT data FROM orders WHERE id=?').bind(id).first();
  if (!row || !row.data) return null;
  try { return JSON.parse(row.data); } catch { return null; }
}

function sanitizeOrderForStorage(order) {
  if (!order || typeof order !== 'object') return null;
  const keepImageRef = (src) => {
    const v = String(src || '').trim();
    if (!v) return null;
    if (/^\/?api\/image\/[a-z0-9_-]+$/i.test(v)) return v.startsWith('/') ? v : '/' + v;
    return /^(https?:|data:|blob:)/i.test(v) ? v : null;
  };
  const id = String(order.id || '').trim();
  if (!id) return null;
  const ts = Number(order.updatedAt || order.ts) || Date.now();
  return {
    ...order,
    id,
    updatedAt: ts,
    ts: Number(order.ts) || ts,
    v: Math.max(1, Number(order.v) || 1),
    products: (order.products || []).map((p) => ({
      ...p,
      img: keepImageRef(p && p.img),
      depotPhoto: keepImageRef(p && p.depotPhoto),
    })),
  };
}

function toAuditActorId(authContext) {
  const actor = authContext && authContext.actorId ? authContext.actorId : '';
  return String(actor || 'system');
}

function toAuditRole(authContext) {
  const role = authContext && authContext.role ? authContext.role : '';
  return String(role || 'System');
}

async function recordOrderAuditEvent(db, eventType, orderId, payload, authContext) {
  if (!db || !eventType || !orderId) return;
  try {
    await ensureSecurityTables(db);
    const now = Date.now();
    const requestId = String(
      (authContext && authContext.requestId) ||
      payload && payload.requestId ||
      `rq_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`
    ).slice(0, 190);
    const eventId = `audit_${String(orderId).slice(0, 80)}_${String(eventType).slice(0, 40)}_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
    await db.prepare(`
      INSERT INTO order_audit_events (event_id,order_id,event_type,actor_id,actor_role,request_id,payload,created_at)
      VALUES (?,?,?,?,?,?,?,?)
    `).bind(
      eventId,
      String(orderId).slice(0, 190),
      String(eventType).slice(0, 80),
      toAuditActorId(authContext).slice(0, 190),
      toAuditRole(authContext).slice(0, 80),
      requestId,
      JSON.stringify(payload && typeof payload === 'object' ? payload : {}),
      now
    ).run();
  } catch (_e) {}
}

async function saveOrdersBulk(db, body, authContext = null) {
  const startedAt = Date.now();
  const list = Array.isArray(body && body.orders) ? body.orders : [];
  if (!list.length) return json({ error: 'orders missing' }, 400);
  if (list.length > ORDER_MAX_BULK_ROWS) {
    return json({ error: 'orders too many', max: ORDER_MAX_BULK_ROWS }, 413);
  }
  await ensureOrderTables(db);

  const keyMap = {
    jb_users: body && body.users,
    jb_suppliers: body && body.suppliers,
    jb_catalog: body && body.catalog,
    jb_templates: body && body.templates,
    jb_settings: body && body.settings,
    jb_notif: body && body.notifSettings,
  };
  for (const [key, value] of Object.entries(keyMap)) {
    if (value === undefined) continue;
    await withD1Retry(() => syncKey(db, key, value), { op: 'sync_key_bulk', orderId: key });
  }

  const byId = new Map();
  for (const raw of list) {
    const clean = sanitizeOrderForStorage(raw);
    if (!clean) continue;
    byId.set(clean.id, clean);
  }
  const rows = [...byId.values()];
  if (!rows.length) return json({ error: 'no_valid_orders' }, 400);

  const requestId = normalizeOpId(
    body && (body.requestId || body.opId),
    `bulk_upsert:${rows.length}:${Date.now()}`
  );
  const knownBulk = await readOrderOpRecord(db, requestId);
  if (knownBulk) {
    const cached = safeParseJson(knownBulk.response, null);
    if (cached && typeof cached === 'object') {
      return json({ ...cached, idempotent: true });
    }
  }

  const upsertSql = `
    INSERT INTO orders (id,ts,data) VALUES (?,?,?)
    ON CONFLICT(id) DO UPDATE SET data=excluded.data, ts=excluded.ts
    WHERE
      COALESCE(CAST(json_extract(orders.data,'$.v') AS INTEGER),0) < ?
      OR (
        COALESCE(CAST(json_extract(orders.data,'$.v') AS INTEGER),0) = ?
        AND COALESCE(
          CAST(json_extract(orders.data,'$.updatedAt') AS INTEGER),
          COALESCE(CAST(json_extract(orders.data,'$.ts') AS INTEGER),0)
        ) <= ?
      )
  `;

  let saved = 0;
  let staleIgnored = 0;
  for (let i = 0; i < rows.length; i += ORDER_BULK_CHUNK_SIZE) {
    const chunk = rows.slice(i, i + ORDER_BULK_CHUNK_SIZE);
    const stmts = chunk.map((order) => {
      const ts = Number(order.updatedAt || order.ts) || Date.now();
      const v = Math.max(1, Number(order.v) || 1);
      const row = {
        ...order,
        updatedAt: ts,
        ts: Number(order.ts) || ts,
        v
      };
      return db.prepare(upsertSql).bind(order.id, row.ts, JSON.stringify(row), v, v, ts);
    });

    const results = await withD1Retry(
      () => executeD1Statements(db, stmts),
      { op: 'bulk_upsert_batch', opId: requestId }
    );

    for (const r of (Array.isArray(results) ? results : [])) {
      const changed = Number(r && r.meta && r.meta.changes) || 0;
      if (changed > 0) saved++;
      else staleIgnored++;
    }
  }

  try {
    await withD1Retry(
      () => db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
        .bind('jb_orders', JSON.stringify(rows)).run(),
      { op: 'bulk_snapshot_write', opId: requestId }
    );
  } catch (_e) {}

  const response = {
    ok: true,
    saved,
    total: rows.length,
    staleIgnored,
    orders: rows.length
  };
  await storeOrderOpRecord(db, {
    opId: requestId,
    orderId: '__bulk__',
    opType: 'bulk_upsert',
    reqV: rows.length,
    reqTs: Date.now(),
    ackV: saved,
    status: staleIgnored ? 'partial' : 'applied',
    response
  });
  await recordOrderAuditEvent(db, 'order_bulk_upsert', '__bulk__', {
    requestId,
    total: rows.length,
    saved,
    staleIgnored
  }, authContext);
  await maybePruneOrderOps(db);
  logWorker('info', 'bulk_orders_upsert', {
    requestId,
    total: rows.length,
    saved,
    staleIgnored,
    latencyMs: Date.now() - startedAt
  });
  return json(response);
}

async function readOrderFromKvSnapshot(db, id) {
  const kv = await db.prepare("SELECT value FROM kv_store WHERE key='jb_orders'").first();
  if (!kv || !kv.value) return null;
  try {
    const list = JSON.parse(kv.value);
    if (!Array.isArray(list)) return null;
    return list.find(o => o && String(o.id) === String(id)) || null;
  } catch {
    return null;
  }
}

function pickFresherOrder(a, b) {
  if (!a) return b || null;
  if (!b) return a || null;
  const av = Number(a.v) || 0;
  const bv = Number(b.v) || 0;
  const ats = Number(a.updatedAt || a.ts) || 0;
  const bts = Number(b.updatedAt || b.ts) || 0;
  if (bv > av || (bv === av && bts > ats)) return b;
  return a;
}

async function getOrders(db, authContext = null, opts = {}) {
  const startedAt = Date.now();
  const sinceTs = Math.max(0, Number(opts && opts.sinceTs) || 0);
  const sinceV = Math.max(0, Number(opts && opts.sinceV) || 0);
  try {
    await ensureOrderTables(db);
    const rows = await withD1Retry(
      () => db.prepare('SELECT * FROM orders ORDER BY ts DESC').all(),
      { op: 'orders_list_table' }
    );
    const tableOrders = rows.results
      .map(r => { try { return JSON.parse(r.data); } catch { return null; } })
      .filter(Boolean);

    // Always merge with kv_store snapshot as a safety net. In some legacy/import
    // scenarios orders table can be partial while kv has the full historical set.
    let kvOrders = [];
    const kv = await withD1Retry(
      () => db.prepare("SELECT value FROM kv_store WHERE key='jb_orders'").first(),
      { op: 'orders_list_kv' }
    );
    if (kv && kv.value) {
      try { kvOrders = JSON.parse(kv.value) || []; } catch { kvOrders = []; }
    }

    const byId = new Map();
    const mergeOne = (order) => {
      if (!order || !order.id) return;
      const id = String(order.id);
      const prev = byId.get(id);
      if (!prev) {
        byId.set(id, order);
        return;
      }
      const ov = Number(order.v) || 0;
      const pv = Number(prev.v) || 0;
      const ots = Number(order.updatedAt || order.ts) || 0;
      const pts = Number(prev.updatedAt || prev.ts) || 0;
      if (ov > pv || (ov === pv && ots >= pts)) {
        byId.set(id, order);
      }
    };

    (Array.isArray(kvOrders) ? kvOrders : []).forEach(mergeOne);
    (Array.isArray(tableOrders) ? tableOrders : []).forEach(mergeOne);

    const orders = [...byId.values()]
      .filter(order => !(order && order.deletedAt))
      .sort((a, b) => (Number(b.ts) || 0) - (Number(a.ts) || 0));
    const incremental = !!(sinceTs > 0 || sinceV > 0);
    const filteredOrders = incremental
      ? orders.filter((order) => {
          const ov = Number(order && order.v) || 0;
          const ots = Number(order && (order.updatedAt || order.ts)) || 0;
          return ov >= sinceV && ots >= sinceTs;
        })
      : orders;
    logWorker('info', 'orders_list_served', {
      tableCount: tableOrders.length,
      kvCount: Array.isArray(kvOrders) ? kvOrders.length : 0,
      mergedCount: orders.length,
      resultCount: filteredOrders.length,
      incremental,
      sinceTs,
      sinceV,
      latencyMs: Date.now() - startedAt
    });
    await bumpSecurityMetric(db, 'orders_list_served', 1);
    return json({
      orders: filteredOrders,
      incremental,
      sinceTs,
      sinceV
    });
  } catch(e) {
    logWorker('error', 'orders_list_failed', {
      message: String(e && e.message ? e.message : e || 'unknown'),
      latencyMs: Date.now() - startedAt
    });
    return json({ orders: [], error: e.message });
  }
}

async function saveOrder(db, payload, authContext = null) {
  const startedAt = Date.now();
  await ensureOrderTables(db);

  const parsed = parseOrderMutationPayload(payload);
  const rawOrder = parsed.order;
  const validationErr = validateOrderPayload(rawOrder);
  if (validationErr) return json({ error: validationErr }, 400);

  const clean = sanitizeOrderForStorage(rawOrder);
  const id = String(clean && clean.id || '').trim();
  if (!id) return json({ error: 'order id missing' }, 400);

  const prevTable = await withD1Retry(
    () => readOrderFromTable(db, id),
    { op: 'order_prev_table', orderId: id }
  );
  const prevKv = await withD1Retry(
    () => readOrderFromKvSnapshot(db, id),
    { op: 'order_prev_kv', orderId: id }
  );
  const prev = pickFresherOrder(prevTable, prevKv);
  const prevV = Math.max(0, Number(prev && prev.v) || 0);
  const prevTs = Number(prev && (prev.updatedAt || prev.ts)) || 0;
  const prevDeleted = !!(prev && prev.deletedAt);

  const requestedV = Math.max(0, Number(parsed.v) || Number(clean && clean.v) || 0);
  const incomingV = Math.max(1, requestedV || (prevV + 1));
  const incomingTs = Number(parsed.ts || clean.updatedAt || clean.ts) || Date.now();
  const incomingDeleted = !!(clean && clean.deletedAt);
  const opId = normalizeOpId(
    parsed.opId || parsed.requestId || (clean && (clean.opId || clean.lastOpId)),
    buildFallbackOpId('upsert', id, incomingV, incomingTs)
  );

  const knownOp = await readOrderOpRecord(db, opId);
  if (knownOp) {
    const cached = safeParseJson(knownOp.response, null);
    if (cached && typeof cached === 'object') {
      logWorker('info', 'order_upsert_idempotent_hit', { orderId: id, opId });
      return json({ ...cached, idempotent: true });
    }
  }

  const stale = (
    incomingV < prevV ||
    (incomingV === prevV && incomingTs < prevTs) ||
    (prevDeleted && !incomingDeleted && incomingV <= prevV)
  );
  if (stale) {
    const staleResponse = {
      ok: true,
      staleIgnored: true,
      ackV: prevV || incomingV,
      conflict: { type: 'stale_upsert', serverV: prevV, serverTs: prevTs },
      shouldRefetch: true
    };
    await storeOrderOpRecord(db, {
      opId,
      orderId: id,
      opType: 'upsert',
      reqV: incomingV,
      reqTs: incomingTs,
      ackV: prevV || incomingV,
      status: 'stale',
      response: staleResponse
    });
    await recordOrderAuditEvent(db, 'order_upsert_stale', id, {
      opId,
      incomingV,
      prevV,
      incomingTs,
      prevTs
    }, authContext);
    logWorker('warn', 'order_upsert_stale', { orderId: id, opId, incomingV, prevV, incomingTs, prevTs });
    return json(staleResponse);
  }

  const nextOrder = {
    ...clean,
    id,
    updatedAt: incomingTs,
    ts: Number(clean && clean.ts) || incomingTs,
    v: incomingV,
    opId,
    lastOpId: opId,
    deletedAt: incomingDeleted ? Number(clean && clean.deletedAt) || incomingTs : undefined,
    products: incomingDeleted ? [] : (Array.isArray(clean && clean.products) ? clean.products : [])
  };

  const upsertSql = `
    INSERT INTO orders (id,ts,data) VALUES (?,?,?)
    ON CONFLICT(id) DO UPDATE SET data=excluded.data, ts=excluded.ts
    WHERE
      COALESCE(CAST(json_extract(orders.data,'$.v') AS INTEGER),0) < ?
      OR (
        COALESCE(CAST(json_extract(orders.data,'$.v') AS INTEGER),0) = ?
        AND COALESCE(
          CAST(json_extract(orders.data,'$.updatedAt') AS INTEGER),
          COALESCE(CAST(json_extract(orders.data,'$.ts') AS INTEGER),0)
        ) <= ?
      )
  `;
  const writeRes = await withD1Retry(
    () => db.prepare(upsertSql)
      .bind(id, Number(nextOrder.ts) || incomingTs, JSON.stringify(nextOrder), incomingV, incomingV, incomingTs)
      .run(),
    { op: 'order_upsert_write', orderId: id, opId }
  );
  const changed = Number(writeRes && writeRes.meta && writeRes.meta.changes) || 0;

  if (!changed) {
    const latest = pickFresherOrder(
      await withD1Retry(() => readOrderFromTable(db, id), { op: 'order_latest_table', orderId: id, opId }),
      await withD1Retry(() => readOrderFromKvSnapshot(db, id), { op: 'order_latest_kv', orderId: id, opId })
    );
    const latestV = Number(latest && latest.v) || prevV || incomingV;
    const latestTs = Number(latest && (latest.updatedAt || latest.ts)) || prevTs || incomingTs;
    const conflictResponse = {
      ok: true,
      staleIgnored: true,
      ackV: latestV,
      conflict: { type: 'concurrent_upsert_conflict', serverV: latestV, serverTs: latestTs },
      shouldRefetch: true
    };
    await storeOrderOpRecord(db, {
      opId,
      orderId: id,
      opType: 'upsert',
      reqV: incomingV,
      reqTs: incomingTs,
      ackV: latestV,
      status: 'stale',
      response: conflictResponse
    });
    await recordOrderAuditEvent(db, 'order_upsert_conflict', id, {
      opId,
      incomingV,
      latestV,
      incomingTs,
      latestTs
    }, authContext);
    logWorker('warn', 'order_upsert_conflict', { orderId: id, opId, incomingV, latestV, incomingTs, latestTs });
    return json(conflictResponse);
  }

  const successResponse = { ok: true, ackV: incomingV, opId };
  await storeOrderOpRecord(db, {
    opId,
    orderId: id,
    opType: 'upsert',
    reqV: incomingV,
    reqTs: incomingTs,
    ackV: incomingV,
    status: 'applied',
    response: successResponse
  });
  await recordOrderAuditEvent(db, incomingDeleted ? 'order_tombstone_applied' : 'order_upsert_applied', id, {
    opId,
    ackV: incomingV,
    deletedAt: incomingDeleted ? Number(nextOrder.deletedAt || incomingTs) : 0,
    status: nextOrder.status || ''
  }, authContext);
  await maybePruneOrderOps(db);
  logWorker('info', 'order_upsert_applied', {
    orderId: id,
    opId,
    ackV: incomingV,
    latencyMs: Date.now() - startedAt
  });
  return json(successResponse);
}

async function saveOrderEvent(db, event, authContext = null) {
  const ev = event && typeof event === 'object' ? event : {};
  const ts = Number(ev.ts) || Date.now();
  const id = String(ev.id || ev.opId || ('evt_' + ts.toString(36) + '_' + Math.random().toString(36).slice(2, 8)));
  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind('order_event_' + id, JSON.stringify({ ...ev, id, ts })).run();
  await recordOrderAuditEvent(db, 'order_event_saved', String(ev.orderId || 'unknown'), {
    id,
    opId: String(ev.opId || id),
    ts,
    type: String(ev.type || ev.action || 'event')
  }, authContext);
  return json({ ok: true, id });
}

async function saveCatalogOrder(db, order) {
  const code = String(order && order.code ? order.code : '').trim().toUpperCase();
  if (!code) return json({ error: 'code missing' }, 400);
  const clean = {
    ...(order && typeof order === 'object' ? order : {}),
    code,
    ts: Number(order && order.ts) || Date.now(),
  };
  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind('catalog_order_' + code, JSON.stringify(clean)).run();
  return json({ ok: true, code });
}

async function getCatalogOrder(db, code) {
  const normalized = String(code || '').trim().toUpperCase();
  if (!normalized) return json({ error: 'code missing' }, 400);
  const row = await db.prepare('SELECT value FROM kv_store WHERE key=?').bind('catalog_order_' + normalized).first();
  if (!row) return json({ error: 'not_found' }, 404);
  try {
    return json(JSON.parse(row.value));
  } catch {
    return json({ error: 'invalid_data' }, 500);
  }
}

async function saveImage(db, data, type) {
  const id = crypto.randomUUID();
  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind('img_' + id, JSON.stringify({ data, type })).run();
  return json({ id });
}

async function getImage(env, id) {
  const db = env.DB;
  const row = await db.prepare('SELECT value FROM kv_store WHERE key=?').bind('img_' + id).first();
  if (!row) return new Response('Not found', { status: 404 });
  const img = JSON.parse(row.value);
  const base64 = img.data.replace(/^data:[^;]+;base64,/, '');
  const binary = Uint8Array.from(atob(base64), c => c.charCodeAt(0));
  return new Response(binary, { headers: { ...CORS, 'Content-Type': img.type || 'image/jpeg', 'Cache-Control': 'public, max-age=86400' } });
}

async function getCatalog(env) {
  const db = env.DB;
  const row = await db.prepare('SELECT value FROM kv_store WHERE key=?').bind('catalog_html').first();
  if (!row) return new Response('Katalog henüz oluşturulmadı.', { status: 404, headers: CORS });
  const html = JSON.parse(row.value);
  return new Response(html, { headers: { ...CORS, 'Content-Type': 'text/html;charset=utf-8' } });
}
