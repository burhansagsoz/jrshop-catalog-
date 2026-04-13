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
const ORDER_COMMAND_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const ORDER_COMMAND_PRUNE_INTERVAL_MS = 5 * 60 * 1000;
const ORDER_COMMAND_LEASE_MS = 15 * 1000;
const ORDER_COMMAND_RETRY_BASE_MS = 250;
const ORDER_COMMAND_MAX_RETRIES = 8;
const ORDER_COMMAND_DRAIN_LIMIT = 8;
const ORDER_COMMAND_WAIT_TIMEOUT_MS = 4500;
const ORDER_COMMAND_WAIT_STEP_MS = 90;
const ORDER_COMMAND_DLQ_RETENTION_MS = 30 * 24 * 60 * 60 * 1000;
const ORDER_RECOVERY_OP_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
const ORDER_QUEUE_MAX_PENDING = 5000;
const ORDER_QUEUE_MAX_FAILED = 2000;
const ORDER_QUEUE_CIRCUIT_COOLDOWN_MS = 30 * 1000;
const ORDER_QUEUE_CIRCUIT_FAIL_THRESHOLD = 20;
const ORDER_QUEUE_AUTO_REDRIVE_LIMIT = 3;
const ORDER_QUEUE_AUTO_REDRIVE_COOLDOWN_MS = 30 * 1000;
const ORDER_QUEUE_AUTO_REDRIVE_HISTORY_LIMIT = 60;
const ORDER_QUEUE_FORCE_DRAIN_LIMIT_MAX = 120;
const ORDER_QUEUE_STUCK_PROCESSING_MAX_AGE_MS = 90 * 1000;
const ORDER_QUEUE_STUCK_RECOVER_LIMIT = 12;
const ORDER_OPS_BATCH_MAX = 50;
const ORDER_UPDATES_LIMIT_DEFAULT = 200;
const ORDER_UPDATES_LIMIT_MAX = 500;
const DEFAULT_ADMIN_EMAIL = 'joobuyadmin@gmail.com';
let _orderTablesEnsuredAt = 0;
let _lastOrderOpsPruneAt = 0;
let _lastOrderCommandsPruneAt = 0;
let _lastOrderCommandDlqPruneAt = 0;
let _lastOrderRecoveryOpsPruneAt = 0;
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
  await db.prepare(`
    CREATE TABLE IF NOT EXISTS order_commands (
      command_id TEXT PRIMARY KEY,
      op_id TEXT NOT NULL,
      order_id TEXT NOT NULL,
      command_type TEXT NOT NULL,
      payload TEXT NOT NULL,
      status TEXT NOT NULL,
      result TEXT,
      error TEXT,
      attempts INTEGER NOT NULL DEFAULT 0,
      lease_until INTEGER NOT NULL DEFAULT 0,
      available_at INTEGER NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    )
  `).run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_order_commands_status_available ON order_commands(status,available_at,created_at)').run();
  await db.prepare('CREATE INDEX IF NOT EXISTS idx_order_commands_op_id ON order_commands(op_id)').run();
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

async function maybePruneOrderCommands(db) {
  const now = Date.now();
  if ((now - _lastOrderCommandsPruneAt) < ORDER_COMMAND_PRUNE_INTERVAL_MS) return;
  _lastOrderCommandsPruneAt = now;
  try {
    await db.prepare('DELETE FROM order_commands WHERE updated_at < ? AND status IN (\'applied\',\'failed\')')
      .bind(now - ORDER_COMMAND_RETENTION_MS)
      .run();
  } catch (_e) {}
}

async function maybePruneOrderCommandDeadLetters(db) {
  const now = Date.now();
  if ((now - _lastOrderCommandDlqPruneAt) < ORDER_COMMAND_PRUNE_INTERVAL_MS) return;
  _lastOrderCommandDlqPruneAt = now;
  try {
    await db.prepare(`
      DELETE FROM kv_store
      WHERE key LIKE 'order_deadletter:%'
        AND COALESCE(CAST(json_extract(value,'$.failedAt') AS INTEGER),0) < ?
    `).bind(now - ORDER_COMMAND_DLQ_RETENTION_MS).run();
  } catch (_e) {}
}

async function maybePruneOrderRecoveryOps(db) {
  const now = Date.now();
  if ((now - _lastOrderRecoveryOpsPruneAt) < ORDER_COMMAND_PRUNE_INTERVAL_MS) return;
  _lastOrderRecoveryOpsPruneAt = now;
  try {
    await db.prepare(`
      DELETE FROM kv_store
      WHERE key LIKE 'order_recovery_op:%'
        AND COALESCE(CAST(json_extract(value,'$.createdAt') AS INTEGER),0) < ?
    `).bind(now - ORDER_RECOVERY_OP_RETENTION_MS).run();
  } catch (_e) {}
}

function getOrderCommandRetryDelayMs(attempts) {
  const n = Math.max(1, Number(attempts) || 1);
  const cappedExponent = Math.min(6, n - 1);
  return Math.min(10_000, ORDER_COMMAND_RETRY_BASE_MS * Math.pow(2, cappedExponent));
}

function toOrderCommandEnvelope(source = {}) {
  const now = Date.now();
  const opId = normalizeOpId(source.opId || source.commandId, `cmd_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`);
  return {
    commandId: opId,
    opId,
    orderId: String(source.orderId || '').trim(),
    commandType: String(source.commandType || 'upsert').trim().toLowerCase(),
    payload: source.payload && typeof source.payload === 'object' ? source.payload : {},
    availableAt: Math.max(0, Number(source.availableAt) || now),
  };
}

async function readOrderCommandRecord(db, commandId) {
  const id = normalizeOpId(commandId);
  if (!id) return null;
  try {
    return await db.prepare('SELECT * FROM order_commands WHERE command_id=?').bind(id).first();
  } catch (_e) {
    return null;
  }
}

function parseOrderCommandResult(row, fallback = null) {
  if (!row || !row.result) return fallback;
  const parsed = safeParseJson(row.result, fallback);
  return parsed && typeof parsed === 'object' ? parsed : fallback;
}

function toOrderCommandView(row) {
  if (!row || typeof row !== 'object') return null;
  return {
    commandId: normalizeOpId(row.command_id || row.commandId),
    opId: normalizeOpId(row.op_id || row.opId),
    orderId: String(row.order_id || row.orderId || ''),
    commandType: String(row.command_type || row.commandType || ''),
    status: String(row.status || ''),
    error: String(row.error || ''),
    attempts: Number(row.attempts) || 0,
    availableAt: Number(row.available_at) || 0,
    leaseUntil: Number(row.lease_until) || 0,
    createdAt: Number(row.created_at) || 0,
    updatedAt: Number(row.updated_at) || 0,
    result: parseOrderCommandResult(row, null)
  };
}

function parseOrderCommandPayload(row) {
  const parsed = safeParseJson(row && row.payload, {});
  return parsed && typeof parsed === 'object' ? parsed : {};
}

function buildOrderRecoveryOpStoreKey(action, requestId) {
  const rid = normalizeOpId(requestId);
  if (!rid) return '';
  const actionKey = normalizeOpId(String(action || '').replace(/[:]/g, '_'), 'recovery');
  return `order_recovery_op:${String(actionKey).slice(0, 80)}:${rid}`;
}

async function readOrderRecoveryOpResult(db, action, requestId) {
  const key = buildOrderRecoveryOpStoreKey(action, requestId);
  if (!key) return null;
  const row = await db.prepare('SELECT value FROM kv_store WHERE key=?').bind(key).first();
  const parsed = safeParseJson(row && row.value, null);
  if (!parsed || typeof parsed !== 'object') return null;
  return parsed;
}

async function storeOrderRecoveryOpResult(db, action, requestId, statusCode, responseBody) {
  const key = buildOrderRecoveryOpStoreKey(action, requestId);
  if (!key) return;
  const now = Date.now();
  const body = responseBody && typeof responseBody === 'object' ? responseBody : {};
  const record = {
    action: String(action || ''),
    requestId: normalizeOpId(requestId),
    statusCode: Math.max(100, Math.min(599, Number(statusCode) || 200)),
    response: body,
    createdAt: now
  };
  await db.prepare(
    'INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'
  ).bind(key, JSON.stringify(record)).run();
}

function percentileFromSorted(values, q) {
  const list = Array.isArray(values) ? values : [];
  if (!list.length) return 0;
  const p = Math.max(0, Math.min(1, Number(q) || 0));
  const idx = Math.min(list.length - 1, Math.max(0, Math.ceil((list.length - 1) * p)));
  return Number(list[idx]) || 0;
}

function buildLatencyPercentiles(rows, field = 'ts', nowTs = Date.now()) {
  const source = Array.isArray(rows) ? rows : [];
  const ages = source
    .map((row) => Math.max(0, nowTs - (Number(row && row[field]) || 0)))
    .filter((age) => Number.isFinite(age))
    .sort((a, b) => a - b);
  if (!ages.length) {
    return { samples: 0, min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0 };
  }
  const sum = ages.reduce((acc, v) => acc + v, 0);
  return {
    samples: ages.length,
    min: ages[0],
    max: ages[ages.length - 1],
    avg: Math.round(sum / ages.length),
    p50: percentileFromSorted(ages, 0.50),
    p95: percentileFromSorted(ages, 0.95),
    p99: percentileFromSorted(ages, 0.99)
  };
}

function getOrderQueueLimitConfig(env = {}) {
  return {
    pendingMax: toBoundedInt(env.ORDER_QUEUE_PENDING_MAX, ORDER_QUEUE_MAX_PENDING, 100, 200000),
    failedMax: toBoundedInt(env.ORDER_QUEUE_FAILED_MAX, ORDER_QUEUE_MAX_FAILED, 50, 100000),
    breakerErrorThreshold: toBoundedInt(env.ORDER_QUEUE_BREAKER_ERROR_THRESHOLD, ORDER_QUEUE_CIRCUIT_FAIL_THRESHOLD, 5, 10000),
    breakerWindowMs: toBoundedInt(env.ORDER_QUEUE_BREAKER_WINDOW_MS, 2 * 60 * 1000, 10 * 1000, 60 * 60 * 1000),
    breakerCooldownMs: toBoundedInt(env.ORDER_QUEUE_BREAKER_COOLDOWN_MS, ORDER_QUEUE_CIRCUIT_COOLDOWN_MS, 1000, 10 * 60 * 1000),
    autoRedriveBatchLimit: toBoundedInt(env.ORDER_AUTO_REDRIVE_BATCH_LIMIT, ORDER_QUEUE_AUTO_REDRIVE_LIMIT, 0, 100),
    autoRedriveDelayMs: toBoundedInt(env.ORDER_AUTO_REDRIVE_DELAY_MS, 0, 0, 60 * 1000),
    autoRedriveCooldownMs: toBoundedInt(env.ORDER_AUTO_REDRIVE_COOLDOWN_MS, ORDER_QUEUE_AUTO_REDRIVE_COOLDOWN_MS, 1000, 10 * 60 * 1000),
    forceDrainMaxLimit: toBoundedInt(env.ORDER_QUEUE_FORCE_DRAIN_MAX_LIMIT, ORDER_QUEUE_FORCE_DRAIN_LIMIT_MAX, 5, 500),
    stuckProcessingMaxAgeMs: toBoundedInt(env.ORDER_QUEUE_STUCK_PROCESSING_MAX_AGE_MS, ORDER_QUEUE_STUCK_PROCESSING_MAX_AGE_MS, 15 * 1000, 24 * 60 * 60 * 1000),
    stuckRecoverBatchLimit: toBoundedInt(env.ORDER_QUEUE_STUCK_RECOVER_BATCH_LIMIT, ORDER_QUEUE_STUCK_RECOVER_LIMIT, 0, 200)
  };
}

function normalizeOrderQueueAutoRedriveReport(report = {}) {
  const src = report && typeof report === 'object' ? report : {};
  const blocked = String(src.blocked || '').trim();
  return {
    ts: Math.max(0, Number(src.ts) || Date.now()),
    limit: Math.max(0, Number(src.limit) || 0),
    attempted: Math.max(0, Number(src.attempted) || 0),
    redriven: Math.max(0, Number(src.redriven) || 0),
    skipped: Math.max(0, Number(src.skipped) || 0),
    failed: Math.max(0, Number(src.failed) || 0),
    throttled: src.throttled === true,
    blocked: blocked ? blocked.slice(0, 80) : '',
    durationMs: Math.max(0, Number(src.durationMs) || 0)
  };
}

async function readOrderQueueRuntimeState(db) {
  const row = await db.prepare("SELECT value FROM kv_store WHERE key='order_queue_runtime_state'").first();
  const parsed = safeParseJson(row && row.value, null);
  if (!parsed || typeof parsed !== 'object') {
    return {
      breakerOpenUntil: 0,
      errorEvents: [],
      lastAutoRedriveAt: 0,
      autoRedriveHistory: [],
      updatedAt: 0
    };
  }
  return {
    breakerOpenUntil: Math.max(0, Number(parsed.breakerOpenUntil) || 0),
    errorEvents: Array.isArray(parsed.errorEvents) ? parsed.errorEvents.map((ts) => Number(ts) || 0).filter((ts) => ts > 0) : [],
    lastAutoRedriveAt: Math.max(0, Number(parsed.lastAutoRedriveAt) || 0),
    autoRedriveHistory: Array.isArray(parsed.autoRedriveHistory)
      ? parsed.autoRedriveHistory.map((item) => normalizeOrderQueueAutoRedriveReport(item)).slice(-ORDER_QUEUE_AUTO_REDRIVE_HISTORY_LIMIT)
      : [],
    updatedAt: Math.max(0, Number(parsed.updatedAt) || 0)
  };
}

async function writeOrderQueueRuntimeState(db, state) {
  const now = Date.now();
  const next = {
    breakerOpenUntil: Math.max(0, Number(state && state.breakerOpenUntil) || 0),
    errorEvents: Array.isArray(state && state.errorEvents) ? state.errorEvents.map((ts) => Number(ts) || 0).filter((ts) => ts > 0).slice(-500) : [],
    lastAutoRedriveAt: Math.max(0, Number(state && state.lastAutoRedriveAt) || 0),
    autoRedriveHistory: Array.isArray(state && state.autoRedriveHistory)
      ? state.autoRedriveHistory.map((item) => normalizeOrderQueueAutoRedriveReport(item)).slice(-ORDER_QUEUE_AUTO_REDRIVE_HISTORY_LIMIT)
      : [],
    updatedAt: now
  };
  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind('order_queue_runtime_state', JSON.stringify(next))
    .run();
  return next;
}

async function appendOrderQueueAutoRedriveReport(db, state, report, opts = {}) {
  const runtime = state && typeof state === 'object' ? state : await readOrderQueueRuntimeState(db);
  const normalized = normalizeOrderQueueAutoRedriveReport(report);
  const history = Array.isArray(runtime.autoRedriveHistory) ? runtime.autoRedriveHistory : [];
  const nextPayload = {
    ...runtime,
    autoRedriveHistory: [...history, normalized].slice(-ORDER_QUEUE_AUTO_REDRIVE_HISTORY_LIMIT)
  };
  if (opts && opts.touchLastAutoRedriveAt) {
    nextPayload.lastAutoRedriveAt = normalized.ts;
  }
  return await writeOrderQueueRuntimeState(db, nextPayload);
}

function pruneBreakerErrorEvents(events, now, windowMs) {
  const list = Array.isArray(events) ? events : [];
  const minTs = now - Math.max(1000, Number(windowMs) || 1000);
  return list
    .map((ts) => Number(ts) || 0)
    .filter((ts) => ts >= minTs && ts <= (now + 60 * 1000));
}

async function markOrderQueueProcessingError(db, env = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const now = Date.now();
  const state = await readOrderQueueRuntimeState(db);
  const events = pruneBreakerErrorEvents(state.errorEvents, now, cfg.breakerWindowMs);
  events.push(now);
  let breakerOpenUntil = Math.max(0, Number(state.breakerOpenUntil) || 0);
  if (events.length >= cfg.breakerErrorThreshold) {
    breakerOpenUntil = Math.max(breakerOpenUntil, now + cfg.breakerCooldownMs);
  }
  await writeOrderQueueRuntimeState(db, {
    ...state,
    errorEvents: events,
    breakerOpenUntil
  });
  return {
    breakerOpenUntil,
    errorCount: events.length
  };
}

async function markOrderQueueProcessingSuccess(db, env = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const now = Date.now();
  const state = await readOrderQueueRuntimeState(db);
  const events = pruneBreakerErrorEvents(state.errorEvents, now, cfg.breakerWindowMs);
  const trimmed = events.length > 4 ? events.slice(Math.floor(events.length / 2)) : events;
  await writeOrderQueueRuntimeState(db, {
    ...state,
    errorEvents: trimmed,
    breakerOpenUntil: Math.max(0, Number(state.breakerOpenUntil) || 0)
  });
}

async function getOrderQueuePressureSnapshot(db) {
  const pendingRow = await db.prepare(`
    SELECT COUNT(*) AS pending_count
    FROM order_commands
    WHERE status IN ('pending','retry','processing')
  `).first();
  const failedRow = await db.prepare(`
    SELECT COUNT(*) AS failed_count
    FROM order_commands
    WHERE status='failed'
  `).first();
  return {
    pending: Number(pendingRow && pendingRow.pending_count) || 0,
    failed: Number(failedRow && failedRow.failed_count) || 0
  };
}

async function isOrderQueueWriteSaturated(db, env = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const pressure = await getOrderQueuePressureSnapshot(db);
  const saturated = pressure.pending >= cfg.pendingMax || pressure.failed >= cfg.failedMax;
  return {
    saturated,
    pressure,
    limits: {
      pendingMax: cfg.pendingMax,
      failedMax: cfg.failedMax
    }
  };
}

async function getOrderQueueRuntimeHealth(db, env = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const now = Date.now();
  const state = await readOrderQueueRuntimeState(db);
  const events = pruneBreakerErrorEvents(state.errorEvents, now, cfg.breakerWindowMs);
  const pressure = await getOrderQueuePressureSnapshot(db);
  const latestAutoRedrive = Array.isArray(state.autoRedriveHistory) && state.autoRedriveHistory.length
    ? state.autoRedriveHistory[state.autoRedriveHistory.length - 1]
    : null;
  return {
    breaker: {
      open: Number(state.breakerOpenUntil || 0) > now,
      openUntil: Number(state.breakerOpenUntil || 0),
      errorCountInWindow: events.length,
      threshold: cfg.breakerErrorThreshold,
      windowMs: cfg.breakerWindowMs,
      cooldownMs: cfg.breakerCooldownMs
    },
    pressure: {
      pending: pressure.pending,
      failed: pressure.failed,
      pendingMax: cfg.pendingMax,
      failedMax: cfg.failedMax,
      writeSaturated: pressure.pending >= cfg.pendingMax || pressure.failed >= cfg.failedMax
    },
    autoRedrive: {
      batchLimit: cfg.autoRedriveBatchLimit,
      delayMs: cfg.autoRedriveDelayMs,
      cooldownMs: cfg.autoRedriveCooldownMs,
      lastAt: Number(state.lastAutoRedriveAt || 0),
      latestResult: latestAutoRedrive
    }
  };
}

async function getOrderQueueBreakerState(db, env = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const now = Date.now();
  const state = await readOrderQueueRuntimeState(db);
  const events = pruneBreakerErrorEvents(state.errorEvents, now, cfg.breakerWindowMs);
  const pressure = await getOrderQueuePressureSnapshot(db);
  return {
    ts: now,
    breaker: {
      open: Number(state.breakerOpenUntil || 0) > now,
      openUntil: Number(state.breakerOpenUntil || 0),
      errorCountInWindow: events.length,
      threshold: cfg.breakerErrorThreshold,
      windowMs: cfg.breakerWindowMs,
      cooldownMs: cfg.breakerCooldownMs,
      remainingOpenMs: Math.max(0, Number(state.breakerOpenUntil || 0) - now)
    },
    pressure: {
      pending: pressure.pending,
      failed: pressure.failed,
      pendingMax: cfg.pendingMax,
      failedMax: cfg.failedMax,
      writeSaturated: pressure.pending >= cfg.pendingMax || pressure.failed >= cfg.failedMax
    },
    recentErrorEvents: events.slice(-20),
    runtimeUpdatedAt: Number(state.updatedAt || 0)
  };
}

async function getOrderQueueAutoRedriveReport(db, limit = 20) {
  const capped = Math.max(1, Math.min(100, Number(limit) || 20));
  const state = await readOrderQueueRuntimeState(db);
  const history = Array.isArray(state.autoRedriveHistory) ? state.autoRedriveHistory : [];
  const items = history.slice(-capped).reverse();
  const summary = items.reduce((acc, item) => {
    acc.attempted += Number(item && item.attempted) || 0;
    acc.redriven += Number(item && item.redriven) || 0;
    acc.skipped += Number(item && item.skipped) || 0;
    acc.failed += Number(item && item.failed) || 0;
    if (item && item.blocked) acc.blocked += 1;
    if (item && item.throttled) acc.throttled += 1;
    return acc;
  }, { attempted: 0, redriven: 0, skipped: 0, failed: 0, blocked: 0, throttled: 0 });
  return {
    items,
    summary,
    lastAt: Number(state.lastAutoRedriveAt || 0),
    runtimeUpdatedAt: Number(state.updatedAt || 0)
  };
}

async function getOrderQueueProcessingStats(db, env = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const now = Date.now();
  const minUpdatedAt = now - cfg.stuckProcessingMaxAgeMs;
  const row = await db.prepare(`
    SELECT
      COUNT(*) AS processing_count,
      MIN(updated_at) AS oldest_processing_updated_at,
      SUM(CASE WHEN lease_until>? AND updated_at<=? THEN 1 ELSE 0 END) AS stuck_count
    FROM order_commands
    WHERE status='processing'
  `).bind(now, minUpdatedAt).first();
  return {
    processing: Number(row && row.processing_count) || 0,
    oldestProcessingUpdatedAt: Number(row && row.oldest_processing_updated_at) || 0,
    stuck: Number(row && row.stuck_count) || 0,
    maxAgeMs: cfg.stuckProcessingMaxAgeMs
  };
}

async function recoverStuckProcessingCommands(db, authContext = null, env = {}, opts = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const now = Date.now();
  const limit = Math.max(0, Math.min(cfg.stuckRecoverBatchLimit, Number(opts && opts.limit) || cfg.stuckRecoverBatchLimit));
  const maxAgeMs = cfg.stuckProcessingMaxAgeMs;
  if (limit <= 0) {
    return { attempted: 0, recovered: 0, limit: 0, maxAgeMs };
  }
  const minUpdatedAt = now - maxAgeMs;
  const rows = await db.prepare(`
    SELECT command_id, order_id, attempts, lease_until, updated_at
    FROM order_commands
    WHERE status='processing'
      AND lease_until>?
      AND updated_at<=?
    ORDER BY updated_at ASC
    LIMIT ?
  `).bind(now, minUpdatedAt, limit).all();
  const candidates = Array.isArray(rows && rows.results) ? rows.results : [];
  let recovered = 0;
  const recoveredIds = [];
  for (const item of candidates) {
    const commandId = normalizeOpId(item && item.command_id);
    if (!commandId) continue;
    const res = await db.prepare(`
      UPDATE order_commands
      SET status='retry',
          lease_until=0,
          available_at=?,
          updated_at=?,
          error=?
      WHERE command_id=?
        AND status='processing'
        AND lease_until>?
        AND updated_at<=?
    `).bind(
      now,
      now,
      `stuck_processing_recovered:${now}`,
      commandId,
      now,
      minUpdatedAt
    ).run();
    const changed = Number(res && res.meta && res.meta.changes) || 0;
    if (!changed) continue;
    recovered++;
    recoveredIds.push(commandId);
    if (opts && opts.audit === true) {
      await recordOrderAuditEvent(db, 'order_command_stuck_recovered', String(item && item.order_id || 'unknown'), {
        commandId,
        previousAttempts: Number(item && item.attempts) || 0,
        previousLeaseUntil: Number(item && item.lease_until) || 0,
        previousUpdatedAt: Number(item && item.updated_at) || 0,
        maxAgeMs
      }, authContext);
    }
  }
  return {
    attempted: candidates.length,
    recovered,
    recoveredIds: recoveredIds.slice(0, 50),
    limit,
    maxAgeMs
  };
}

async function getOrderQueueWriteGuard(db, env = {}) {
  const health = await getOrderQueueRuntimeHealth(db, env);
  if (health && health.breaker && health.breaker.open) {
    return { blocked: true, reason: 'circuit_open', health };
  }
  if (health && health.pressure && health.pressure.writeSaturated) {
    return { blocked: true, reason: 'queue_saturated', health };
  }
  return { blocked: false, reason: '', health };
}

async function maybeAutoRedriveDeadLetters(db, authContext = null, env = {}, opts = {}) {
  const cfg = getOrderQueueLimitConfig(env);
  const limit = Math.max(0, Math.min(cfg.autoRedriveBatchLimit, Number(opts.limit) || cfg.autoRedriveBatchLimit));
  if (limit <= 0) return { attempted: 0, redriven: 0, skipped: 0, failed: 0, limit: 0 };
  const now = Date.now();
  const startedAt = now;
  const runtime = await readOrderQueueRuntimeState(db);
  const minGapMs = cfg.autoRedriveCooldownMs;
  if (Number(runtime.breakerOpenUntil || 0) > now) {
    const blockedReport = {
      ts: now, limit, attempted: 0, redriven: 0, skipped: 0, failed: 0, blocked: 'circuit_open', durationMs: 0
    };
    await appendOrderQueueAutoRedriveReport(db, runtime, blockedReport);
    return blockedReport;
  }
  const pressure = await getOrderQueuePressureSnapshot(db);
  if (pressure.pending >= cfg.pendingMax || pressure.failed >= cfg.failedMax) {
    const blockedReport = {
      ts: now, limit, attempted: 0, redriven: 0, skipped: 0, failed: 0, blocked: 'queue_saturated', durationMs: 0
    };
    await appendOrderQueueAutoRedriveReport(db, runtime, blockedReport);
    return blockedReport;
  }
  if ((now - Number(runtime.lastAutoRedriveAt || 0)) < minGapMs) {
    return { ts: now, limit, attempted: 0, redriven: 0, skipped: 0, failed: 0, throttled: true, durationMs: 0 };
  }
  const deadletters = await listOrderCommandDeadLetters(db, limit);
  const results = [];
  for (const item of deadletters) {
    const id = normalizeOpId(item && item.commandId);
    if (!id) continue;
    const result = await redriveOrderCommandById(db, id, authContext, {
      delayMs: cfg.autoRedriveDelayMs,
      requestId: normalizeOpId(`auto_redrive:${id}:${now}`, `auto_redrive_${now}`)
    });
    results.push(result);
  }
  const report = {
    ts: now,
    limit,
    attempted: results.length,
    redriven: results.filter((r) => r && r.redriven).length,
    skipped: results.filter((r) => r && r.skipped).length,
    failed: results.filter((r) => !r || r.ok === false).length,
    durationMs: Math.max(0, Date.now() - startedAt)
  };
  await appendOrderQueueAutoRedriveReport(db, runtime, report, { touchLastAutoRedriveAt: true });
  return report;
}

async function recordOrderCommandDeadLetter(db, row, reason, detail = '') {
  if (!db || !row) return;
  const commandId = normalizeOpId(row.command_id || row.commandId);
  if (!commandId) return;
  const now = Date.now();
  const payload = parseOrderCommandPayload(row);
  const deadLetter = {
    commandId,
    opId: normalizeOpId(row.op_id || commandId),
    orderId: String(row.order_id || ''),
    commandType: String(row.command_type || ''),
    attempts: Number(row.attempts) || 0,
    status: String(row.status || ''),
    reason: String(reason || 'command_failed'),
    detail: String(detail || '').slice(0, 1000),
    payload,
    failedAt: now
  };
  await db.prepare(
    'INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value'
  ).bind(`order_deadletter:${commandId}`, JSON.stringify(deadLetter)).run();
}

async function listOrderCommandDeadLetters(db, limit = 50) {
  const capped = Math.max(1, Math.min(200, Number(limit) || 50));
  const rows = await db.prepare(`
    SELECT key, value,
           COALESCE(CAST(json_extract(value,'$.failedAt') AS INTEGER),0) AS failed_at
    FROM kv_store
    WHERE key LIKE 'order_deadletter:%'
    ORDER BY failed_at DESC
    LIMIT ?
  `).bind(capped).all();
  return (rows.results || [])
    .map((row) => safeParseJson(row && row.value, null))
    .filter((item) => item && typeof item === 'object');
}

async function getOrderCommandDeadLetterById(db, commandId) {
  const id = normalizeOpId(commandId);
  if (!id) return null;
  const row = await db.prepare('SELECT value FROM kv_store WHERE key=?')
    .bind(`order_deadletter:${id}`)
    .first();
  const parsed = safeParseJson(row && row.value, null);
  return parsed && typeof parsed === 'object' ? parsed : null;
}

async function removeOrderCommandDeadLetterById(db, commandId) {
  const id = normalizeOpId(commandId);
  if (!id) return;
  await db.prepare('DELETE FROM kv_store WHERE key=?').bind(`order_deadletter:${id}`).run();
}

async function redriveOrderCommandById(db, commandId, authContext = null, opts = {}) {
  await ensureOrderTables(db);
  const id = normalizeOpId(commandId);
  if (!id) return { ok: false, error: 'command_id_missing' };
  const now = Date.now();
  const delayMs = Math.max(0, Math.min(60_000, Number(opts.delayMs) || 0));
  const requestId = normalizeOpId(
    opts.requestId || (authContext && authContext.requestId),
    `rq_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`
  );

  const existing = await readOrderCommandRecord(db, id);
  if (existing) {
    const status = String(existing.status || '').trim().toLowerCase();
    if (status === 'applied') {
      await removeOrderCommandDeadLetterById(db, id);
      return { ok: true, skipped: true, reason: 'already_applied', commandId: id, requestId };
    }
    if (status === 'pending' || status === 'retry' || status === 'processing') {
      return { ok: true, skipped: true, reason: 'already_queued', commandId: id, requestId };
    }
    if (status === 'failed') {
      await db.prepare(`
        UPDATE order_commands
        SET status='retry',
            error='',
            lease_until=0,
            available_at=?,
            updated_at=?,
            attempts=0
        WHERE command_id=?
      `).bind(now + delayMs, now, id).run();
      await removeOrderCommandDeadLetterById(db, id);
      await recordOrderAuditEvent(db, 'order_command_redriven', String(existing.order_id || 'unknown'), {
        requestId,
        source: 'failed_command',
        sourceCommandId: id,
        commandId: id,
        opId: String(existing.op_id || ''),
        delayMs,
        previousStatus: status,
        previousAttempts: Number(existing.attempts) || 0,
        previousError: String(existing.error || '')
      }, authContext);
      return { ok: true, redriven: true, commandId: id, source: 'failed_command', requestId };
    }
  }

  const deadLetter = await getOrderCommandDeadLetterById(db, id);
  if (!deadLetter) return { ok: false, error: 'deadletter_not_found', commandId: id, requestId };

  const payload = deadLetter.payload && typeof deadLetter.payload === 'object' ? deadLetter.payload : {};
  const commandType = String(deadLetter.commandType || '').trim().toLowerCase();
  if (!payload || !commandType) {
    return { ok: false, error: 'deadletter_invalid', commandId: id, requestId };
  }
  const redriveCommandId = normalizeOpId(`redrive:${id}:${now.toString(36)}`, `redrive_${now}`);
  await enqueueOrderCommand(db, {
    commandId: redriveCommandId,
    opId: normalizeOpId(deadLetter.opId || deadLetter.commandId || redriveCommandId),
    orderId: String(deadLetter.orderId || ''),
    commandType,
    availableAt: now + delayMs,
    payload
  });
  await removeOrderCommandDeadLetterById(db, id);
  await recordOrderAuditEvent(db, 'order_command_redriven', String(deadLetter.orderId || 'unknown'), {
    requestId,
    source: 'deadletter',
    sourceCommandId: id,
    commandId: redriveCommandId,
    fromDeadLetterCommandId: id,
    opId: String(deadLetter.opId || ''),
    delayMs,
    deadLetterReason: String(deadLetter.reason || ''),
    deadLetterAttempts: Number(deadLetter.attempts) || 0
  }, authContext);
  return { ok: true, redriven: true, commandId: redriveCommandId, source: 'deadletter', requestId };
}

async function redriveOrderCommandsBulk(db, commandIds, authContext = null, opts = {}) {
  const now = Date.now();
  const requestId = normalizeOpId(
    opts.requestId || (authContext && authContext.requestId),
    `rq_${now.toString(36)}_${Math.random().toString(36).slice(2, 8)}`
  );
  const requested = Array.isArray(commandIds) ? commandIds : [];
  const normalized = requested
    .map((id) => normalizeOpId(id))
    .filter(Boolean);
  const unique = [...new Set(normalized)];
  const maxItems = Math.max(1, Math.min(200, Number(opts.maxItems) || 100));
  const targetIds = unique.slice(0, maxItems);
  const results = [];
  for (const id of targetIds) {
    const res = await redriveOrderCommandById(db, id, authContext, { ...opts, requestId });
    results.push({ commandId: id, ...res });
  }
  const summary = results.reduce((acc, row) => {
    if (row && row.redriven) acc.redriven += 1;
    else if (row && row.skipped) acc.skipped += 1;
    else acc.failed += 1;
    return acc;
  }, { requested: requested.length, processed: results.length, redriven: 0, skipped: 0, failed: 0 });
  await recordOrderAuditEvent(db, 'order_command_redrive_bulk', '__bulk__', {
    requestId,
    delayMs: Math.max(0, Math.min(60_000, Number(opts.delayMs) || 0)),
    requested: summary.requested,
    processed: summary.processed,
    redriven: summary.redriven,
    skipped: summary.skipped,
    failed: summary.failed,
    commandIdsSample: targetIds.slice(0, 20)
  }, authContext);
  return { ...summary, requestId, results };
}

async function enqueueOrderCommand(db, command) {
  const env = toOrderCommandEnvelope(command);
  const now = Date.now();
  await db.prepare(`
    INSERT INTO order_commands (
      command_id,op_id,order_id,command_type,payload,status,result,error,attempts,lease_until,available_at,created_at,updated_at
    ) VALUES (?,?,?,?,?,'pending',NULL,'',0,0,?,?,?)
    ON CONFLICT(command_id) DO NOTHING
  `).bind(
    env.commandId,
    env.opId,
    env.orderId,
    env.commandType,
    JSON.stringify(env.payload || {}),
    env.availableAt,
    now,
    now
  ).run();
  return await readOrderCommandRecord(db, env.commandId);
}

async function tryClaimOrderCommand(db, commandId, leaseMs = ORDER_COMMAND_LEASE_MS) {
  const id = normalizeOpId(commandId);
  if (!id) return null;
  const now = Date.now();
  const leaseUntil = now + Math.max(1000, Number(leaseMs) || ORDER_COMMAND_LEASE_MS);
  const claimRes = await db.prepare(`
    UPDATE order_commands
    SET status='processing',
        attempts=attempts+1,
        lease_until=?,
        updated_at=?
    WHERE command_id=?
      AND available_at<=?
      AND (
        status IN ('pending','retry')
        OR (status='processing' AND lease_until<=?)
      )
  `).bind(leaseUntil, now, id, now, now).run();
  const changed = Number(claimRes && claimRes.meta && claimRes.meta.changes) || 0;
  if (!changed) return null;
  return await readOrderCommandRecord(db, id);
}

async function completeOrderCommand(db, commandId, result, status = 'applied', errorText = '') {
  const id = normalizeOpId(commandId);
  if (!id) return null;
  const now = Date.now();
  await db.prepare(`
    UPDATE order_commands
    SET status=?,
        result=?,
        error=?,
        lease_until=0,
        updated_at=?
    WHERE command_id=?
  `).bind(
    status,
    JSON.stringify(result && typeof result === 'object' ? result : {}),
    String(errorText || '').slice(0, 800),
    now,
    id
  ).run();
  return await readOrderCommandRecord(db, id);
}

async function retryOrFailOrderCommand(db, row, err) {
  const now = Date.now();
  const id = normalizeOpId(row && row.command_id);
  if (!id) return null;
  const attempts = Math.max(1, Number(row && row.attempts) || 1);
  const message = String(err && err.message ? err.message : err || 'unknown_error').slice(0, 800);
  const retryable = isRetryableD1Error(err) && attempts < ORDER_COMMAND_MAX_RETRIES;
  if (!retryable) {
    await db.prepare(`
      UPDATE order_commands
      SET status='failed',
          error=?,
          result=?,
          lease_until=0,
          updated_at=?
      WHERE command_id=?
    `).bind(
      message,
      JSON.stringify({ ok: false, error: 'command_failed', detail: message }),
      now,
      id
    ).run();
    const failedRow = await readOrderCommandRecord(db, id);
    await recordOrderCommandDeadLetter(db, failedRow || row, 'non_retryable_or_retry_exhausted', message);
    return await readOrderCommandRecord(db, id);
  }
  const delayMs = getOrderCommandRetryDelayMs(attempts);
  await db.prepare(`
    UPDATE order_commands
    SET status='retry',
        error=?,
        lease_until=0,
        available_at=?,
        updated_at=?
    WHERE command_id=?
  `).bind(message, now + delayMs, now, id).run();
  return await readOrderCommandRecord(db, id);
}

async function executeClaimedOrderCommand(db, row, authContext = null) {
  const commandType = String(row && row.command_type || '').trim().toLowerCase();
  const commandId = normalizeOpId(row && row.command_id);
  try {
    const payload = safeParseJson(row && row.payload, {});
    let result = null;
    if (commandType === 'upsert') {
      result = await applyOrderUpsertMutation(db, payload, authContext);
    } else if (commandType === 'delete') {
      result = await applyOrderDeleteMutation(db, payload, authContext);
    } else {
      result = { ok: false, error: 'unsupported_command_type', commandType };
    }
    const normalizedResult = result && typeof result === 'object' ? result : { ok: false, error: 'invalid_command_result' };
    const finalStatus = normalizedResult.ok === true ? 'applied' : 'failed';
    await completeOrderCommand(db, commandId, normalizedResult, finalStatus, normalizedResult.error || '');
    return normalizedResult;
  } catch (err) {
    await retryOrFailOrderCommand(db, row, err);
    return { ok: false, error: 'command_retry_scheduled', retryable: isRetryableD1Error(err) };
  }
}

async function drainOrderCommandQueue(db, authContext = null, opts = {}) {
  const env = opts && opts.env && typeof opts.env === 'object' ? opts.env : {};
  const cfg = getOrderQueueLimitConfig(env);
  const bypassGuards = !!(opts && opts.bypassGuards);
  const now = Date.now();
  const runtime = await readOrderQueueRuntimeState(db);
  if (!bypassGuards) {
    if (Number(runtime.breakerOpenUntil || 0) > now) {
      return 0;
    }
    const pressure = await getOrderQueuePressureSnapshot(db);
    if (pressure.pending >= cfg.pendingMax || pressure.failed >= cfg.failedMax) {
      logWorker('warn', 'order_queue_saturated', {
        pending: pressure.pending,
        failed: pressure.failed,
        pendingMax: cfg.pendingMax,
        failedMax: cfg.failedMax
      });
      return 0;
    }
  }
  if ((opts && opts.recoverStuck !== false) && cfg.stuckRecoverBatchLimit > 0) {
    const recovery = await recoverStuckProcessingCommands(db, authContext, env, {
      limit: Math.max(0, Math.min(cfg.stuckRecoverBatchLimit, Number(opts && opts.stuckRecoverLimit) || cfg.stuckRecoverBatchLimit))
    });
    if (Number(recovery && recovery.recovered) > 0) {
      logWorker('warn', 'order_queue_stuck_processing_recovered', {
        recovered: Number(recovery.recovered) || 0,
        attempted: Number(recovery.attempted) || 0,
        maxAgeMs: Number(recovery.maxAgeMs) || 0
      });
    }
  }

  const limit = Math.max(1, Math.min(30, Number(opts.limit) || ORDER_COMMAND_DRAIN_LIMIT));
  let processed = 0;
  for (let i = 0; i < limit; i++) {
    const cycleNow = Date.now();
    const next = await db.prepare(`
      SELECT * FROM order_commands
      WHERE available_at<=?
        AND (
          status IN ('pending','retry')
          OR (status='processing' AND lease_until<=?)
        )
      ORDER BY available_at ASC, created_at ASC
      LIMIT 1
    `).bind(cycleNow, cycleNow).first();
    if (!next) break;
    const claimed = await tryClaimOrderCommand(db, next.command_id, ORDER_COMMAND_LEASE_MS);
    if (!claimed) continue;
    try {
      await executeClaimedOrderCommand(db, claimed, authContext);
      await markOrderQueueProcessingSuccess(db, env);
    } catch (err) {
      await markOrderQueueProcessingError(db, env);
      logWorker('error', 'order_queue_processing_error', {
        commandId: String(claimed.command_id || ''),
        message: String(err && err.message ? err.message : err || 'unknown')
      });
    }
    processed++;
  }
  if (processed) {
    await maybePruneOrderCommands(db);
  }
  if ((opts && opts.autoRedrive !== false) && cfg.autoRedriveBatchLimit > 0) {
    await maybeAutoRedriveDeadLetters(db, authContext, env, {
      limit: Math.max(0, Math.min(cfg.autoRedriveBatchLimit, Number(opts && opts.autoRedriveLimit) || cfg.autoRedriveBatchLimit))
    });
  }
  await maybePruneOrderCommandDeadLetters(db);
  return processed;
}

async function runOrderCommandDrainCycle(db, authContext = null, opts = {}) {
  await ensureOrderTables(db);
  const limit = Math.max(1, Math.min(100, Number(opts && opts.limit) || ORDER_COMMAND_DRAIN_LIMIT));
  const autoRedrive = opts && opts.autoRedrive !== undefined ? !!opts.autoRedrive : true;
  const autoRedriveLimit = Math.max(0, Math.min(20, Number(opts && opts.autoRedriveLimit) || ORDER_QUEUE_AUTO_REDRIVE_LIMIT));
  const bypassGuards = !!(opts && opts.bypassGuards);
  const processed = await drainOrderCommandQueue(db, authContext, {
    limit,
    env: opts && opts.env ? opts.env : {},
    autoRedrive,
    autoRedriveLimit,
    bypassGuards
  });
  await maybePruneOrderRecoveryOps(db);
  return processed;
}

async function getOrderQueueOpsHealth(db, env = {}) {
  const runtime = await getOrderQueueRuntimeHealth(db, env);
  return {
    breaker: runtime.breaker,
    pressure: runtime.pressure,
    autoRedrive: runtime.autoRedrive
  };
}

async function getOrderCommandStatus(db, commandId) {
  await ensureOrderTables(db);
  const row = await readOrderCommandRecord(db, commandId);
  return toOrderCommandView(row);
}

async function processOrderCommandById(db, commandId, authContext = null, opts = {}) {
  const id = normalizeOpId(commandId);
  if (!id) return { ok: false, error: 'command_id_missing' };
  const startedAt = Date.now();
  const timeoutMs = Math.max(500, Math.min(15_000, Number(opts.timeoutMs) || ORDER_COMMAND_WAIT_TIMEOUT_MS));
  while ((Date.now() - startedAt) < timeoutMs) {
    const current = await readOrderCommandRecord(db, id);
    if (!current) return { ok: false, error: 'command_not_found', commandId: id };
    const status = String(current.status || '').trim().toLowerCase();
    if (status === 'applied' || status === 'failed') {
      return parseOrderCommandResult(current, { ok: status === 'applied', error: current.error || '' }) || { ok: status === 'applied' };
    }
    const claimed = await tryClaimOrderCommand(db, id, ORDER_COMMAND_LEASE_MS);
    if (claimed) {
      await executeClaimedOrderCommand(db, claimed, authContext);
      continue;
    }
    await waitMs(ORDER_COMMAND_WAIT_STEP_MS);
  }
  return { ok: false, error: 'command_timeout', commandId: id };
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
  if (path === '/api/order-queue/runtime' && m === 'GET') return ['Admin'];
  if (path === '/api/order-queue/runtime/control' && m === 'POST') return ['Admin'];
  if (path === '/api/order-queue/auto-redrive-report' && m === 'GET') return ['Admin'];
  if (path === '/api/order-queue/drain' && m === 'POST') return ['Admin'];
  if (path === '/api/backup' && m === 'GET') return ['Admin'];
  if (path === '/api/backup/import' && m === 'POST') return ['Admin'];
  if (path.startsWith('/api/order-command/') && m === 'GET') return ['Admin'];
  if (path.startsWith('/api/order-command/') && m === 'POST') return ['Admin'];
  if (path === '/api/order-deadletters' && m === 'GET') return ['Admin'];
  if (path === '/api/order-deadletters/requeue' && m === 'POST') return ['Admin'];
  if (path === '/api/order-deadletters/clear' && m === 'POST') return ['Admin'];
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
        const { key, value, event } = await request.json();
        return await syncKey(db, key, value, event);
      }
      if (path === '/api/bootstrap' && method === 'GET') {
        return json(await getSyncBootstrapPayload(db, authContext, env));
      }
      if (path === '/api/ops' && method === 'POST') {
        const body = await request.json().catch(() => ({}));
        const response = await processOrderOpsBatch(db, body, authContext, env);
        return json(response);
      }
      if (path === '/api/updates' && method === 'GET') {
        const cursor = String(url.searchParams.get('cursor') || '').trim();
        const limit = Number(url.searchParams.get('limit')) || ORDER_UPDATES_LIMIT_DEFAULT;
        await runOrderCommandDrainCycle(db, authContext, { limit: 6, env });
        const response = await getOrderUpdates(db, cursor, limit);
        return json(response);
      }

      // Orders
      if (path === '/api/orders' && method === 'GET') {
        const sinceTs = Math.max(0, Number(url.searchParams.get('sinceTs')) || 0);
        const sinceV = Math.max(0, Number(url.searchParams.get('sinceV')) || 0);
        await runOrderCommandDrainCycle(db, authContext, { limit: 6, env });
        return await getOrders(db, authContext, { sinceTs, sinceV });
      }
      if (path.startsWith('/api/order-command/') && method === 'GET') {
        const commandId = decodeURIComponent(path.split('/').pop() || '').trim();
        if (!commandId) return json({ error: 'command_id missing' }, 400);
        const command = await getOrderCommandStatus(db, commandId);
        if (!command) return json({ error: 'not_found' }, 404);
        return json({ ok: true, command });
      }
      if (path === '/api/order-deadletters' && method === 'GET') {
        const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit')) || 50));
        const deadletters = await listOrderCommandDeadLetters(db, limit);
        return json({ ok: true, count: deadletters.length, deadletters });
      }
      if (path === '/api/order-queue/runtime' && method === 'GET') {
        const breaker = await getOrderQueueBreakerState(db, env);
        const runtimeHealth = await getOrderQueueRuntimeHealth(db, env);
        const processing = await getOrderQueueProcessingStats(db, env);
        return json({
          ok: true,
          ts: Date.now(),
          breaker,
          autoRedrive: runtimeHealth && runtimeHealth.autoRedrive ? runtimeHealth.autoRedrive : null,
          processing
        });
      }
      if (path === '/api/order-queue/runtime/control' && method === 'POST') {
        const body = await request.json().catch(() => ({}));
        const action = String(body && body.action || '').trim().toLowerCase();
        const requestId = normalizeOpId(
          body && body.requestId,
          (authContext && authContext.requestId) || getRequestIdFromRequest(request)
        );
        const opAction = `runtime_control:${normalizeOpId(action || 'unknown', 'unknown')}`;
        const known = await readOrderRecoveryOpResult(db, opAction, requestId);
        if (known && known.response && typeof known.response === 'object') {
          return json({ ...known.response, idempotent: true, requestId }, Number(known.statusCode) || 200);
        }
        const cfg = getOrderQueueLimitConfig(env);
        const runtime = await readOrderQueueRuntimeState(db);
        let response = null;
        if (action === 'breaker_reset') {
          const next = await writeOrderQueueRuntimeState(db, {
            ...runtime,
            breakerOpenUntil: 0,
            errorEvents: []
          });
          response = {
            ok: true,
            action,
            requestId,
            breakerOpenUntil: Number(next && next.breakerOpenUntil) || 0,
            errorCountInWindow: Array.isArray(next && next.errorEvents) ? next.errorEvents.length : 0
          };
        } else if (action === 'breaker_open') {
          const openMs = toBoundedInt(body && body.cooldownMs, cfg.breakerCooldownMs, 1000, 10 * 60 * 1000);
          const next = await writeOrderQueueRuntimeState(db, {
            ...runtime,
            breakerOpenUntil: Date.now() + openMs
          });
          response = {
            ok: true,
            action,
            requestId,
            breakerOpenUntil: Number(next && next.breakerOpenUntil) || 0,
            cooldownMs: openMs
          };
        } else if (action === 'recover_stuck_processing') {
          const limit = Math.max(1, Math.min(cfg.stuckRecoverBatchLimit || ORDER_QUEUE_STUCK_RECOVER_LIMIT, Number(body && body.limit) || cfg.stuckRecoverBatchLimit || ORDER_QUEUE_STUCK_RECOVER_LIMIT));
          const recovery = await recoverStuckProcessingCommands(db, authContext, env, { limit, audit: true });
          response = {
            ok: true,
            action,
            requestId,
            ...recovery
          };
        } else {
          return json({ ok: false, error: 'unsupported_action', action, allowed: ['breaker_reset', 'breaker_open', 'recover_stuck_processing'] }, 400);
        }
        const runtimeHealth = await getOrderQueueRuntimeHealth(db, env);
        response.queueRuntime = runtimeHealth;
        await storeOrderRecoveryOpResult(db, opAction, requestId, 200, response);
        await maybePruneOrderRecoveryOps(db);
        return json(response);
      }
      if (path === '/api/order-queue/auto-redrive-report' && method === 'GET') {
        const limit = Math.max(1, Math.min(100, Number(url.searchParams.get('limit')) || 20));
        const report = await getOrderQueueAutoRedriveReport(db, limit);
        return json({ ok: true, limit, ...report, count: Array.isArray(report.items) ? report.items.length : 0 });
      }
      if (path === '/api/order-queue/drain' && method === 'POST') {
        const body = await request.json().catch(() => ({}));
        const requestId = normalizeOpId(
          body && body.requestId,
          (authContext && authContext.requestId) || getRequestIdFromRequest(request)
        );
        const known = await readOrderRecoveryOpResult(db, 'force_drain', requestId);
        if (known && known.response && typeof known.response === 'object') {
          return json({ ...known.response, idempotent: true, requestId }, Number(known.statusCode) || 200);
        }
        const cfg = getOrderQueueLimitConfig(env);
        const limit = Math.max(1, Math.min(cfg.forceDrainMaxLimit, Number(body && body.limit) || ORDER_COMMAND_DRAIN_LIMIT));
        const force = body && body.force === true;
        const autoRedrive = body && body.autoRedrive === true;
        const autoRedriveLimit = Math.max(0, Math.min(cfg.autoRedriveBatchLimit, Number(body && body.autoRedriveLimit) || cfg.autoRedriveBatchLimit));
        const startedAt = Date.now();
        const processed = await runOrderCommandDrainCycle(db, authContext, {
          limit,
          env,
          autoRedrive,
          autoRedriveLimit,
          bypassGuards: force
        });
        const queueRuntime = await getOrderQueueRuntimeHealth(db, env);
        const response = {
          ok: true,
          requestId,
          processed,
          limit,
          force,
          autoRedrive,
          autoRedriveLimit,
          durationMs: Math.max(0, Date.now() - startedAt),
          queueRuntime
        };
        await storeOrderRecoveryOpResult(db, 'force_drain', requestId, 200, response);
        await maybePruneOrderRecoveryOps(db);
        return json(response);
      }
      if (path.startsWith('/api/order-command/') && method === 'POST') {
        const commandId = decodeURIComponent(path.split('/').pop() || '').trim();
        if (!commandId) return json({ error: 'command_id missing' }, 400);
        const body = await request.json().catch(() => ({}));
        const requestId = normalizeOpId(
          body && body.requestId,
          (authContext && authContext.requestId) || getRequestIdFromRequest(request)
        );
        const recoveryAction = `redrive_one:${normalizeOpId(commandId, 'unknown')}`;
        const known = await readOrderRecoveryOpResult(db, recoveryAction, requestId);
        if (known && known.response && typeof known.response === 'object') {
          return json({ ...known.response, idempotent: true, requestId }, Number(known.statusCode) || 200);
        }
        const delayMs = Math.max(0, Math.min(60_000, Number(body && body.delayMs) || 0));
        const result = await redriveOrderCommandById(db, commandId, authContext, { delayMs, requestId });
        const statusCode = result && result.ok ? 200 : 400;
        const response = { ...result, requestId };
        await storeOrderRecoveryOpResult(db, recoveryAction, requestId, statusCode, response);
        await maybePruneOrderRecoveryOps(db);
        return json(response, statusCode);
      }
      if (path === '/api/order-deadletters/requeue' && method === 'POST') {
        const body = await request.json().catch(() => ({}));
        const requestId = normalizeOpId(
          body && body.requestId,
          (authContext && authContext.requestId) || getRequestIdFromRequest(request)
        );
        const known = await readOrderRecoveryOpResult(db, 'redrive_bulk', requestId);
        if (known && known.response && typeof known.response === 'object') {
          return json({ ...known.response, idempotent: true, requestId }, Number(known.statusCode) || 200);
        }
        const commandIds = Array.isArray(body && body.commandIds) ? body.commandIds : [];
        const delayMs = Math.max(0, Math.min(60_000, Number(body && body.delayMs) || 0));
        const result = await redriveOrderCommandsBulk(db, commandIds, authContext, { delayMs, maxItems: 200, requestId });
        const response = { ok: true, ...result, requestId };
        await storeOrderRecoveryOpResult(db, 'redrive_bulk', requestId, 200, response);
        await maybePruneOrderRecoveryOps(db);
        return json(response);
      }
      if (path === '/api/order-deadletters/clear' && method === 'POST') {
        const body = await request.json().catch(() => ({}));
        const requestId = normalizeOpId(
          body && body.requestId,
          (authContext && authContext.requestId) || getRequestIdFromRequest(request)
        );
        const known = await readOrderRecoveryOpResult(db, 'deadletter_clear', requestId);
        if (known && known.response && typeof known.response === 'object') {
          return json({ ...known.response, idempotent: true, requestId }, Number(known.statusCode) || 200);
        }
        const commandIds = Array.isArray(body && body.commandIds) ? body.commandIds : [];
        const normalized = [...new Set(commandIds.map((id) => normalizeOpId(id)).filter(Boolean))];
        for (const id of normalized) {
          await removeOrderCommandDeadLetterById(db, id);
        }
        const response = { ok: true, requestId, cleared: normalized.length };
        await recordOrderAuditEvent(db, 'order_deadletter_cleared', '__bulk__', {
          requestId,
          cleared: normalized.length,
          commandIdsSample: normalized.slice(0, 20)
        }, authContext);
        await storeOrderRecoveryOpResult(db, 'deadletter_clear', requestId, 200, response);
        await maybePruneOrderRecoveryOps(db);
        return json(response);
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
        return await saveOrderDelete(db, id, req, authContext);
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
        const queueRuntime = await getOrderQueueRuntimeHealth(db, env);
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
        const commandQueue = await db.prepare(`
          SELECT
            COUNT(*) AS pending_count,
            MIN(created_at) AS oldest_created_at
          FROM order_commands
          WHERE status IN ('pending','retry','processing')
        `).first();
        const queueLatencyRows = await db.prepare(`
          SELECT created_at AS ts
          FROM order_commands
          WHERE status IN ('pending','retry','processing')
          ORDER BY created_at DESC
          LIMIT 500
        `).all();
        const commandFailed = await db.prepare(`
          SELECT
            COUNT(*) AS failed_count,
            MIN(updated_at) AS oldest_failed_at
          FROM order_commands
          WHERE status='failed'
        `).first();
        const failedLatencyRows = await db.prepare(`
          SELECT updated_at AS ts
          FROM order_commands
          WHERE status='failed'
          ORDER BY updated_at DESC
          LIMIT 500
        `).all();
        const deadLetters = await db.prepare(`
          SELECT
            COUNT(*) AS deadletter_count,
            MAX(COALESCE(CAST(json_extract(value,'$.failedAt') AS INTEGER),0)) AS latest_failed_at
          FROM kv_store
          WHERE key LIKE 'order_deadletter:%'
        `).first();
        const nowForLatency = Date.now();
        const queueLatency = buildLatencyPercentiles((queueLatencyRows && queueLatencyRows.results) || [], 'ts', nowForLatency);
        const failedLatency = buildLatencyPercentiles((failedLatencyRows && failedLatencyRows.results) || [], 'ts', nowForLatency);
        const processingStats = await getOrderQueueProcessingStats(db, env);
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
          commandQueue: {
            pending: Number(commandQueue && commandQueue.pending_count) || 0,
            oldestCreatedAt: Number(commandQueue && commandQueue.oldest_created_at) || 0,
            failed: Number(commandFailed && commandFailed.failed_count) || 0,
            oldestFailedAt: Number(commandFailed && commandFailed.oldest_failed_at) || 0,
            latencyMs: queueLatency,
            failedAgeMs: failedLatency,
            processing: processingStats,
            pressure: queueRuntime && queueRuntime.pressure ? queueRuntime.pressure : null,
            circuitBreaker: queueRuntime && queueRuntime.breaker ? queueRuntime.breaker : null,
            autoRedrive: queueRuntime && queueRuntime.autoRedrive ? queueRuntime.autoRedrive : null
          },
          deadLetters: {
            total: Number(deadLetters && deadLetters.deadletter_count) || 0,
            latestFailedAt: Number(deadLetters && deadLetters.latest_failed_at) || 0
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
  ,
  async scheduled(_event, env, _ctx) {
    const db = env && env.DB ? env.DB : null;
    if (!db) return;
    try {
      await runOrderCommandDrainCycle(db, null, { limit: 25, env });
    } catch (_e) {}
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

function getSyncMetaStoreKey(key) {
  return `sync_meta:${String(key || '').slice(0, 180)}`;
}

function parseSyncEventMeta(event, key) {
  if (!event || typeof event !== 'object') return null;
  const eventKey = String(event.key || key || '').trim();
  if (!eventKey) return null;
  if (String(key || '').trim() && eventKey !== String(key).trim()) return null;
  const opId = normalizeOpId(event.opId || event.id || event.lastOpId, '');
  if (!opId) return null;
  return {
    key: eventKey,
    opId,
    v: Math.max(1, Number(event.v) || 1),
    ts: Math.max(1, Number(event.ts) || Date.now())
  };
}

function normalizeChatMessageForStore(msg) {
  const base = msg && typeof msg === 'object' ? msg : {};
  const ts = Number(base.ts) || Date.now();
  const from = String(base.from || '');
  const to = String(base.to || '');
  const text = String(base.text || '');
  const id = base.id
    ? String(base.id)
    : `${from}|${to}|${ts}|${text.slice(0, 120)}`;
  return {
    ...base,
    id,
    from,
    to,
    text,
    ts,
    updatedAt: Number(base.updatedAt) || ts,
    v: Math.max(1, Number(base.v) || 1),
    read: !!base.read
  };
}

function mergeChatMessagesConservative(existingRaw, incomingRaw) {
  const existing = Array.isArray(existingRaw) ? existingRaw : [];
  const incoming = Array.isArray(incomingRaw) ? incomingRaw : [];
  const byId = new Map();
  const mergeOne = (raw) => {
    const msg = normalizeChatMessageForStore(raw);
    const prev = byId.get(msg.id);
    if (!prev) {
      byId.set(msg.id, msg);
      return;
    }
    const prevV = Number(prev.v) || 0;
    const nextV = Number(msg.v) || 0;
    const prevTs = Number(prev.updatedAt || prev.ts) || 0;
    const nextTs = Number(msg.updatedAt || msg.ts) || 0;
    const winner = (nextV > prevV || (nextV === prevV && nextTs >= prevTs))
      ? { ...prev, ...msg }
      : { ...msg, ...prev };
    // "read=true" should be monotonic; once read, never regress.
    winner.read = !!(prev.read || msg.read);
    byId.set(msg.id, winner);
  };
  existing.forEach(mergeOne);
  incoming.forEach(mergeOne);
  return [...byId.values()].sort((a, b) => (Number(a.ts) || 0) - (Number(b.ts) || 0));
}

async function syncKey(db, key, value, event = null) {
  const normalizedKey = String(key || '').trim();
  if (!normalizedKey) return json({ error: 'key missing' }, 400);
  const eventMeta = parseSyncEventMeta(event, normalizedKey);
  const metaKey = getSyncMetaStoreKey(normalizedKey);
  let previousMeta = null;
  if (eventMeta) {
    const metaRow = await db.prepare('SELECT value FROM kv_store WHERE key=?').bind(metaKey).first();
    previousMeta = safeParseJson(metaRow && metaRow.value, null);
    if (previousMeta && previousMeta.opId && String(previousMeta.opId) === String(eventMeta.opId)) {
      return json({ ok: true, idempotent: true, opId: eventMeta.opId, key: normalizedKey });
    }
    if (
      previousMeta &&
      (
        Number(previousMeta.v || 0) > Number(eventMeta.v || 0) ||
        (
          Number(previousMeta.v || 0) === Number(eventMeta.v || 0) &&
          Number(previousMeta.ts || 0) > Number(eventMeta.ts || 0)
        )
      )
    ) {
      return json({
        ok: true,
        staleIgnored: true,
        key: normalizedKey,
        ackV: Number(previousMeta.v || 0),
        opId: String(previousMeta.opId || ''),
        shouldRefetch: true
      });
    }
  }

  if (normalizedKey === 'jb_users') {
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
    if (eventMeta) {
      await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
        .bind(metaKey, JSON.stringify({ ...eventMeta, updatedAt: Date.now() })).run();
    }
    return json({ ok: true, users: nextUsers.length });
  }

  if (normalizedKey.startsWith('jb_chat_')) {
    let existingChat = [];
    const row = await db.prepare('SELECT value FROM kv_store WHERE key=?').bind(normalizedKey).first();
    if (row && row.value) {
      const parsed = safeParseJson(row.value, []);
      existingChat = Array.isArray(parsed) ? parsed : [];
    }
    const mergedChat = mergeChatMessagesConservative(existingChat, value);
    await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
      .bind(normalizedKey, JSON.stringify(mergedChat)).run();
    if (eventMeta) {
      await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
        .bind(metaKey, JSON.stringify({ ...eventMeta, updatedAt: Date.now() })).run();
    }
    return json({ ok: true, key: normalizedKey, messages: mergedChat.length, merged: true });
  }

  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind(normalizedKey, JSON.stringify(value)).run();
  if (eventMeta) {
    await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
      .bind(metaKey, JSON.stringify({ ...eventMeta, updatedAt: Date.now() })).run();
  }
  return json({ ok: true, key: normalizedKey });
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
    const orders = tableOrders
      .filter(order => !(order && order.deletedAt))
      .sort((a, b) => (Number(b.ts) || 0) - (Number(a.ts) || 0));
    const incremental = !!(sinceTs > 0 || sinceV > 0);
    const filteredOrders = incremental
      ? orders.filter((order) => {
          const ov = Number(order && order.v) || 0;
          const ots = Number(order && (order.updatedAt || order.ts)) || 0;
          if (sinceV <= 0 && sinceTs <= 0) return true;
          if (sinceV > 0 && sinceTs > 0) return ov > sinceV || ots > sinceTs;
          if (sinceV > 0) return ov > sinceV;
          return ots > sinceTs;
        })
      : orders;
    logWorker('info', 'orders_list_served', {
      tableCount: tableOrders.length,
      mergedCount: orders.length,
      resultCount: filteredOrders.length,
      incremental,
      sinceTs,
      sinceV,
      latencyMs: Date.now() - startedAt
    });
    await bumpSecurityMetric(db, 'orders_list_served', 1);
    const nextCursor = await getLatestSyncCursor(db);
    return json({
      orders: filteredOrders,
      incremental,
      sinceTs,
      sinceV,
      nextCursor
    });
  } catch(e) {
    logWorker('error', 'orders_list_failed', {
      message: String(e && e.message ? e.message : e || 'unknown'),
      latencyMs: Date.now() - startedAt
    });
    return json({ orders: [], error: e.message });
  }
}

function toSyncCursor(ts, v) {
  return `${Math.max(0, Number(ts) || 0)}:${Math.max(0, Number(v) || 0)}`;
}

function parseSyncCursor(raw) {
  const text = String(raw || '').trim();
  if (!text) return { ts: 0, v: 0 };
  const normalized = text.includes('|') ? text.replace('|', ':') : text;
  const [tsPart, vPart] = normalized.split(':');
  return {
    ts: Math.max(0, Number(tsPart) || 0),
    v: Math.max(0, Number(vPart) || 0)
  };
}

async function getLatestSyncCursor(db) {
  await ensureOrderTables(db);
  await ensureSecurityTables(db);
  const orderRow = await db.prepare(`
    SELECT
      MAX(
        COALESCE(
          CAST(json_extract(data,'$.updatedAt') AS INTEGER),
          COALESCE(CAST(json_extract(data,'$.ts') AS INTEGER),0),
          ts
        )
      ) AS max_ts,
      MAX(COALESCE(CAST(json_extract(data,'$.v') AS INTEGER),0)) AS max_v
    FROM orders
  `).first();
  const auditRow = await db.prepare('SELECT MAX(created_at) AS max_audit_ts FROM order_audit_events').first();
  const maxTs = Math.max(
    Number(orderRow && orderRow.max_ts) || 0,
    Number(auditRow && auditRow.max_audit_ts) || 0
  );
  const maxV = Math.max(0, Number(orderRow && orderRow.max_v) || 0);
  return toSyncCursor(maxTs, maxV);
}

async function getSyncBootstrapPayload(db, authContext = null, env = {}) {
  const runtime = await getOrderQueueRuntimeHealth(db, env);
  const nextCursor = await getLatestSyncCursor(db);
  return {
    ok: true,
    ts: Date.now(),
    session: {
      actorId: String(authContext && authContext.actorId || 'anonymous'),
      role: String(authContext && authContext.role || 'Guest'),
      isAuthenticated: !!(authContext && authContext.isAuthenticated),
      authMode: String(authContext && authContext.authMode || 'open')
    },
    config: {
      opsBatchMax: toBoundedInt(env.ORDER_OPS_BATCH_MAX, ORDER_OPS_BATCH_MAX, 1, 200),
      updatesLimitDefault: toBoundedInt(env.ORDER_UPDATES_LIMIT_DEFAULT, ORDER_UPDATES_LIMIT_DEFAULT, 20, ORDER_UPDATES_LIMIT_MAX),
      updatesLimitMax: toBoundedInt(env.ORDER_UPDATES_LIMIT_MAX, ORDER_UPDATES_LIMIT_MAX, 50, 2000)
    },
    queue: {
      pressure: runtime && runtime.pressure ? runtime.pressure : null,
      circuitBreaker: runtime && runtime.breaker ? runtime.breaker : null
    },
    initialCursor: nextCursor
  };
}

function normalizeOperationType(op) {
  const text = String(op && op.type || '').trim().toUpperCase();
  if (text === 'CREATE_ORDER' || text === 'UPDATE_ORDER' || text === 'UPSERT_ORDER') return 'upsert';
  if (text === 'DELETE_ORDER' || text === 'REMOVE_ORDER') return 'delete';
  return '';
}

async function readJsonResponseSafe(res) {
  if (!res || typeof res !== 'object') return { status: 500, body: { ok: false, error: 'invalid_response' } };
  let body = {};
  try {
    body = await res.json();
  } catch {
    body = {};
  }
  return {
    status: Number(res.status) || 500,
    body: body && typeof body === 'object' ? body : {}
  };
}

function buildUpsertPayloadFromOperation(opEnvelope) {
  const payload = opEnvelope && opEnvelope.payload && typeof opEnvelope.payload === 'object'
    ? opEnvelope.payload
    : {};
  const orderSource = payload.order && typeof payload.order === 'object' ? payload.order : payload;
  const entityId = String(opEnvelope.entityId || orderSource.id || '').trim();
  if (!entityId) return null;
  const baseVersion = Math.max(0, Number(opEnvelope.baseVersion) || 0);
  const incomingTs = Number(payload.updatedAt || payload.ts || orderSource.updatedAt || orderSource.ts) || Date.now();
  const requestedV = Math.max(1, Number(orderSource.v) || 0, baseVersion + 1);
  return {
    order: {
      ...orderSource,
      id: entityId,
      v: requestedV,
      updatedAt: incomingTs,
      ts: Number(orderSource.ts) || incomingTs,
      opId: opEnvelope.opId,
      lastOpId: opEnvelope.opId
    },
    opId: opEnvelope.opId,
    requestId: opEnvelope.opId,
    v: requestedV,
    ts: incomingTs
  };
}

function buildDeletePayloadFromOperation(opEnvelope) {
  const payload = opEnvelope && opEnvelope.payload && typeof opEnvelope.payload === 'object'
    ? opEnvelope.payload
    : {};
  const entityId = String(opEnvelope.entityId || payload.entityId || payload.id || '').trim();
  if (!entityId) return null;
  const baseVersion = Math.max(0, Number(opEnvelope.baseVersion) || 0);
  const deletedAt = Number(payload.deletedAt || payload.ts) || Date.now();
  const nextV = Math.max(1, Number(payload.v) || 0, baseVersion + 1);
  return {
    id: entityId,
    req: {
      opId: opEnvelope.opId,
      requestId: opEnvelope.opId,
      v: nextV,
      deletedAt
    }
  };
}

async function applyOrderOperationEnvelope(db, opEnvelope, authContext = null) {
  const opType = normalizeOperationType(opEnvelope);
  if (!opType) {
    return { kind: 'rejected', rejected: { opId: opEnvelope.opId, reason: 'UNSUPPORTED_OPERATION' } };
  }
  if (opType === 'upsert') {
    const payload = buildUpsertPayloadFromOperation(opEnvelope);
    if (!payload || !payload.order || !payload.order.id) {
      return { kind: 'rejected', rejected: { opId: opEnvelope.opId, reason: 'INVALID_PAYLOAD' } };
    }
    const response = await saveOrder(db, payload, authContext);
    const parsed = await readJsonResponseSafe(response);
    const body = parsed.body || {};
    if (parsed.status >= 500 || body.ok === false) {
      return {
        kind: 'failed',
        failed: {
          opId: opEnvelope.opId,
          reason: String(body.error || 'TRANSIENT_ERROR'),
          retryable: true
        }
      };
    }
    if (body.staleIgnored || (body.conflict && typeof body.conflict === 'object')) {
      return {
        kind: 'rejected',
        rejected: {
          opId: opEnvelope.opId,
          reason: 'VERSION_CONFLICT',
          serverVersion: Math.max(0, Number(body.ackV) || 0)
        }
      };
    }
    return { kind: 'acked', acked: { opId: opEnvelope.opId } };
  }
  const payload = buildDeletePayloadFromOperation(opEnvelope);
  if (!payload || !payload.id) {
    return { kind: 'rejected', rejected: { opId: opEnvelope.opId, reason: 'INVALID_PAYLOAD' } };
  }
  const response = await saveOrderDelete(db, payload.id, payload.req, authContext);
  const parsed = await readJsonResponseSafe(response);
  const body = parsed.body || {};
  if (parsed.status >= 500 || body.ok === false) {
    return {
      kind: 'failed',
      failed: {
        opId: opEnvelope.opId,
        reason: String(body.error || 'TRANSIENT_ERROR'),
        retryable: true
      }
    };
  }
  if (body.staleIgnored || (body.conflict && typeof body.conflict === 'object')) {
    return {
      kind: 'rejected',
      rejected: {
        opId: opEnvelope.opId,
        reason: 'VERSION_CONFLICT',
        serverVersion: Math.max(0, Number(body.ackV) || 0)
      }
    };
  }
  return { kind: 'acked', acked: { opId: opEnvelope.opId } };
}

async function processOrderOpsBatch(db, body, authContext = null, env = {}) {
  const maxBatch = toBoundedInt(env.ORDER_OPS_BATCH_MAX, ORDER_OPS_BATCH_MAX, 1, 200);
  const input = Array.isArray(body && body.operations) ? body.operations : [];
  const operations = input.slice(0, maxBatch);
  const acked = [];
  const rejected = [];
  const failed = [];
  for (const raw of operations) {
    const normalized = normalizeOperationInput(raw);
    const opId = normalizeOpId(normalized && normalized.opId);
    if (!opId) {
      rejected.push({ opId: '', reason: 'OP_ID_MISSING' });
      continue;
    }
    const opEnvelope = {
      opId,
      type: String(normalized.type || '').trim(),
      entityId: String(normalized.entityId || '').trim(),
      payload: normalized.payload && typeof normalized.payload === 'object' ? normalized.payload : {},
      baseVersion: Math.max(0, Number(normalized.baseVersion) || 0),
      createdAt: Math.max(0, Number(normalized.createdAt) || Date.now())
    };
    const known = await readOrderOpRecord(db, opId);
    if (known) {
      const cached = safeParseJson(known.response, {});
      if (cached && (cached.staleIgnored || cached.conflict)) {
        rejected.push({
          opId,
          reason: 'VERSION_CONFLICT',
          serverVersion: Math.max(0, Number(cached.ackV) || 0)
        });
      } else {
        acked.push({ opId });
      }
      continue;
    }
    const applied = await applyOrderOperationEnvelope(db, opEnvelope, authContext);
    if (applied.kind === 'acked') acked.push(applied.acked);
    else if (applied.kind === 'rejected') rejected.push(applied.rejected);
    else failed.push(applied.failed);
  }
  const nextCursor = await getLatestSyncCursor(db);
  return {
    ok: true,
    received: input.length,
    processed: operations.length,
    acked,
    rejected,
    failed,
    nextCursor
  };
}

function normalizeOperationInput(raw) {
  const src = raw && typeof raw === 'object' ? raw : {};
  return {
    opId: normalizeOpId(src.opId || src.id || src.requestId),
    type: String(src.type || src.action || '').trim(),
    entityId: String(src.entityId || src.orderId || src.id || '').trim(),
    payload: src.payload && typeof src.payload === 'object'
      ? src.payload
      : (src.order && typeof src.order === 'object' ? { order: src.order } : {}),
    baseVersion: Math.max(0, Number(src.baseVersion || src.v || src.version) || 0),
    createdAt: Math.max(0, Number(src.createdAt || src.ts) || Date.now())
  };
}

async function getOrderUpdates(db, cursorRaw, limitRaw) {
  await ensureOrderTables(db);
  await ensureSecurityTables(db);
  const cursor = parseSyncCursor(cursorRaw);
  const limit = Math.max(
    1,
    Math.min(
      ORDER_UPDATES_LIMIT_MAX,
      Number(limitRaw) || ORDER_UPDATES_LIMIT_DEFAULT
    )
  );
  const orderLimit = Math.max(1, Math.floor(limit * 0.7));
  const auditLimit = Math.max(1, limit - orderLimit);
  const rows = await db.prepare(`
    SELECT
      data,
      COALESCE(
        CAST(json_extract(data,'$.updatedAt') AS INTEGER),
        COALESCE(CAST(json_extract(data,'$.ts') AS INTEGER),0),
        ts
      ) AS updated_at,
      COALESCE(CAST(json_extract(data,'$.v') AS INTEGER),0) AS version
    FROM orders
    WHERE
      COALESCE(
        CAST(json_extract(data,'$.updatedAt') AS INTEGER),
        COALESCE(CAST(json_extract(data,'$.ts') AS INTEGER),0),
        ts
      ) > ?
      OR COALESCE(CAST(json_extract(data,'$.v') AS INTEGER),0) > ?
    ORDER BY updated_at ASC, version ASC
    LIMIT ?
  `).bind(cursor.ts, cursor.v, orderLimit).all();
  const orderEvents = (rows.results || [])
    .map((row) => {
      const order = safeParseJson(row && row.data, null);
      if (!order || typeof order !== 'object') return null;
      return {
        eventId: normalizeOpId(
          `order_${String(order.id || '').slice(0, 80)}_${Number(row && row.version) || 0}_${Number(row && row.updated_at) || 0}`,
          `order_evt_${Date.now()}`
        ),
        type: 'ORDER_SNAPSHOT',
        entity: 'order',
        entityId: String(order.id || ''),
        ts: Number(row && row.updated_at) || 0,
        version: Number(row && row.version) || 0,
        data: order
      };
    })
    .filter(Boolean);
  const auditRows = await db.prepare(`
    SELECT event_id, order_id, event_type, payload, created_at, request_id, actor_id, actor_role
    FROM order_audit_events
    WHERE created_at > ?
    ORDER BY created_at ASC
    LIMIT ?
  `).bind(cursor.ts, auditLimit).all();
  const auditEvents = (auditRows.results || [])
    .map((row) => ({
      eventId: String(row && row.event_id || ''),
      type: String(row && row.event_type || 'ORDER_EVENT'),
      entity: 'order',
      entityId: String(row && row.order_id || ''),
      ts: Number(row && row.created_at) || 0,
      version: 0,
      payload: safeParseJson(row && row.payload, {}),
      requestId: String(row && row.request_id || ''),
      actorId: String(row && row.actor_id || ''),
      actorRole: String(row && row.actor_role || '')
    }))
    .filter((item) => item.eventId);
  const events = [...orderEvents, ...auditEvents]
    .sort((a, b) => (Number(a.ts) || 0) - (Number(b.ts) || 0))
    .slice(0, limit);
  let maxTs = cursor.ts;
  let maxV = cursor.v;
  for (const event of events) {
    maxTs = Math.max(maxTs, Number(event && event.ts) || 0);
    maxV = Math.max(maxV, Number(event && event.version) || 0);
  }
  const nextCursor = toSyncCursor(maxTs, maxV);
  return {
    ok: true,
    cursor: toSyncCursor(cursor.ts, cursor.v),
    nextCursor,
    count: events.length,
    limit,
    events
  };
}

function buildDeleteCommandPayload(id, req = {}) {
  const nowTs = Date.now();
  const requestedDeletedAt = Number(req && req.deletedAt) || nowTs;
  const requestedV = Number(req && req.v) || 0;
  const requestedOpId = normalizeOpId(req && req.opId, `del_${id}_${requestedV || 0}_${requestedDeletedAt}`);
  return {
    id: String(id || '').trim(),
    deletedAt: requestedDeletedAt,
    v: requestedV,
    opId: requestedOpId
  };
}

async function saveOrderDelete(db, id, req, authContext = null) {
  await ensureOrderTables(db);
  const saturation = await isOrderQueueWriteSaturated(db);
  if (saturation && saturation.saturated) {
    return json({
      ok: false,
      error: 'order_queue_saturated',
      pending: Number(saturation.pressure && saturation.pressure.pending) || 0,
      failed: Number(saturation.pressure && saturation.pressure.failed) || 0,
      pendingMax: Number(saturation.limits && saturation.limits.pendingMax) || ORDER_QUEUE_MAX_PENDING,
      failedMax: Number(saturation.limits && saturation.limits.failedMax) || ORDER_QUEUE_MAX_FAILED
    }, 503);
  }
  const payload = buildDeleteCommandPayload(id, req);
  if (!payload.id) return json({ error: 'id missing' }, 400);

  const knownOp = await readOrderOpRecord(db, payload.opId);
  if (knownOp) {
    const cached = safeParseJson(knownOp.response, null);
    if (cached && typeof cached === 'object') return json({ ...cached, idempotent: true });
  }

  await enqueueOrderCommand(db, {
    commandId: payload.opId,
    opId: payload.opId,
    orderId: payload.id,
    commandType: 'delete',
    payload
  });
  const result = await processOrderCommandById(db, payload.opId, authContext);
  if (!result || result.ok !== true) {
    return json({
      ok: false,
      error: 'order_delete_failed',
      queued: !!(result && result.error === 'command_timeout'),
      detail: result && result.error ? result.error : 'command_failed'
    }, 503);
  }
  return json(result);
}

async function saveOrder(db, payload, authContext = null) {
  await ensureOrderTables(db);
  const saturation = await isOrderQueueWriteSaturated(db);
  if (saturation && saturation.saturated) {
    return json({
      ok: false,
      error: 'order_queue_saturated',
      pending: Number(saturation.pressure && saturation.pressure.pending) || 0,
      failed: Number(saturation.pressure && saturation.pressure.failed) || 0,
      pendingMax: Number(saturation.limits && saturation.limits.pendingMax) || ORDER_QUEUE_MAX_PENDING,
      failedMax: Number(saturation.limits && saturation.limits.failedMax) || ORDER_QUEUE_MAX_FAILED
    }, 503);
  }
  const parsed = parseOrderMutationPayload(payload);
  const rawOrder = parsed.order;
  const validationErr = validateOrderPayload(rawOrder);
  if (validationErr) return json({ error: validationErr }, 400);

  const clean = sanitizeOrderForStorage(rawOrder);
  const id = String(clean && clean.id || '').trim();
  if (!id) return json({ error: 'order id missing' }, 400);

  const requestedV = Math.max(0, Number(parsed.v) || Number(clean && clean.v) || 0);
  const requestedTs = Number(parsed.ts || clean.updatedAt || clean.ts) || Date.now();
  const opId = normalizeOpId(
    parsed.opId || parsed.requestId || (clean && (clean.opId || clean.lastOpId)),
    buildFallbackOpId('upsert', id, Math.max(1, requestedV || 1), requestedTs)
  );
  const knownOp = await readOrderOpRecord(db, opId);
  if (knownOp) {
    const cached = safeParseJson(knownOp.response, null);
    if (cached && typeof cached === 'object') {
      logWorker('info', 'order_upsert_idempotent_hit', { orderId: id, opId });
      return json({ ...cached, idempotent: true });
    }
  }

  await enqueueOrderCommand(db, {
    commandId: opId,
    opId,
    orderId: id,
    commandType: 'upsert',
    payload: {
      order: clean,
      opId,
      requestId: parsed.requestId || opId,
      v: requestedV,
      ts: requestedTs
    }
  });
  const result = await processOrderCommandById(db, opId, authContext);
  if (!result || result.ok !== true) {
    return json({
      ok: false,
      error: 'order_upsert_failed',
      queued: !!(result && result.error === 'command_timeout'),
      detail: result && result.error ? result.error : 'command_failed'
    }, 503);
  }
  return json(result);
}

async function applyOrderDeleteMutation(db, payload, authContext = null) {
  await ensureOrderTables(db);
  const id = String(payload && payload.id || '').trim();
  if (!id) return { ok: false, error: 'id missing' };
  const nowTs = Date.now();
  const requestedDeletedAt = Number(payload && payload.deletedAt) || nowTs;
  const requestedV = Number(payload && payload.v) || 0;
  const requestedOpId = normalizeOpId(payload && payload.opId, `del_${id}_${requestedV || 0}_${requestedDeletedAt}`);

  const knownOp = await readOrderOpRecord(db, requestedOpId);
  if (knownOp) {
    const cached = safeParseJson(knownOp.response, null);
    if (cached && typeof cached === 'object') return { ...cached, idempotent: true };
  }

  const prevOrder = await withD1Retry(
    () => readOrderFromTable(db, id),
    { op: 'order_delete_prev_table', orderId: id, opId: requestedOpId }
  );
  const prevV = Number(prevOrder && prevOrder.v) || 0;
  const prevTs = Number(prevOrder && (prevOrder.updatedAt || prevOrder.ts)) || 0;
  const prevDeleted = !!(prevOrder && prevOrder.deletedAt);
  const nextV = Math.max(1, requestedV, prevV + 1);
  const deletedAt = Math.max(requestedDeletedAt, prevTs, nowTs);

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
    return staleResponse;
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
  const writeRes = await withD1Retry(
    () => db.prepare(`
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
    `).bind(id, deletedAt, JSON.stringify(tombstone), nextV, nextV, deletedAt).run(),
    { op: 'order_delete_write', orderId: id, opId: requestedOpId }
  );
  const changed = Number(writeRes && writeRes.meta && writeRes.meta.changes) || 0;
  if (!changed) {
    const latest = await withD1Retry(
      () => readOrderFromTable(db, id),
      { op: 'order_delete_latest_table', orderId: id, opId: requestedOpId }
    );
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
    return staleResponse;
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
  await maybePruneOrderCommands(db);
  return successResponse;
}

async function applyOrderUpsertMutation(db, payload, authContext = null) {
  const startedAt = Date.now();
  await ensureOrderTables(db);
  const parsed = parseOrderMutationPayload(payload);
  const rawOrder = parsed.order;
  const validationErr = validateOrderPayload(rawOrder);
  if (validationErr) return { ok: false, error: validationErr };

  const clean = sanitizeOrderForStorage(rawOrder);
  const id = String(clean && clean.id || '').trim();
  if (!id) return { ok: false, error: 'order id missing' };

  const prev = await withD1Retry(
    () => readOrderFromTable(db, id),
    { op: 'order_prev_table', orderId: id }
  );
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
      return { ...cached, idempotent: true };
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
    return staleResponse;
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
    const latest = await withD1Retry(
      () => readOrderFromTable(db, id),
      { op: 'order_latest_table', orderId: id, opId }
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
    return conflictResponse;
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
  await maybePruneOrderCommands(db);
  logWorker('info', 'order_upsert_applied', {
    orderId: id,
    opId,
    ackV: incomingV,
    latencyMs: Date.now() - startedAt
  });
  return successResponse;
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
