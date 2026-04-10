// Legacy Cloudflare Worker (not used in Firebase-only runtime)

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

const HUALEI = {
  baseUrl: 'http://193.112.161.59:8082',
  username: 'BURHAN',
  password: 'HSD369',
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...CORS, 'Content-Type': 'application/json' },
  });
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
    if (path === '/catalog' && method === 'GET') return await getCatalog(env);
    if (path.startsWith('/api/image/') && method === 'GET') return await getImage(env, path.replace('/api/image/', ''));

    // Setup - public, auth gerekmez
    if (path === '/api/setup' && method === 'GET') {
      try {
        const db = env.DB;
        // Tablolar
        await db.prepare('CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL)').run();
        await db.prepare('CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, ts INTEGER NOT NULL, data TEXT NOT NULL)').run();
        // Admin kullanıcısı ekle (mevcut kullanıcı listesini EZME)
        const adminUser = {"id":"u1","email":"joobuyadmin@gmail.com","password":"joobuy1212.","role":"Admin","dname":"Admin"};
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
        const hasAdmin = users.some(u => u && String(u.email || '').toLowerCase() === String(adminUser.email).toLowerCase());
        if (!hasAdmin) users.unshift(adminUser);
        await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
          .bind('jb_users', JSON.stringify(users)).run();
        return json({ ok: true, message: 'Setup tamam! Kullanıcı listesi korundu.', usersCount: users.length });
      } catch(e) { return json({ error: e.message }, 500); }
    }

    // Auth devre dışı

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
      if (path === '/api/orders' && method === 'GET') return await getOrders(db);
      if (path === '/api/order' && method === 'POST') {
        const { order } = await request.json();
        return await saveOrder(db, order);
      }
      if (path === '/api/order-event' && method === 'POST') {
        const event = await request.json();
        return await saveOrderEvent(db, event);
      }
      if (path.startsWith('/api/order/') && method === 'DELETE') {
        const id = path.split('/').pop();
        await db.prepare('DELETE FROM orders WHERE id=?').bind(id).run();
        return json({ ok: true });
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
        try {
          const res = await fetch(`${HUALEI.baseUrl}/selectAuth.htm`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: new URLSearchParams({ username: HUALEI.username, password: HUALEI.password }),
          });
          const text = await res.text();
          let data; try { data = JSON.parse(text); } catch { data = { raw: text }; }
          return json({ ok: true, data });
        } catch(e) { return json({ ok: false, error: e.message }); }
      }

      if (path === '/api/logistics/create' && method === 'POST') {
        const { order } = await request.json();
        try {
          const params = new URLSearchParams({
            username: HUALEI.username, password: HUALEI.password,
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
          const res = await fetch(`${HUALEI.baseUrl}/createOrderApi.htm`, {
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
        const no = url.searchParams.get('no') || '';
        return Response.redirect(`http://193.112.161.59:8089/order/FastRpt/PDF_NEW.aspx?no=${no}`, 302);
      }

      if (path === '/api/logistics/track' && method === 'POST') {
        const { trackingNo } = await request.json();
        try {
          const res = await fetch(`${HUALEI.baseUrl}/trackOrder.htm?no=${trackingNo}&username=${HUALEI.username}&password=${HUALEI.password}`);
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
  return json({ data });
}

async function syncKey(db, key, value) {
  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind(key, JSON.stringify(value)).run();
  return json({ ok: true });
}

async function getOrders(db) {
  try {
    // orders tablosundan dene
    await db.prepare("CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY, ts INTEGER NOT NULL, data TEXT NOT NULL)").run();
    const rows = await db.prepare('SELECT * FROM orders ORDER BY ts DESC').all();
    let orders = rows.results.map(r => { try { return JSON.parse(r.data); } catch { return null; } }).filter(Boolean);
    // orders tablosu boşsa kv_store'dan dene
    if(!orders.length){
      const kv = await db.prepare("SELECT value FROM kv_store WHERE key='jb_orders'").first();
      if(kv) { try { orders = JSON.parse(kv.value) || []; } catch {} }
    }
    return json({ orders });
  } catch(e) {
    return json({ orders: [], error: e.message });
  }
}

async function saveOrder(db, order) {
  const clean = { ...order, products: (order.products||[]).map(p => ({ ...p, img: (p.img&&p.img.startsWith('http'))?p.img:null })) };
  await db.prepare('INSERT INTO orders (id,ts,data) VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET data=excluded.data, ts=excluded.ts')
    .bind(order.id, order.ts || Date.now(), JSON.stringify(clean)).run();
  return json({ ok: true });
}

async function saveOrderEvent(db, event) {
  const ev = event && typeof event === 'object' ? event : {};
  const ts = Number(ev.ts) || Date.now();
  const id = String(ev.id || ev.opId || ('evt_' + ts.toString(36) + '_' + Math.random().toString(36).slice(2, 8)));
  await db.prepare('INSERT INTO kv_store (key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .bind('order_event_' + id, JSON.stringify({ ...ev, id, ts })).run();
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
