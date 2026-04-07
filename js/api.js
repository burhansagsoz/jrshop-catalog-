// ════════════════════════════════════════════════════
// JRSHOP API Modülü — Tüm cloud API çağrıları burada
// Backend: Cloudflare Workers + D1
// ════════════════════════════════════════════════════

const API = (() => {

  // Konfigürasyon
  function getConfig() {
    return {
      url: localStorage.getItem('jb_cloud_api') || 'https://jrshop-api.sagsozburhan1.workers.dev',
      key: localStorage.getItem('jb_cloud_key') || 'JrShop2026'
    };
  }

  function isConfigured() {
    return !!getConfig().key;
  }

  // Temel istek fonksiyonu
  async function request(method, path, body = null, isPublic = false) {
    const cfg = getConfig();
    if (!isPublic && !cfg.key) {
      throw new Error('API anahtarı ayarlanmamış. Lütfen Settings > Cloud Sync bölümünden ayarlayın.');
    }

    const opts = {
      method,
      headers: { 'Content-Type': 'application/json' }
    };

    if (!isPublic) {
      opts.headers['Authorization'] = 'Bearer ' + cfg.key;
    }

    if (body !== null) {
      opts.body = JSON.stringify(body);
    }

    try {
      const res = await fetch(cfg.url + path, opts);
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || 'Sunucu hatası: ' + res.status);
      }
      return await res.json();
    } catch (e) {
      if (e.name === 'TypeError') {
        throw new Error('Bağlantı kurulamadı. İnternet bağlantınızı kontrol edin.');
      }
      throw e;
    }
  }

  return {
    // ── Genel ──
    getConfig,
    isConfigured,

    // ── Veri ──
    getData:    ()          => request('GET',  '/api/data'),
    saveData:   (data)      => request('POST', '/api/data', data),
    syncKey:    (key, val)  => request('POST', '/api/sync', { key, value: val }),
    getBackup:  ()          => request('GET',  '/api/backup'),

    // ── Siparişler ──
    getOrders:    ()      => request('GET',    '/api/orders'),
    saveOrder:    (order) => request('POST',   '/api/order',  { order }),
    deleteOrder:  (id)    => request('DELETE', '/api/order/' + id),

    // ── Katalog ──
    saveCatalogPage: (html) => request('POST', '/api/catalog-page', { html }),

    // ── Resim ──
    saveImage:    (data, type) => request('POST', '/api/image', { data, type: type || 'image/jpeg' }),
    getImageUrl:  (id)         => getConfig().url + '/api/image/' + id,

    // ── AI ──
    askAI: (messages, system) => request('POST', '/api/anthropic', {
      model: 'claude-sonnet-4-20250514',
      max_tokens: 1000,
      system: system || 'Sen JRSHOP sipariş yönetim sisteminin AI asistanısın. Türkçe ve kısa cevap ver.',
      messages
    }),
  };

})();

// ── Hata yönetimiyle sarılı API çağrısı ──
// Kullanım: const data = await apiCall(() => API.getData(), 'Veri yüklenemedi');
async function apiCall(fn, errorMsg) {
  try {
    return await fn();
  } catch (e) {
    console.error('[API Error]', e.message);
    if (typeof toast === 'function') {
      toast('❌ ' + (e.message || errorMsg || 'Bir hata oluştu'), 'err');
    }
    return null;
  }
}
