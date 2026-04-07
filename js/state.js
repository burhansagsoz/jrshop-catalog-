// ════════════════════════════════════════════════════
// JRSHOP State Modülü — Merkezi uygulama durumu
// Global değişken karmaşasını azaltır
// ════════════════════════════════════════════════════

const State = (() => {

  // Özel state nesnesi
  let _data = {
    orders:           [],
    users:            [],
    suppliers:        [],
    catalog:          [],
    catalogCategories:[],
    settings:         {},
    templates:        {},
    notifSettings:    {},
    activityLog:      [],
    me:               null,
    cloudConnected:   false,
  };

  // Değişiklik dinleyicileri
  const _listeners = {};

  function _notify(key) {
    (_listeners[key]  || []).forEach(fn => { try { fn(_data[key]); }       catch(e){} });
    (_listeners['*']  || []).forEach(fn => { try { fn(key, _data[key]); } catch(e){} });
  }

  return {

    // Değer oku
    get(key) {
      return _data[key];
    },

    // Değer yaz (tek key)
    set(key, value) {
      _data[key] = value;
      _notify(key);
    },

    // Birden fazla key güncelle
    update(patch) {
      Object.entries(patch).forEach(([k, v]) => {
        _data[k] = v;
        _notify(k);
      });
    },

    // Değişiklik dinle
    // Kullanım: State.on('orders', orders => renderOrders(orders))
    on(key, fn) {
      if (!_listeners[key]) _listeners[key] = [];
      _listeners[key].push(fn);
      return () => {
        // Dinlemeyi bırak (cleanup)
        _listeners[key] = _listeners[key].filter(f => f !== fn);
      };
    },

    // Tüm state'i al (debug için)
    dump() {
      return JSON.parse(JSON.stringify(_data));
    },

    // LocalStorage'dan yükle
    loadFromStorage(key, fallback = null) {
      try {
        const raw = localStorage.getItem(key);
        const val = raw ? JSON.parse(raw) : fallback;
        if (key.startsWith('jb_')) {
          const stateKey = key.replace('jb_', '');
          if (stateKey in _data) this.set(stateKey, val);
        }
        return val;
      } catch { return fallback; }
    },

    // LocalStorage'a kaydet
    saveToStorage(key, value) {
      try {
        localStorage.setItem(key, JSON.stringify(value));
      } catch(e) {
        console.error('[State] Storage error:', e);
      }
    },

  };

})();
