// ── JRSHOP Service Worker ──
const CACHE_VERSION = 'jrshop-v20260412-cn-jr-ref-v2';
const CACHE_NAME = CACHE_VERSION;

const STATIC_ASSETS = [
  '/',
  '/index.html',
  '/manifest.json',
  '/icon-192.png',
  '/icon-512.png',
  '/js/api.js',
  '/js/auth.js',
  '/js/state.js',
];

// Install
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(STATIC_ASSETS))
      .then(() => self.skipWaiting())
      .catch(err => console.warn('SW install error:', err))
  );
});

// Activate - eski cache sil
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys()
      .then(keys => Promise.all(
        keys.filter(k => k !== CACHE_NAME).map(k => caches.delete(k))
      ))
      .then(() => self.clients.claim())
  );
});

// Fetch
self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // API istekleri - cache'leme, her zaman network
  if (
    url.pathname.startsWith('/api/') ||
    e.request.method !== 'GET'
  ) {
    e.respondWith(
      fetch(e.request).catch(() => new Response(JSON.stringify({error:'Offline'}), {
        status: 503,
        headers: {'Content-Type':'application/json'}
      }))
    );
    return;
  }

  // index.html ve JS modülleri: Network First
  // → Yeni deploy'da hemen güncellenir
  if (
    url.pathname === '/' ||
    url.pathname === '/index.html' ||
    url.pathname.startsWith('/js/')
  ) {
    e.respondWith(
      fetch(e.request)
        .then(response => {
          if (response.ok) {
            const clone = response.clone();
            caches.open(CACHE_NAME).then(cache => cache.put(e.request, clone));
          }
          return response;
        })
        .catch(() => caches.match(e.request))
    );
    return;
  }

  // Diğerleri (ikonlar, manifest): Cache First
  e.respondWith(
    caches.match(e.request).then(cached => {
      if (cached) return cached;
      return fetch(e.request).then(response => {
        if (response.ok) {
          const clone = response.clone();
          caches.open(CACHE_NAME).then(cache => cache.put(e.request, clone));
        }
        return response;
      });
    })
  );
});

// Ana uygulamadan gelen mesaj: skipWaiting
self.addEventListener('message', e => {
  if (e.data === 'skipWaiting') self.skipWaiting();
});

// ── PUSH NOTIFICATIONS ──
self.addEventListener('push', event => {
  const data = event.data ? event.data.json() : {};
  const title = data.title || 'JRSHOP';
  const options = {
    body: data.body || 'You have a new notification',
    icon: '/icon-192.png',
    badge: '/icon-192.png',
    vibrate: [200, 100, 200],
    data: data.data || {},
    requireInteraction: data.requireInteraction || false,
    actions: data.actions || []
  };
  
  event.waitUntil(
    self.registration.showNotification(title, options)
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  
  const data = event.notification.data || {};
  let url = '/';
  
  // Navigate based on notification type
  if(data.type === 'order' && data.orderId){
    url = '/?order=' + data.orderId;
  } else if(data.type === 'chat'){
    url = '/?chat=1';
  } else if(data.type === 'alert'){
    url = '/?alerts=1';
  }
  
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true })
      .then(clientList => {
        // If app is already open, focus it
        for(const client of clientList){
          if(client.url.includes(self.location.origin) && 'focus' in client){
            client.navigate(url);
            return client.focus();
          }
        }
        // Otherwise open new window
        if(clients.openWindow){
          return clients.openWindow(url);
        }
      })
  );
});

self.addEventListener('notificationclose', event => {
  // Optional: Track notification dismissal
  console.log('Notification closed:', event.notification);
});

self.addEventListener('message', event => {
  const data = event && event.data ? event.data : null;
  if (!data || typeof data !== 'object') return;
  if (data.type !== 'SHOW_LOCAL_NOTIFICATION') return;
  const title = data.title || 'JRSHOP';
  const options = {
    body: data.body || 'New activity',
    icon: '/icon-192.png',
    badge: '/icon-192.png',
    tag: data.tag || ('jrshop-' + Date.now()),
    renotify: !!data.renotify,
    requireInteraction: !!data.requireInteraction,
    vibrate: Array.isArray(data.vibrate) ? data.vibrate : [160, 90, 160],
    data: data.data || {}
  };
  event.waitUntil(self.registration.showNotification(title, options));
});
