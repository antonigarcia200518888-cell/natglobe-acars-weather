const PILOT_SHELL_CACHE = 'nga-pilot-shell-2026-07-v2';
const PILOT_SHELL_ASSETS = [
  '/pilot-manifest.webmanifest',
  '/icon-192.png',
  '/icon-512.png',
  '/fonts/computer-says-no.woff2'
];

self.addEventListener('install', event => {
  event.waitUntil(caches.open(PILOT_SHELL_CACHE).then(cache => cache.addAll(PILOT_SHELL_ASSETS)));
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys()
      .then(keys => Promise.all(keys.filter(key => key.startsWith('nga-pilot-shell-') && key !== PILOT_SHELL_CACHE).map(key => caches.delete(key))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', event => {
  const request = event.request;
  if (request.method !== 'GET') return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin || url.pathname.startsWith('/api/')) return;

  if (request.mode === 'navigate' && url.pathname === '/booking-ops') {
    event.respondWith(
      fetch(request)
        .then(async response => {
          if (response.ok && response.headers.get('X-NGA-Pilot-Shell') === '1') {
            const cache = await caches.open(PILOT_SHELL_CACHE);
            await cache.put('/booking-ops', response.clone());
          }
          return response;
        })
        .catch(async () => (await caches.match('/booking-ops')) || Response.error())
    );
    return;
  }

  if (PILOT_SHELL_ASSETS.includes(url.pathname)) {
    event.respondWith(
      caches.match(request).then(cached => cached || fetch(request).then(response => {
        if (response.ok) caches.open(PILOT_SHELL_CACHE).then(cache => cache.put(request, response.clone()));
        return response;
      }))
    );
  }
});
