const PILOT_SHELL_CACHE = 'nga-pilot-shell-2026-09-v16';
const PILOT_STYLESHEET_VERSION = '2026-09-03-1';
const PILOT_EFB_STYLESHEETS = [
  `/pilot-efb.css?v=${PILOT_STYLESHEET_VERSION}`,
  `/pilot-app-reference.css?v=${PILOT_STYLESHEET_VERSION}`,
  `/pilot-tools-reference.css?v=${PILOT_STYLESHEET_VERSION}`,
  `/pilot-secondary-reference.css?v=${PILOT_STYLESHEET_VERSION}`,
  `/pilot-ofp-reference.css?v=${PILOT_STYLESHEET_VERSION}`,
  `/pilot-logbook.css?v=${PILOT_STYLESHEET_VERSION}`
];
const PILOT_SHELL_ASSETS = [
  '/pilot-offline.html',
  '/pilot-manifest.webmanifest',
  ...PILOT_EFB_STYLESHEETS,
  '/icon-192.png',
  '/icon-512.png',
  '/piper-render-transparent.png',
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
        .catch(async () => (await caches.match('/booking-ops')) || (await caches.match('/pilot-offline.html')) || Response.error())
    );
    return;
  }

  const shellAsset = PILOT_SHELL_ASSETS.find(asset => new URL(asset, self.location.origin).pathname === url.pathname);
  if (shellAsset) {
    const isEfbStylesheet = PILOT_EFB_STYLESHEETS.some(asset => new URL(asset, self.location.origin).pathname === url.pathname);
    if (isEfbStylesheet) {
      event.respondWith(
        fetch(request)
          .then(response => {
            if (response.ok) caches.open(PILOT_SHELL_CACHE).then(cache => cache.put(request, response.clone()));
            return response;
          })
          .catch(async () => (await caches.match(request)) || (await caches.match(shellAsset)) || Response.error())
      );
      return;
    }
    event.respondWith(
      caches.match(request).then(async cached => cached || (await caches.match(shellAsset)) || fetch(request).then(response => {
        if (response.ok) caches.open(PILOT_SHELL_CACHE).then(cache => cache.put(request, response.clone()));
        return response;
      }))
    );
  }
});
