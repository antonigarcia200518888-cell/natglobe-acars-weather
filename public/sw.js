const CACHE_NAME = 'natglobe-acars-v2';
const ASSETS = [
  '/',
  '/index.html',
  '/manifest.webmanifest',
  '/icon-192.png',
  '/icon-512.png'
];
self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE_NAME).then((cache) => cache.addAll(ASSETS)));
  self.skipWaiting();
});
self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);
  if (event.request.method !== 'GET') return;
  if (url.pathname.startsWith('/api/')) {
    event.respondWith(
      fetch(event.request).catch(async () => {
        if (url.pathname === '/api/report-json') {
          return new Response(JSON.stringify({ error: 'offline' }), { headers: { 'Content-Type': 'application/json' }, status: 503 });
        }
        return new Response('offline', { status: 503 });
      })
    );
    return;
  }
  event.respondWith(
    caches.match(event.request).then((cached) => cached || fetch(event.request).then((resp) => {
      const copy = resp.clone();
      caches.open(CACHE_NAME).then((cache) => cache.put(event.request, copy));
      return resp;
    }).catch(() => caches.match('/index.html')))
  );
});
