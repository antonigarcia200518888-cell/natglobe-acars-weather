self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', event => event.waitUntil(self.clients.claim()));

// Passenger passes intentionally remain online-only: no personal ticket data is cached on the device.
self.addEventListener('fetch', () => {});
