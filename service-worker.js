self.addEventListener('install', event => {
  console.log("🔧 Service Worker installed");
  event.waitUntil(
    caches.open('mkr1010-cache').then(cache => {
      return cache.addAll([
        'index.html',
        'manifest.json',
        // Add any other essential assets here
      ]);
    })
  );
});

self.addEventListener('fetch', event => {
  event.respondWith(
    caches.match(event.request).then(response => {
      return response || fetch(event.request);
    })
  );
});
