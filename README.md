# İstanbul Macerası — oyun sunucusu

Çocuklar için güvenli, özel odalı çok oyunculu relay + oda sunucusu (WebSocket, protokol v1).

- Başlat: `npm ci --omit=dev && node src/server.js` (port: `PORT` ortam değişkeni, varsayılan 8787)
- Sağlık: `GET /health`
- Test: `npm ci && npm test`
