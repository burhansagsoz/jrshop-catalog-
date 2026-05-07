# JRSHOP – Yapılan Değişiklikler ve Test Notları

Bu dosya, bu oturumda yapılan tüm önemli değişiklikleri ve neden yapıldıklarını özetler.
Amaç: sistemi localde çalıştırırken canlıya yakın şekilde test edebilmek, gereksiz polling / yenilemeleri azaltmak ve hata mesajlarının kaynağını netleştirmek.

---

## 1) Sayfanın sürekli yenilenmesi engellendi

### Problem
Uygulama bazen kendi kendini yeniliyor ya da sürekli sync denemesi yapıyordu.
Bu da hem kullanıcı deneyimini bozuyordu hem de Network sekmesinde çok fazla istek üretiyordu.

### Yapılan değişiklikler
- Service worker update sonrası otomatik reload davranışı yumuşatıldı.
- `controllerchange` event’i artık sayfayı zorla reload etmiyor.
- Böylece uygulama arka planda güncellenince sayfa kullanıcıyı kesintiye uğratmadan çalışabiliyor.

### Etkilenen dosya
- [index.html](index.html)

---

## 2) Gereksiz polling azaltıldı / kaldırıldı

### Problem
Uygulama veri değişti mi diye sık sık kontrol yapıyordu.
Bu da her 1–2 saniyede bir istek oluşmasına sebep oluyordu.

### Yapılan değişiklikler
- Auto-sync akışındaki interval polling büyük ölçüde kaldırıldı.
- `startAutoSync()` artık sadece gerçek zamanlı mekanizmayı başlatıyor.
- `visibilitychange` içinde tekrar tekrar polling başlatan kod temizlendi.
- Chat ve pending order polling davranışları da azaltıldı / kapatıldı.

### Sonuç
- Veri değişmiyorsa ekstra request olmuyor.
- Değişiklik varsa listener / realtime mekanizma devreye giriyor.

### Etkilenen dosya
- [index.html](index.html)

---

## 3) Firebase / Firestore realtime kullanımına geçildi

### Problem
Çoklu kullanıcı senaryosunda admin ekranına reseller order’larının anında düşmesi isteniyordu.
Polling yerine gerçek zamanlı çözüm gerekiyordu.

### Yapılan değişiklikler
- Firebase enabled durumu gerçek ayar üzerinden okunur hale getirildi.
- Firebase açıksa `initFirebaseRealtime()` ile Firestore listener’lar çalışıyor.
- `orders`, `feed` ve `data` koleksiyonları için `onSnapshot` tabanlı realtime akış kullanılıyor.
- Realtime bağlantı sağlamsa gereksiz yeniden başlatmalar azaltıldı.

### Beklenen davranış
- Reseller order girince admin ekranı anında güncellenir.
- Sayfa sürekli yenilenmez.
- Polling yerine Firestore event’leri kullanılır.

### Etkilenen dosya
- [index.html](index.html)

---

## 4) API base yanlış host’a gitme sorunu düzeltildi

### Problem
Bazı API istekleri yanlışlıkla `http://localhost:5500` üzerinden gidiyordu.
Bu yüzden `404 Not Found` alınıyordu.

Örnek:
- `/api/orders`
- `/api/image`
- `/api/data`

### Yapılan değişiklikler
- `getApiBase()` içindeki Firebase koşulu kaldırıldı.
- API base artık yanlışlıkla frontend origin’ine düşmüyor.
- Worker’a gitmesi gereken istekler yeniden doğru base’e yönlendirildi.

### Sonuç
- Local frontend server (5500) sadece statik dosyaları sunar.
- API istekleri worker tarafına gider.

### Etkilenen dosya
- [index.html](index.html)

---

## 5) Görsel yükleme / görsel URL’leri düzeltildi

### Problem
Görsel istekleri bazen relative path ile oluşturuluyordu ve tarayıcı bunları `localhost:5500/api/image` olarak çağırıyordu.
Bu da 404 üretiyordu.

### Yapılan değişiklikler
- `normalizeImageUrlForRender()` fonksiyonu güncellendi.
- `img_[id]` ve `/api/image/...` formatları worker base üzerinden üretiliyor.
- İndirme tarafında da `apiUrl('/api/image/...')` kullanıldı.
- Böylece görseller yanlış host’a gitmiyor.

### Sonuç
- Image upload / render / download daha stabil.
- Görsel URL’leri tek standarda bağlandı.

### Etkilenen dosya
- [index.html](index.html)

---

## 6) Backup import “cloud sync yok” diye durmasın diye düzeltili

### Problem
Data Backup bölümünde import işlemi, cloud sync başarısız olunca kullanıcıya sanki import kullanılmıyormuş gibi davranıyordu.

### Yapılan değişiklikler
- `importBackup()` içinde lokal import adımı korunup öncelikli hale getirildi.
- Cloudflare sync artık **zorunlu** değil, **best-effort** çalışıyor.
- Cloud tarafı başarısız olursa import durmuyor.
- Kullanıcıya sadece bilgilendirici mesaj veriliyor.

### Sonuç
- Backup dosyası localde mutlaka yükleniyor.
- Cloud sync erişilemezse bile veri kaybı olmuyor.

### Etkilenen dosya
- [index.html](index.html)

---

## 7) Cloud sync durum göstergesi neden “Issues detected” oluyor açıklığa kavuştu

### Problem
UI’da “Cloud Sync → Issues detected” mesajı çıkıyordu.
Bu mesajın localde mi yoksa kodda mı oluştuğu belirsizdi.

### Bulgu
Bu durum local olduğu için otomatik çıkmıyor.
`_health.apiErrors > 0` olunca bu label kırmızıya dönüyor.
Yani herhangi bir API hatası bu uyarıyı tetikliyor.

### Nedenler
- yanlış API base
- service worker cache kaynaklı 404
- Firestore / Firebase permission veya connection hataları
- backend tarafında 429 / rate limit

### Sonuç
Bu uyarı bir “local çalışıyor” göstergesi değil, gerçek hata göstergesi.

### Etkilenen dosya
- [index.html](index.html)

---

## 8) Firestore kanalındaki terminate/reconnect döngüsü azaltıldı

### Problem
Firestore Listen kanalında `TYPE=terminate` istekleri görünüyordu.
Bu, listener’ın sık kapanıp yeniden açıldığını gösteriyordu.

### Yapılan değişiklikler
- `startRealtimeMaintenanceLoop()` içindeki zorlayıcı reconnect davranışı yumuşatıldı.
- Listener zaten sağlamsa tekrar `force:true` ile init edilmesi engellendi.

### Sonuç
- Firestore kanalının gereksiz terminate / reconnect üretmesi azalır.
- Realtime bağlantı daha stabil olur.

### Etkilenen dosya
- [index.html](index.html)

---

## 9) Local testte 429 rate limit sorunu giderildi

### Problem
Localde `POST /api/order` isteği 429 Too Many Requests döndürüyordu.
Bu, tek bir işlemden çok tekrar olmuş gibi görünüyordu.

### Yapılan değişiklikler
- Worker’daki rate limit guard, loopback IP’lerde kapatıldı.
- `127.0.0.1`, `::1` ve `localhost` üzerinden gelen local test istekleri artık rate limit’e takılmıyor.

### Sonuç
- Local geliştirme sırasında order yazma testleri 429 yemiyor.
- Canlıdaki rate limit mantığı korunuyor; sadece local geliştirme için gevşetildi.

### Etkilenen dosya
- [worker.js](worker.js)

---

## 10) API route ve method yapısı netleştirildi

### Worker endpoint’leri
- `GET /api/test`
- `POST /api/login`
- `GET /api/data`
- `POST /api/data`
- `POST /api/sync`
- `GET /api/meta`
- `GET /api/stream`
- `POST /api/image`
- `GET /api/image/<id>`
- `POST /api/orders/bulk`
- `POST /api/backup/import`
- `POST /api/order`
- `DELETE /api/order/<id>`

### Not
`/api/order` için `POST`, görsel okumak için `GET /api/image/<id>`, görsel yüklemek için `POST /api/image` kullanılır.

### Etkilenen dosya
- [worker.js](worker.js)

---

## 11) Local çalışma mantığı ve canlıya yakın test yaklaşımı

### Şu anki mantık
- Frontend: `http://localhost:5500`
- Worker/API: `http://localhost:8787`
- Firebase enabled ise Firestore listener çalışır.
- Realtime yoksa eski fallback mekanizmalar devreye girer.

### Canlıya yakın test için öneri
- ayrı staging Firebase projesi kullan
- service worker cache’i temizle
- mümkünse HTTPS üstünden test yap
- iki tarayıcı açıp admin / reseller aynı anda dene

---

## 12) Önemli notlar

- Bu dosyada anlatılan bazı davranışlar canlı dev ortamı ile local ortam arasında farklılık gösterebilir.
- Local testlerde service worker cache, yanlış API base veya rate limit en sık problem çıkaran alanlardır.
- Veri değişmediği halde request atılıyorsa sebep genelde polling veya reconnect döngüsüdür.

---

## Kısacası

Bu oturumda yapılan ana iş:
- gereksiz refresh / polling azaltıldı,
- Firebase realtime davranışı güçlendirildi,
- API route ve host karışıklıkları düzeltildi,
- görsel URL’leri standardize edildi,
- backup import’un cloud hatasında bozulmaması sağlandı,
- local geliştirmede 429 rate limit problemi gevşetildi.

