# JRSHOP — Sistem Çalışma Mantığı

## Özet
Bu belge, proje mimarisinin ve önemli çalışma akışlarının kısa bir özeti ve üretime alma öncesi kontrol listesidir. Amaç: frontend tarafındaki ağır `localStorage` kullanımını azaltmak, veriyi Cloudflare Worker + D1 merkezli "server-first" modeline geçirmek ve senkronizasyonu güvenli/tekrarlanabilir hale getirmektir.

## Mimari (yüksek seviyede)
- Frontend: Tek sayfa uygulama (vanilla JS) — `index.html` içinde tüm UI, sync ve state yönetimi.
- Backend: Cloudflare Worker + D1 (SQLite-benzeri) tablo yapısı. Worker, `/api/*` endpoint'leri ile yetkili veri sağlayıcıdır.
- Cache/ops: Okunma ağırlıklı, sık değişmeyen veriler için Cache API veya Workers KV önerilir; D1 okuma/yazma limitleri göz önünde tutulmalı.

## Ana Endpoints
- `GET/POST /api/data` — Toplu anahtar (jb_*) okuma/yazma. Frontend `writeLocalJson` ile buraya yazar.
- `GET /api/orders` — Sipariş snapshot/pagination (yetkili). `applyAuthoritativeOrdersSnapshot` ile frontend tarafından kullanılır.
- `POST /api/ops` — Operasyon bazlı yazma (opId, idempotent işleme).
- `GET /api/image/:id` — Ürün/sipariş resimlerini servis eder (404 varsa frontend cache'e alınıp tekrar denenmeyecek).

## Frontend Davranışı (Server-First)
- Başlangıçta `hydrateSharedStateServerFirst()` ile gerekli `jb_*` anahtarları topluca `/api/data?keys=` çağrısıyla alınır.
- Tüm `save*()` fonksiyonları (`saveUsers()`, `saveCatalog()`, vb.) artık `writeLocalJson(key,value)` çağırır; bu fonksiyon önce `/api/data`'ya yazar, başarısızsa fallback olarak `localStorage` kullanır.
- `jb_orders` gibi büyük snapshot'lar artık tarayıcıya tam kaydedilmiyor. Yerine `persistOrdersLocalSafe(orders)` sadece küçük meta (adet + ts) kaydeder.
- Offline veya bağlantı hatası için `jb_sync_fallback` isimli kuyruk tutulur; başarısız ops tekrar denenir.

## Senkronizasyon Modeli
- Operasyon (op) tabanlı: her yazma bir `opId` ile gönderilir; Worker idempotent şekilde işler (aynı `opId` tekrar gelirse atlar).
- Versiyonlama: Veri anahtarları için `v`/`version` kullanılarak çakışmalar azaltılır.
- Silent load / auto-sync: Belirli aralıklarla `loadFromCloudSilent()` çalışır; hataları yutar ve fallback kuyruğunu kontrol eder.

## Siparişler ve Raf Durumu
- `getShelfStatus(order)` ve `isReady(order)` helper'ları order içindeki `products[].arrived` alanlarını inceler; tüm ürünler gelmişse sipariş `ready` kabul edilir.
- `advanceStatus()` gibi fonksiyonlar artık bulut kaydını bekler (`await saveOrders()`) — böylece optimistik revert'ler engellenir.

## Görseller (Image) Yönetimi
- `isUsableOrderImage(url)` ile yalnızca güvenli/uygun kaynaklar (`https:, data:, blob:, /api/image/:id`) render edilir.
- `getRenderableImageUrl(url)` dönen URL'yi normalize eder; kırık resimler `_brokenImageCache` içinde tutulur ve tekrar istenmez.
- `handleBrokenImage(el)` şimdi farklı varyantları (relative, absolute, origin-prefixed) cache'e ekleyerek tekrar eden 404 isteklerini engeller.

## Değişiklikler (LocalStorage azaltma)
- Taşınan anahtarlar: `jb_users`, `jb_suppliers`, `jb_catalog`, `jb_settings`, `jb_templates`, `jb_notif`, `jb_activity`, `jb_cat_categories`, `jb_feed` (ve benzeri). Bunlar artık `writeLocalJson` üzerinden D1/Worker tarafından servis ediliyor.
- `jb_orders` büyük snapshot'ı artık localStorage'a yazılmıyor; sadece hafif meta saklanıyor.

## Hata/Kritik Durumlar ve Düzeltmeler
- `ReferenceError: getShelfStatus is not defined` — helper geri eklendi.
- `persistOrdersLocalSafe is not defined` — güvenli meta kaydı fonksiyonu eklendi.
- Kırık görsellerin tekrar sorgulanması — `_brokenImageCache` genişletildi, varyantler cache'e alınıyor.

## Ölçek ve Üretime Alma Önerileri
1. Smoke testler (zorunlu):
   - Login, order listeleme, badge güncellenmesi, ürün "arrived" işareti, kullanıcı ekleme.
   - Görsel 404 davranışı: eksik görseller sistemde görünür ama tekrar istenmez.
2. CORS: `wrangler dev` ile geliştirme yaparken preflight hataları varsa Worker'a `Access-Control-Allow-Origin: *` (veya frontend origin) header ekleyin ve preflight izinlerini tanımlayın.
3. D1 limitleri ve önlemler:
   - D1 okuma/yazma sınırlarına dikkat edin (hesap/plan bazlı).
   - Okunma-ağırlıklı anahtarlar için Cache API veya Workers KV kullanın.
   - Büyük listeler için pagination ve batch okumalar kullanın.
4. İzleme: yayına aldıktan sonra 24–72 saat aktif log ve hata izleme (Worker logs + D1 metrics) yapın.

## Deploy Checklist
- [ ] Tüm smoke testleri temiz (lokalde + staging).
- [ ] Worker CORS ve preflight kontrolü yapılmış.
- [ ] D1 quota uygun (veya read-heavy anahtarlar cache'e taşınmış).
- [ ] `wrangler publish` ile staging; 24 saat sonra prod.

## Geri Alım / Rollback
- Worker sürüm yönetimi ile önceki worker sürümüne dönün; gerektiğinde frontend tarafında `writeLocalJson` fallbacks korunuyor, bu da rollback'i kolaylaştırır.

---
Dosya: [SYSTEM.md](SYSTEM.md)

Eğer isterseniz ben şimdi bu dosyayı daha ayrıntılı hale getiririm (ER diyagramı, endpoint örnekleri, D1 tablo şeması, Playwright test senaryoları). Hangi derinlikte istersiniz?