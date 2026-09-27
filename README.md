# SCOPE — System Control, Operations & Performance Engine 🚀

Sistem monitoring cerdas & real-time untuk memeriksa status job scheduler (Chronos/Cronacle), pemantauan beban sistem (CPU & RAM), active jobs live streaming, serta multi-target broadcast alert ke WhatsApp.

---

## 📁 Struktur Project

```text
Monitoring Eska/
├── auth_info_baileys/    # Sesi login WhatsApp (dibuat otomatis setelah scan QR)
├── config.js             # Konfigurasi target server, interval cron, & nomor WA
├── logger.json           # Cache riwayat error untuk mencegah spam notifikasi
├── package.json          # Dependency & script npm
├── app.js                # Core engine monitoring & koneksi WhatsApp
├── .gitignore            # Ignore credentials & node_modules
└── README.md             # Petunjuk instalasi & penggunaan
```

---

## ⚙️ Persiapan & Konfigurasi

Buka file [`config.js`](file:///c:/xampp/htdocs/Monitoring%20Eska/config.js) dan sesuaikan:

1. **`TARGET_SERVERS`**: Daftarkan nama server, baseUrl, dan endpoint API Chronos.
2. **`WA_TARGET_JID`**: Nomor WhatsApp penerima alert.
   - Format Personal: `628xxxxxxxxxx@s.whatsapp.net`
   - Format Group: `xxxxxxxxxx@g.us`
3. **`CRON_SCHEDULE`**: Interval pengecekan (default: `* * * * *` = tiap 1 menit).
4. **`IGNORE_SSL_ERRORS`**: `true` jika server menggunakan sertifikat self-signed.

---

## 🚀 Cara Instalasi & Menjalankan

### 1. Install Dependencies
Buka terminal / CMD di folder project ini, lalu jalankan:

```bash
npm install
```

### 2. Jalankan Engine
```bash
node app.js
```
atau
```bash
npm start
```

### 3. Scan QR Code
1. Pada terminal akan muncul tampilan **QR Code**.
2. Buka aplikasi **WhatsApp** di smartphone Anda.
3. Masuk ke **Perangkat Tertaut (Linked Devices)** > **Tautkan Perangkat (Link a Device)**.
4. Scan QR code yang tampil di terminal.

---

## 🔍 Cara Kerja Sistem

1. **Auto-Trigger**: Saat WhatsApp terhubung, bot langsung melakukan cek pertama, lalu menjadwalkan pengecekan sesuai cron (`* * * * *`).
2. **API Fetching**: Bot melakukan request HTTP GET ke endpoint scheduler masing-masing server.
3. **Error Filtering**: Memfilter job dengan status `Error`, `FAILED`, atau `Fail`.
4. **Anti-Spam Deduping**: Sistem mencocokkan `Server + JobName + LastRunTime` ke [`logger.json`](file:///c:/xampp/htdocs/Monitoring%20Eska/logger.json). Jika sudah pernah dikirim, notifikasi tidak akan diulang (mencegah spam).
5. **Instant Alert**: Mengirim pesan format darurat ke WhatsApp target.
6. **Health Check API**: Dapat diakses melalui browser di `http://localhost:3000/` atau `http://localhost:3000/check-now` untuk memicu pengecekan instan.
