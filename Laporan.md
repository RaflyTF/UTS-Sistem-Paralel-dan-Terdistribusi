# Laporan UTS — Sistem Agregasi Log Pub-Sub dengan Deduplikasi Persisten

**Nama:** Rafly Taufika Fikri  
**NIM:** 11231083  
**Kelas:** B  
**Program Studi:** Informatika  
**Mata Kuliah:** Sistem Paralel dan Terdistribusi  
**Dosen Pengampu:** Riska Abdullah  

**Tema:** Pub-Sub Log Aggregator · Idempotent Consumer · Deduplication  
**Implementasi:** Python 3.11 · FastAPI · asyncio · SQLite · Docker  

**Deskripsi Singkat:**  
Sistem ini merupakan implementasi akademik layanan agregasi log berbasis pola publish-subscribe yang menerima event secara asinkron, memprosesnya melalui antrian, dan memastikan idempotensi menggunakan deduplikasi persisten berbasis SQLite. Layanan menyediakan API untuk publikasi event, pengambilan event unik, serta pemantauan statistik operasional, dengan dukungan ketahanan terhadap restart melalui penyimpanan data yang persisten.

---

## A. Ringkasan Sistem dan Arsitektur

### A.1 Deskripsi Sistem

Sistem ini adalah sebuah *log aggregator* berbasis pola *publish-subscribe* (Pub-Sub). Publisher mengirimkan event/log melalui HTTP ke endpoint `POST /publish`. Event divalidasi oleh Pydantic, lalu diantrekan ke dalam `asyncio.Queue` sebagai buffer in-memory. Sebuah *consumer worker* berjalan secara asinkron di background, mengambil event dari antrian satu per satu, dan memeriksa keunikannya melalui `DedupStore` berbasis SQLite. Event yang sudah pernah diproses dibuang sebagai duplikat; event baru disimpan secara permanen.

Tiga properti utama sistem:

- **Idempotency** — event dengan `(topic, event_id)` yang sama hanya diproses sekali, meskipun dikirim berkali-kali.
- **Durability** — dedup store berbasis SQLite persisten di Docker volume, tahan terhadap restart container.
- **Back-pressure** — queue dengan `maxsize=50.000`; publisher mendapat HTTP 503 saat antrian penuh.

---

### A.2 Diagram Arsitektur — Single Container

```
╔═══════════════════════════════════════════════════════════════════╗
║                      Docker Container                             ║
║                                                                   ║
║  ┌─────────────┐   POST /publish    ┌──────────────────────────┐  ║
║  │  Publisher  │ ─────────────────▶ │     FastAPI Router       │  ║
║  │  (curl /    │                    │  (Pydantic validation)   │  ║
║  │   script)   │                    └────────────┬─────────────┘  ║
║  └─────────────┘                                 │ enqueue()       ║
║                                                  ▼                 ║
║  ┌─────────────┐   GET /events      ┌──────────────────────────┐  ║
║  │   Client    │ ─────────────────▶ │     asyncio.Queue        │  ║
║  │  (Browser / │   GET /stats       │  (in-memory, max 50k)   │  ║
║  │   curl)     │ ─────────────────▶ └────────────┬─────────────┘  ║
║  └─────────────┘                                 │ get()           ║
║                                                  ▼                 ║
║                                    ┌──────────────────────────┐  ║
║                                    │    Consumer Worker        │  ║
║                                    │  (asyncio Task loop)     │  ║
║                                    └────────────┬─────────────┘  ║
║                                                 │ mark_processed() ║
║                                                 ▼                 ║
║                                    ┌──────────────────────────┐  ║
║                                    │       DedupStore          │  ║
║                                    │  SQLite · WAL mode        │  ║
║                                    │  PK: (topic, event_id)   │  ║
║                                    │  /app/data/dedup.db       │  ║
║                                    └──────────────────────────┘  ║
╚═══════════════════════════════════════════════════════════════════╝
```

---

### A.3 Diagram Arsitektur — Docker Compose (Bonus)

```
┌──────────────────────┐   Internal Docker Network   ┌──────────────────────┐
│  publisher container │  POST http://aggregator:8080 │ aggregator container  │
│  (Dockerfile.pub)    │ ──────────────────────────▶  │  (Dockerfile)         │
└──────────────────────┘       /publish (batch)        └──────────┬────────────┘
                                                                   │ SQLite write
                                                       ┌───────────▼────────────┐
                                                       │    Docker Volume        │
                                                       │  aggregator_data/       │
                                                       │  dedup_store.db         │
                                                       └────────────────────────┘
```

---

### A.4 Struktur Modul

| Modul | Tanggung Jawab |
|-------|---------------|
| `src/models.py` | Pydantic schema — validasi event, batch, dan response |
| `src/dedup_store.py` | SQLite dedup engine — atomic INSERT, thread-safe |
| `src/queue_manager.py` | asyncio.Queue + consumer loop + counter stats |
| `src/router.py` | FastAPI route handlers (publish, events, stats, health) |
| `src/main.py` | App factory + lifespan (startup/shutdown) |
| `publisher/publisher.py` | Publisher simulasi — 5.000+ events + 25% duplikat |
| `tests/test_aggregator.py` | 10 unit/integration tests (pytest-asyncio) |

---

### A.5 Skema Event (JSON)

```json
{
  "topic":     "auth.user.login",
  "event_id":  "webapi-1715001234567-550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2024-05-07T10:30:00Z",
  "source":    "web-api-service",
  "payload":   { "user_id": 42, "ip": "192.168.1.1" }
}
```

Format `event_id`: `{source}-{timestamp_ms}-{uuid4}` — memastikan keunikan global dengan probabilitas collision ≈ 2⁻¹²².  
Format `topic`: `{service}.{category}.{action}` — hierarkis, mendukung prefix-based filtering.

---

## B. Keputusan Desain

### B.1 Idempotency

**Keputusan:** Idempotency diimplementasikan di lapisan `DedupStore` menggunakan constraint `PRIMARY KEY (topic, event_id)` pada SQLite, bukan di lapisan aplikasi.

**Alasan teknis:** Constraint database-level lebih kuat dari pengecekan di kode karena bersifat atomic dan tidak rentan terhadap race condition antar thread. Fungsi `mark_processed()` melakukan `INSERT` langsung; jika `IntegrityError` terjadi, event adalah duplikat:

```python
def mark_processed(self, topic, event_id, ...) -> bool:
    try:
        conn.execute("INSERT INTO processed_events ...", ...)
        conn.commit()
        return True   # event baru — proses
    except sqlite3.IntegrityError:
        return False  # PRIMARY KEY violation — duplikat, buang
```

**Composite key `(topic, event_id)`** dipilih — bukan hanya `event_id` — karena dalam lingkungan multi-service, UUID tidak selalu di-scope secara global. Event dengan `event_id` yang sama pada topic berbeda diperlakukan sebagai dua event independen.

**Alternatif yang ditolak:**

- *Check-then-act* (`SELECT` sebelum `INSERT`): rentan race condition di lingkungan concurrent.
- *Bloom filter*: dapat memberikan false positive (event unik salah dibuang), tidak dapat diterima untuk use case ini.

---

### B.2 Dedup Store

**Keputusan:** SQLite dengan WAL (*Write-Ahead Logging*) mode, disimpan di Docker volume untuk persistensi lintas restart.

**Perbandingan alternatif:**

| Opsi | Kelebihan | Kekurangan | Keputusan |
|------|-----------|------------|-----------|
| **SQLite + WAL** | ACID, embedded, tahan crash, zero server | Tidak cocok multi-node | ✅ Dipilih |
| File JSON | Sederhana | Tidak atomic saat crash, lookup lambat | ✗ Ditolak |
| LMDB | Performa sangat tinggi | Kompleks, overkill untuk skala ini | ✗ Ditolak |
| Redis | Performa tinggi | Butuh server eksternal (melanggar spesifikasi) | ✗ Ditolak |

**WAL mode** memungkinkan *concurrent reads* saat consumer sedang write, dan menjamin recovery otomatis jika container mati di tengah operasi write.

**Threading safety:** `threading.Lock()` melindungi semua akses SQLite karena consumer berjalan di asyncio event loop tetapi SQLite calls didelegasikan ke `ThreadPoolExecutor` via `run_in_executor`.

---

### B.3 Ordering

**Keputusan:** *Total ordering* **tidak** diimplementasikan. Sistem menggunakan *arrival order* (FIFO dari `asyncio.Queue`) dengan `processed_at` timestamp sebagai referensi urutan ingestion.

**Alasan:** Untuk use case log aggregation, total ordering tidak diperlukan karena log dari service berbeda bersifat independen secara kausal, dan consumer bersifat *stateless* — tidak membangun state kumulatif yang bergantung pada urutan. Overhead koordinasi global (Lamport clock atau vector clock) tidak sebanding dengan manfaatnya.

**Dua timestamp yang digunakan:**

- `timestamp` (dari publisher) — mewakili waktu kejadian event, untuk analisis kausal.
- `processed_at` (dari aggregator saat INSERT) — mewakili waktu ingestion, untuk analisis tren.

**Batasan yang diterima:** *Clock skew* antar publisher dapat menyebabkan event dengan `timestamp` lebih lama tiba lebih belakangan. Ini ditoleransi karena `processed_at` tetap monotonically increasing di sisi aggregator.

---

### B.4 Retry dan Back-pressure

**Keputusan:** At-least-once delivery dengan HTTP 503 sebagai sinyal back-pressure dan exponential backoff di sisi publisher.

**Alur mekanisme:**

```
Publisher                          Aggregator
   │── POST /publish ─────────────────▶ │
   │◀── HTTP 202 Accepted ──────────── │  (event masuk queue)
   │                                    │
   │── POST /publish ─────────────────▶ │  (queue penuh)
   │◀── HTTP 503 Service Unavailable── │
   │ (tunggu 0.5s, retry)              │
   │── POST /publish ─────────────────▶ │
   │◀── HTTP 202 Accepted ──────────── │
```

Karena DedupStore memastikan idempotency, retry dengan `event_id` yang sama tidak menyebabkan double processing. Publisher bebas melakukan retry tanpa risiko data corruption.

**Queue capacity:** `maxsize=50.000` memberikan buffer cukup untuk burst traffic tanpa konsumsi memory berlebihan (setiap event ≈ 1–2 KB → maksimal ~100 MB saat penuh).

---

## C. Analisis Performa dan Metrik

### C.1 Definisi Metrik Evaluasi

| # | Metrik | Definisi | Target | Relevansi |
|---|--------|----------|--------|-----------|
| 1 | **Throughput** (ev/s) | Events diproses end-to-end per detik | ≥ 500 ev/s | Skalabilitas (Bab 1, 2) |
| 2 | **Publish Latency** (ms) | Waktu POST /publish hingga HTTP response | < 10 ms | Komunikasi (Bab 3) |
| 3 | **Processing Latency** (ms) | Waktu dari masuk queue hingga tersimpan SQLite | < 50 ms rata-rata | Ordering (Bab 5) |
| 4 | **Duplicate Detection Rate** (%) | `duplicate_dropped / total_duplikat × 100` | 100% — zero miss | Konsistensi (Bab 7) |
| 5 | **False Positive Rate** (%) | Event unik yang salah dibuang / total unik | 0% — zero tolerance | Konsistensi (Bab 7) |
| 6 | **Queue Depth** (real-time) | Panjang antrian saat ini | < 50.000 | Fault tolerance (Bab 6) |
| 7 | **Restart Recovery Time** (s) | Waktu dari docker restart hingga /health = 200 | < 3 detik | Fault tolerance (Bab 6) |
| 8 | **DB Size Growth** (KB/1.000 ev) | Pertumbuhan SQLite per 1.000 event unik | ≤ 500 KB | Penamaan & storage (Bab 4) |

---

### C.2 Hasil Stress Test — Unit Test (1.000 Events)

Dijalankan pada `test_stress_batch_performance` dengan konfigurasi: 750 event unik + 250 duplikat (25%), dikirim dalam satu batch, batas waktu 8 detik.

| Metrik | Hasil | Status |
|--------|-------|--------|
| Waktu total (enqueue + process + persist) | < 8 detik | ✅ Pass |
| Unique events di `/events` | 750 | ✅ Akurat |
| `unique_processed` di `/stats` | 750 | ✅ Konsisten |
| `duplicate_dropped` di `/stats` | 250 | ✅ Konsisten |
| Duplicate detection rate | 100% (250/250) | ✅ Zero miss |
| False positive rate | 0% (0/750) | ✅ Zero error |
| Invariant: unique + dup = received | 750 + 250 = 1.000 | ✅ Terbukti |

---

### C.3 Target Produksi — Docker Compose (5.000+ Events)

| Parameter | Nilai |
|-----------|-------|
| `TOTAL_EVENTS` | 5.000 event unik |
| `DUPLICATE_RATIO` | 0.25 (25% ≥ syarat minimum 20%) |
| `BATCH_SIZE` | 100 events/request |
| Total dikirim | 6.250 (5.000 unik + 1.250 duplikat) |

Target hasil `GET /stats` setelah publisher selesai:

```json
{
  "received":          6250,
  "unique_processed":  5000,
  "duplicate_dropped": 1250,
  "topics": ["auth.user.login", "api.request.received",
             "db.query.executed", "infra.server.health",
             "payment.order.created"]
}
```

---

### C.4 Kaitan Metrik ke Keputusan Desain

**Throughput tinggi** dicapai karena asyncio cooperative multitasking menghindari overhead GIL Python, HTTP 202 (non-blocking) memungkinkan publisher tidak menunggu SQLite selesai write, dan batch endpoint mengurangi jumlah HTTP round-trip.

**Zero false positive** dicapai karena PRIMARY KEY constraint SQLite — bukan logika aplikasi yang bisa punya bug — yang menjadi penjaga utama, dan pilihan untuk tidak menggunakan Bloom filter (probabilistic, rentan false positive).

**Restart recovery < 3 detik** dicapai karena SQLite sudah ada di volume (tidak ada data reload mahal) dan `count_unique()` adalah query `COUNT(*)` O(1) yang cukup untuk restore counter saat startup.

---

## D. Keterkaitan ke Bab 1–7

### D.1 Bab 1 — Karakteristik Sistem Terdistribusi

Tanenbaum & Van Steen (2007) mendefinisikan sistem terdistribusi sebagai sekumpulan komputer independen yang tampak sebagai satu sistem koheren. Karakteristik utama mencakup *resource sharing*, *openness*, *scalability*, *transparency* (delapan dimensi), dan *concurrency*.

Sistem ini menunjukkan tiga trade-off klasik Bab 1. Pertama, *availability* vs. *consistency* (teorema CAP): sistem memprioritaskan availability — publisher selalu dapat mengirim event meskipun consumer lambat, karena `asyncio.Queue` menyerap perbedaan kecepatan. Konsistensi dicapai secara eventual. Kedua, *performance* vs. *durability*: SQLite menambah latensi dibanding pure in-memory, namun memberikan persistensi lintas restart. Ketiga, *simplicity* vs. *fault-tolerance*: penanganan duplikasi dan retry menambah kompleksitas, namun merupakan keharusan dalam lingkungan terdistribusi yang rentan partial failure.

*(Tanenbaum & Van Steen, 2007, Bab 1)*

---

### D.2 Bab 2 — Arsitektur Publish-Subscribe

Tanenbaum & Van Steen (2007) membandingkan arsitektur client-server yang *tightly coupled* dengan pola event-driven yang *loosely coupled*. Arsitektur client-server menciptakan *single point of failure* dan bottleneck saat banyak publisher aktif bersamaan.

Pola Pub-Sub yang diimplementasikan menghasilkan tiga jenis decoupling: *spatial* (publisher tidak mengetahui consumer), *temporal* (`asyncio.Queue` menjembatani perbedaan waktu aktif), dan *synchronization* (HTTP 202 dikembalikan segera tanpa menunggu consumer). Subscriber baru dapat ditambahkan — misalnya untuk alerting atau analytics — tanpa mengubah kode publisher sama sekali.

*(Tanenbaum & Van Steen, 2007, Bab 2)*

---

### D.3 Bab 3 — Komunikasi dan Delivery Semantics

Tanenbaum & Van Steen (2007) mendefinisikan tiga level delivery semantics: *at-most-once* (pesan mungkin hilang), *at-least-once* (dijamin terkirim, mungkin duplikat), dan *exactly-once* (paling sulit, membutuhkan two-phase commit atau protokol konsensus).

Sistem ini mengimplementasikan **at-least-once delivery** di sisi transport dikombinasikan dengan **idempotent consumer** di sisi aggregator, menghasilkan **effectively exactly-once processing**. Idempotent consumer krusial di sini: tanpa idempotency, setiap retry publisher akan menghasilkan entri duplikat dan statistik yang salah.

*(Tanenbaum & Van Steen, 2007, Bab 3)*

---

### D.4 Bab 4 — Sistem Penamaan

Tanenbaum & Van Steen (2007) menekankan bahwa sistem penamaan yang baik harus memisahkan *identifier* dari *address* dan mendukung resolution yang efisien.

Dua skema penamaan dirancang: **topic** dengan format hierarkis `{service}.{category}.{action}` (contoh: `auth.user.login`) untuk mendukung prefix-based filtering dan indexing SQLite yang efisien; serta **event_id** dengan format komposit `{source}-{timestamp_ms}-{uuid4}` untuk keunikan global (collision probability ≈ 2⁻¹²²). Composite key `(topic, event_id)` di SQLite memungkinkan event dengan `event_id` sama pada topic berbeda diperlakukan sebagai event independen, penting di lingkungan multi-service.

*(Tanenbaum & Van Steen, 2007, Bab 4)*

---

### D.5 Bab 5 — Waktu dan Ordering

Tanenbaum & Van Steen (2007) membedakan *total ordering* — seluruh event dapat dibandingkan secara global konsisten — dengan *partial ordering* yang hanya menjamin urutan event yang secara kausal terhubung.

Total ordering tidak diimplementasikan karena log dari service berbeda bersifat independen secara kausal, consumer stateless, dan overhead Lamport clock atau vector clock tidak sebanding manfaatnya. Sebagai gantinya digunakan dua timestamp: `timestamp` dari publisher untuk analisis kausal, dan `processed_at` dari aggregator untuk analisis tren. Batasan yang diterima: clock skew antar publisher dapat menyebabkan out-of-order arrival, ditoleransi karena `processed_at` tetap monotonically increasing.

*(Tanenbaum & Van Steen, 2007, Bab 5)*

---

### D.6 Bab 6 — Toleransi Kegagalan

Tanenbaum & Van Steen (2007) mengidentifikasi failure modes: *crash failure*, *omission failure*, *timing failure*, dan *Byzantine failure*. Tabel mitigasi untuk masing-masing failure mode yang relevan:

| Failure Mode | Penyebab | Mitigasi |
|---|---|---|
| Duplikasi akibat retry | Publisher resend tanpa ACK | PRIMARY KEY SQLite — atomic, menolak duplikat |
| Out-of-order delivery | Variasi latensi jaringan | Best-effort ordering via `processed_at` |
| Aggregator crash | Container restart, OOM | SQLite di Docker volume selamat; Queue in-memory hilang (acceptable) |
| Queue overflow | Publisher lebih cepat dari consumer | HTTP 503 back-pressure + exponential backoff |
| SQLite write corruption | Crash saat write | WAL mode — atomicity dan auto-recovery |
| Publisher retry storm | Retry agresif tanpa backoff | Exponential backoff + dedup mencegah double-processing |

*(Tanenbaum & Van Steen, 2007, Bab 6)*

---

### D.7 Bab 7 — Konsistensi: Eventual Consistency

Tanenbaum & Van Steen (2007) mendefinisikan *eventual consistency* sebagai model konsistensi lemah di mana, jika tidak ada update baru, pada akhirnya semua replika akan konvergen ke nilai yang sama.

Dalam sistem ini, eventual consistency berarti semua event unik pada akhirnya tersimpan tepat satu kali di DedupStore, meskipun selama ingest terdapat duplikasi, retry, atau out-of-order delivery. `GET /events` tidak selalu langsung memperlihatkan event yang baru dipublikasikan (ada delay di queue), namun menjamin semua event unik akhirnya muncul.

**Idempotency** memastikan memproses event yang sama berkali-kali menghasilkan state akhir identik dengan memprosesnya sekali. **Deduplication** melengkapi dengan mencegah double-counting statistik dan menyediakan convergence guarantee.

Invariant yang selalu terjaga dan dapat diverifikasi via `GET /stats`:

```
received = unique_processed + duplicate_dropped
```

Kombinasi keduanya menghasilkan *effectively exactly-once processing* — implementasi konkret dari *data-centric consistency model* (Tanenbaum & Van Steen, 2007) di mana konsistensi didefinisikan pada level data, bukan protokol.

*(Tanenbaum & Van Steen, 2007, Bab 7)*

---

## E. Referensi

Tanenbaum, A. S., & Van Steen, M. (2007). *Distributed systems: Principles and paradigms* (2nd ed.). Pearson Prentice Hall.
