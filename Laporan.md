# Laporan UTS — Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication

**Mata Kuliah:** Sistem Paralel dan Terdistribusi  
**Tema:** Pub-Sub Log Aggregator · Idempotent Consumer · Deduplication  
**Bahasa Implementasi:** Python 3.11 · FastAPI · SQLite · Docker  

---

## 1. Ringkasan Sistem dan Arsitektur

### 1.1 Deskripsi Singkat

Sistem ini adalah sebuah *log aggregator* berbasis pola *publish-subscribe* (Pub-Sub). Publisher mengirimkan event/log melalui HTTP ke endpoint `POST /publish`. Event diantrekan ke dalam `asyncio.Queue` (in-memory buffer) lalu diproses secara asinkron oleh sebuah *consumer worker*. Setiap event divalidasi dan dicatat ke SQLite hanya apabila belum pernah diproses sebelumnya — inilah inti dari mekanisme *idempotency* dan *deduplication*.

### 1.2 Diagram Arsitektur

```
╔══════════════════════════════════════════════════════════════════════╗
║                        Docker Container                              ║
║                                                                      ║
║  ┌─────────────┐   POST /publish   ┌───────────────────────────────┐ ║
║  │  Publisher  │ ────────────────▶ │         FastAPI App           │ ║
║  │  (Service)  │                   │                               │ ║
║  └─────────────┘                   │  ┌─────────────────────────┐  │ ║
║                                    │  │    asyncio.Queue         │  │ ║
║  ┌─────────────┐   GET /events      │  │  (in-memory buffer)      │  │ ║
║  │   Client    │ ──────────────▶   │  └────────────┬────────────┘  │ ║
║  │  (Browser/  │   GET /stats       │               │               │ ║
║  │   curl)     │ ──────────────▶   │  ┌────────────▼────────────┐  │ ║
║  └─────────────┘                   │  │   Consumer Worker        │  │ ║
║                                    │  │  (asyncio Task)          │  │ ║
║                                    │  └────────────┬────────────┘  │ ║
║                                    │               │               │ ║
║                                    │  ┌────────────▼────────────┐  │ ║
║                                    │  │     DedupStore           │  │ ║
║                                    │  │  (SQLite + WAL mode)     │  │ ║
║                                    │  │  PRIMARY KEY             │  │ ║
║                                    │  │  (topic, event_id)       │  │ ║
║                                    │  └─────────────────────────┘  │ ║
║                                    └───────────────────────────────┘ ║
╚══════════════════════════════════════════════════════════════════════╝
```

**Untuk Docker Compose (bonus):**

```
┌────────────────────────┐      Internal Docker Network      ┌──────────────────────────┐
│   publisher container  │ ─── POST http://aggregator:8080 ─▶│  aggregator container    │
│   (Dockerfile.pub)     │      /publish (batch)             │  (Dockerfile)            │
└────────────────────────┘                                   └──────────────────────────┘
                                                                          │
                                                               ┌──────────▼──────────────┐
                                                               │  Docker Volume           │
                                                               │  aggregator_data/        │
                                                               │  dedup_store.db (SQLite) │
                                                               └──────────────────────────┘
```

---

## 2. Bagian Teori (T1–T8)

### T1 — Bab 1: Karakteristik Utama Sistem Terdistribusi dan Trade-off Pub-Sub

Tanenbaum & Van Steen (2007) mendefinisikan sistem terdistribusi sebagai sekumpulan komputer independen yang tampak sebagai satu sistem koheren bagi penggunanya. Terdapat beberapa karakteristik utama: (1) *resource sharing* — komponen dapat berbagi sumber daya secara transparan; (2) *openness* — sistem menggunakan antarmuka dan protokol standar yang terbuka; (3) *scalability* — sistem dapat tumbuh secara horizontal dengan menambah node; (4) *transparency* dalam delapan dimensi (*access*, *location*, *replication*, *failure*, *migration*, *concurrency*, *performance*, *scaling*); dan (5) *concurrency* — beberapa proses berjalan secara paralel.

Pada desain Pub-Sub log aggregator, terdapat beberapa *trade-off* mendasar. Pertama, *scalability* vs *consistency*: semakin banyak publisher yang mengirim event secara bersamaan, semakin sulit menjamin *total ordering* event secara global. Kedua, *availability* vs *consistency* (implikasi teorema CAP): sistem ini memprioritaskan *availability* — ketika consumer crash, publisher tetap dapat mengirim event ke queue. Ketiga, *performance* vs *durability*: menyimpan event ke SQLite (disk) menambah latensi dibanding in-memory store, namun memberikan jaminan persistensi lintas restart. Keempat, *simplicity* vs *fault-tolerance*: penanganan duplikasi dan *at-least-once delivery* menambah kompleksitas implementasi, namun merupakan keharusan dalam lingkungan terdistribusi yang rentan terhadap kegagalan parsial.

*(Tanenbaum & Van Steen, 2007, Bab 1)*

---

### T2 — Bab 2: Client-Server vs Publish-Subscribe untuk Aggregator

Arsitektur *client-server* bersifat *tightly coupled*: client memanggil server secara langsung melalui RPC atau REST API sinkron. Keunggulannya adalah kesederhanaan, kemudahan debugging, dan *request-response* yang langsung. Namun, kelemahannya signifikan untuk use case aggregasi: terdapat *single point of failure* pada server, potensi *bottleneck* saat banyak publisher mengirim secara bersamaan, dan setiap publisher harus mengetahui alamat tepat consumer-nya (*tight coupling*).

Arsitektur *publish-subscribe* menggunakan *event broker* atau *message queue* sebagai perantara. Publisher mengirim event ke suatu *topic*; subscriber menerima event dari topic yang relevan tanpa mengetahui siapa publisher-nya (Tanenbaum & Van Steen, 2007, Bab 2). Hal ini menghasilkan tiga jenis *decoupling*: (1) *spatial decoupling* — publisher dan subscriber tidak perlu saling mengetahui; (2) *temporal decoupling* — tidak perlu online bersamaan; (3) *synchronization decoupling* — tidak perlu eksekusi sinkron.

Pub-Sub lebih tepat dipilih untuk log aggregator karena: (a) multiple consumer dapat mengonsumsi event dari topic yang sama secara independen; (b) *buffering* alamiah melalui `asyncio.Queue` mencegah kehilangan event ketika consumer sedang lambat; (c) penambahan subscriber baru (misalnya untuk alerting atau analytics) tidak memerlukan perubahan pada publisher; (d) *fault isolation* — crash pada consumer tidak mempengaruhi publisher. Sesuai Tanenbaum & Van Steen (2007), Pub-Sub adalah pola arsitektur *event-driven* yang paling sesuai untuk sistem terdistribusi dengan kebutuhan *scalability* dan *loose coupling* tinggi.

*(Tanenbaum & Van Steen, 2007, Bab 2)*

---

### T3 — Bab 3: At-Least-Once vs Exactly-Once dan Pentingnya Idempotent Consumer

Tanenbaum & Van Steen (2007) membahas *delivery semantics* dalam komunikasi terdistribusi dalam tiga level: (1) *at-most-once* — pesan mungkin hilang, tidak pernah duplikat; (2) *at-least-once* — pesan dijamin terkirim setidaknya satu kali, namun mungkin duplikat akibat *retry*; (3) *exactly-once* — pesan terkirim tepat satu kali, paling sulit diimplementasikan.

*At-least-once delivery* lebih mudah diimplementasikan karena publisher cukup melakukan *retry* saat tidak mendapat ACK dalam batas waktu, menggunakan `event_id` yang sama. Namun, akibatnya adalah kemungkinan event diterima lebih dari satu kali di sisi consumer. *Exactly-once delivery* memerlukan protokol koordinasi yang kompleks seperti *two-phase commit* atau *idempotency token* yang diakui oleh seluruh lapisan sistem, yang sangat sulit dalam lingkungan terdistribusi.

*Idempotent consumer* menjadi krusial dalam kondisi *at-least-once delivery* karena consumer harus mampu menerima event yang sama berkali-kali namun hanya memproses efeknya sekali. Tanpa idempotency, duplikasi dari *retry* akan menyebabkan inkonsistensi data — misalnya, log yang sama dicatat dua kali atau counter statistik bertambah secara salah.

Pada sistem ini, idempotency diimplementasikan melalui `DedupStore.mark_processed()` yang menggunakan `INSERT` ke SQLite dengan *primary key* `(topic, event_id)`. Jika `IntegrityError` terjadi, event adalah duplikat dan dibuang. Pendekatan ini mengkombinasikan *at-least-once delivery* dengan *idempotent consumer* sehingga secara efektif mencapai semantik *effectively exactly-once processing*.

*(Tanenbaum & Van Steen, 2007, Bab 3)*

---

### T4 — Bab 4: Skema Penamaan Topic dan Event ID

Tanenbaum & Van Steen (2007) menekankan bahwa *naming system* harus memisahkan *identifier* (nama logis) dari *location* (alamat fisik), serta mendukung *resolution* yang efisien. Pada sistem ini dirancang dua skema penamaan:

**Topic Naming** — format hierarkis: `{service}.{category}.{action}`  
Contoh: `auth.user.login`, `payment.order.created`, `infra.server.health`  
Keuntungan: (a) mencerminkan struktur organisasi logis; (b) mendukung *prefix-based filtering* (misalnya semua event `auth.*`); (c) mudah diindeks di SQLite; (d) intuitif untuk debugging dan observabilitas.

**Event ID Naming** — format komposit: `{source}-{timestamp_ms}-{uuid4}`  
Contoh: `webapi-1715001234567-550e8400-e29b-41d4-a716-446655440000`  
Komponen: (a) `source` untuk traceability dan debugging; (b) `timestamp_ms` untuk natural temporal ordering dan korelasi; (c) `uuid4` untuk jaminan keunikan global (*collision probability* ≈ 2⁻¹²²).

**Dampak terhadap deduplication**: (1) *Composite key* `(topic, event_id)` memungkinkan event dengan `event_id` yang sama pada topic berbeda diperlakukan sebagai event independen — penting dalam sistem *multi-service* di mana UUID tidak selalu di-scope secara global; (2) UUID v4 memastikan tidak ada collision bahkan di lingkungan *high-throughput*; (3) SQLite `PRIMARY KEY (topic, event_id)` memanfaatkan *B-tree index* untuk lookup O(log n), membuat operasi dedup tetap efisien bahkan untuk jutaan event.

*(Tanenbaum & Van Steen, 2007, Bab 4)*

---

### T5 — Bab 5: Ordering Event — Kapan Total Ordering Tidak Diperlukan

Tanenbaum & Van Steen (2007) membedakan antara *total ordering* (semua event dapat dibandingkan secara global konsisten di semua node) dan *partial ordering* (hanya event yang secara kausal terhubung yang perlu diurutkan). *Total ordering* membutuhkan koordinasi global seperti *Lamport clock* atau *vector clock*, yang menambah overhead komunikasi signifikan.

Dalam konteks log aggregator, **total ordering tidak diperlukan** karena: (1) log dari service-service yang berbeda bersifat independen secara kausal — urutan antara event `auth.user.login` dan `payment.order.created` tidak memiliki makna operasional; (2) consumer pada umumnya memproses log untuk tujuan *auditing*, *monitoring*, dan *observability*, bukan untuk membangun state terurut yang kritis; (3) overhead koordinasi global tidak sebanding dengan manfaatnya untuk use case ini.

**Pendekatan praktis yang diusulkan**: kombinasi *event timestamp* (ISO 8601 dari sisi publisher, mewakili waktu kejadian) dengan `processed_at` (dari sisi aggregator, mewakili waktu ingestion). Ini memberikan *partial ordering* yang cukup: event dalam satu topic dapat diurutkan berdasarkan `processed_at` untuk analisis tren, sedangkan `event timestamp` digunakan untuk analisis kausal.

**Batasan pendekatan ini**: (1) *clock skew* antar publisher dapat menyebabkan event dengan timestamp lebih lama tiba lebih belakangan (*out-of-order delivery*); (2) tanpa *vector clock* (Tanenbaum & Van Steen, 2007), *causal ordering* lintas service tidak dapat dijamin; (3) NTP drift dapat mencapai beberapa detik, membuat ordering berdasarkan timestamp saja tidak selalu akurat. Untuk aggregator, *best-effort ordering* melalui timestamp sudah memadai — consumer dianggap bersifat *stateless* terhadap urutan event.

*(Tanenbaum & Van Steen, 2007, Bab 5)*

---

### T6 — Bab 6: Failure Modes dan Strategi Mitigasi

Tanenbaum & Van Steen (2007) mengidentifikasi berbagai jenis kegagalan dalam sistem terdistribusi: *crash failure*, *omission failure*, *timing failure*, dan *Byzantine failure*. Pada sistem ini, failure modes yang relevan beserta strategi mitigasinya adalah:

**1. Duplikasi event akibat retry publisher**  
*Penyebab*: Publisher mengirim ulang event yang sama karena tidak mendapat ACK tepat waktu (*at-least-once delivery*).  
*Mitigasi*: Dedup store SQLite dengan `PRIMARY KEY (topic, event_id)` — INSERT atom memastikan hanya satu instance yang tersimpan; setiap duplikat di-log sebagai `[DUPLICATE]`.

**2. Out-of-order delivery**  
*Penyebab*: Variasi latensi jaringan menyebabkan event tiba tidak sesuai urutan kronologis.  
*Mitigasi*: Tidak kritis untuk aggregator; dibiarkan dengan *best-effort ordering* via `processed_at` timestamp di SQLite.

**3. Aggregator crash (loss of in-memory queue)**  
*Penyebab*: Container restart menyebabkan `asyncio.Queue` (in-memory) hilang.  
*Mitigasi*: SQLite dedup store persisten di Docker volume. Event yang sudah diproses tidak akan diproses ulang setelah restart. Event yang masih di queue saat crash hilang — *acceptable trade-off* untuk log use case (publisher dapat melakukan retry).

**4. Queue overflow (back-pressure)**  
*Penyebab*: Publisher lebih cepat dari consumer, queue mencapai `maxsize`.  
*Mitigasi*: `asyncio.Queue` dengan `maxsize=50_000`; publisher mendapat `HTTP 503` saat queue penuh — sinyal untuk melakukan *exponential backoff*.

**5. SQLite write corruption**  
*Penyebab*: Crash di tengah-tengah write operasi.  
*Mitigasi*: SQLite WAL (*Write-Ahead Logging*) mode aktif — menjamin atomicity dan kemampuan recovery.

**6. Publisher retry storm**  
*Penyebab*: Publisher melakukan retry agresif tanpa backoff, membanjiri sistem.  
*Mitigasi*: Publisher diimplementasikan dengan *exponential backoff* (delay 0.5s, 1s, ...) dan dedup store mencegah pemrosesan ganda.

*(Tanenbaum & Van Steen, 2007, Bab 6)*

---

### T7 — Bab 7: Eventual Consistency melalui Idempotency dan Deduplication

Tanenbaum & Van Steen (2007) mendefinisikan *eventual consistency* sebagai model konsistensi lemah (*weak consistency*) di mana: jika tidak ada update baru, maka pada akhirnya semua replika/node akan konvergen ke nilai yang sama. Ini berbeda dari *strong consistency* yang memerlukan semua pembaca melihat update terbaru secara instan.

Dalam konteks log aggregator, *eventual consistency* bermakna: semua event yang valid pada akhirnya akan tersimpan tepat satu kali di `DedupStore`, meskipun selama proses ingest terdapat duplikasi, *retry*, atau *out-of-order delivery*. Sistem tidak menjamin bahwa `GET /events` langsung memperlihatkan semua event yang baru dipublikasi (karena ada delay di `asyncio.Queue`), namun menjamin bahwa **pada akhirnya** semua event unik akan muncul.

**Peran Idempotency**: Memastikan bahwa memproses event yang sama berkali-kali menghasilkan state akhir yang identik dengan memprosesnya sekali. `DedupStore.mark_processed()` adalah operasi *idempotent* — memanggil fungsi ini dengan argumen yang sama dua kali atau seribu kali menghasilkan tepat satu baris di SQLite.

**Peran Deduplication**: Melengkapi idempotency dengan cara: (1) mencegah *double-counting* pada statistik sistem; (2) mencegah duplikasi entry pada output `GET /events`; (3) menyediakan *convergence guarantee* — state `DedupStore` akan konvergen ke himpunan event unik yang benar, terlepas dari urutan atau frekuensi penerimaan.

Kombinasi keduanya memberikan jaminan *effectively exactly-once processing*: meski delivery bersifat *at-least-once*, efek pemrosesan pada state (SQLite) hanya terjadi sekali. Ini adalah implementasi konkret dari *data-centric consistency model* (Tanenbaum & Van Steen, 2007) di mana konsistensi didefinisikan pada level data, bukan pada level protokol.

*(Tanenbaum & Van Steen, 2007, Bab 7)*

---

### T8 — Bab 1–7: Metrik Evaluasi Sistem

Evaluasi sistem Pub-Sub log aggregator yang komprehensif memerlukan metrik yang mencakup seluruh aspek arsitektur (Bab 1–7):

**1. Throughput (events/second)**  
Kapasitas pemrosesan end-to-end dari `POST /publish` hingga tersimpan di SQLite. Target: ≥ 5.000 events per run tanpa degradasi responsivitas. *Relevansi*: skalabilitas (Bab 1) dan arsitektur Pub-Sub (Bab 2).

**2. Publish Latency (ms)**  
Waktu antara event diterima di `/publish` dan event selesai diproses consumer. Dibagi menjadi *queue wait time* dan *DB write time*. *Relevansi*: komunikasi (Bab 3) dan ordering (Bab 5).

**3. Duplicate Detection Rate (%)**  
`duplicate_dropped / received × 100`. Harus = 100% untuk semua event yang memang duplikat. *False positive rate* (event unik yang salah dibuang) harus = 0%. *Relevansi*: idempotency dan konsistensi (Bab 7).

**4. Queue Depth (real-time)**  
Panjang `asyncio.Queue` saat ini. Indikator *backpressure* dan risiko overflow. *Relevansi*: fault tolerance (Bab 6).

**5. Restart Recovery Time (ms)**  
Waktu dari container start hingga `/health` mengembalikan 200. Harus < 5 detik untuk SQLite. *Relevansi*: konsistensi lintas restart (Bab 7) dan toleransi kegagalan (Bab 6).

**6. Dedup Store Size Growth (MB/juta event)**  
Pertumbuhan file SQLite seiring bertambahnya event unik. Penting untuk *capacity planning* dan menentukan apakah diperlukan partisi atau purging. *Relevansi*: penamaan dan penyimpanan (Bab 4).

**Kaitan ke Keputusan Desain**: SQLite dipilih atas in-memory store karena mendukung *Restart Recovery Time* < 1 detik. `asyncio.Queue` dipilih atas multi-threading karena throughput lebih tinggi dengan overhead minimal (cooperative multitasking). *Composite key* `(topic, event_id)` dipilih untuk mendukung *zero false positive* pada dedup.

*(Tanenbaum & Van Steen, 2007, Bab 1–7)*

---

## 3. Keputusan Desain

### 3.1 Idempotency

Idempotency diimplementasikan di lapisan `DedupStore` menggunakan SQLite `PRIMARY KEY (topic, event_id)`. Operasi `mark_processed()` melakukan atomic `INSERT` — jika `IntegrityError` terjadi (key sudah ada), fungsi mengembalikan `False` dan consumer membuang event. Pendekatan ini memastikan tidak ada *false positive* (event unik yang salah dibuang) karena SQLite menjamin integritas constraint.

### 3.2 Dedup Store

SQLite dipilih karena: (a) embedded, tidak memerlukan server terpisah; (b) WAL mode mendukung concurrent reads; (c) survives container restart melalui Docker volume; (d) ACID compliant untuk atomicity; (e) sesuai spesifikasi "local-only". Alternatif yang dipertimbangkan: file JSON (tidak atomic saat crash), LMDB (lebih kompleks, overkill untuk use case ini).

### 3.3 Ordering

*Total ordering* tidak diimplementasikan. Event diproses dalam urutan FIFO dari `asyncio.Queue` (*arrival order*). `processed_at` timestamp di SQLite memungkinkan *ingestion-order* queries. Untuk use case log aggregation, ini sudah memadai.

### 3.4 Retry dan Back-pressure

Publisher mendapat `HTTP 503` jika queue penuh (`maxsize=50_000`). Publisher (`publisher.py`) mengimplementasikan *retry with exponential backoff* (delay 0.5s per attempt, max 2 attempts per batch). Dedup store memastikan retry tidak menyebabkan double processing.

---

## 4. Analisis Performa dan Metrik

### 4.1 Hasil Stress Test (Unit Test 10)

| Metrik | Hasil |
|--------|-------|
| Total events dikirim | 1.000 (750 unik + 250 duplikat) |
| Waktu total (enqueue + process + persist) | < 8 detik |
| Duplicate detection rate | 100% (250/250) |
| False positive rate | 0% |
| Unique events di `/events` | 750 |

### 4.2 Target Produksi (≥ 5.000 events via Docker Compose)

| Metrik | Target |
|--------|--------|
| Total events | 5.000 unik + 1.250 duplikat = 6.250 total |
| Throughput | ≥ 500 events/second |
| Duplicate rate | 25% (≥ minimal 20%) |
| Restart recovery | < 2 detik |

---

## 5. Referensi (APA Edisi ke-7)

Tanenbaum, A. S., & Van Steen, M. (2007). *Distributed systems: Principles and paradigms* (Edisi ke-2). Pearson Prentice Hall.
