# UTS — Pub-Sub Log Aggregator

Layanan log aggregator berbasis pola *publish-subscribe* dengan **idempotent consumer** dan **deduplication** persisten menggunakan SQLite. Dibangun dengan Python 3.11 · FastAPI · asyncio · Docker.

---

## Prasyarat

- Docker Engine ≥ 24
- Docker Compose ≥ 2.x (untuk bonus)
- Python 3.11+ (untuk menjalankan tests secara lokal)

---

## Build & Run

### Opsi A — Docker (Standalone Aggregator)

```bash
# 1. Build image
docker build -t uts-aggregator .

# 2. Run container (port 8080, data volume untuk persistensi SQLite)
docker run -p 8080:8080 \
  -v aggregator_data:/app/data \
  --name uts-aggregator \
  uts-aggregator

# 3. Cek kesehatan service
curl http://localhost:8080/health
# → {"status":"ok"}
```

### Opsi B — Docker Compose (Aggregator + Publisher, Bonus +10%)

```bash
# 1. Build dan jalankan kedua service sekaligus
docker compose up --build

# 2. Pantau log publisher (proses pengiriman 5.000+ events)
docker compose logs -f publisher

# 3. Pantau log aggregator (dedup, processed, dll.)
docker compose logs -f aggregator

# 4. Setelah selesai, lihat stats
curl http://localhost:8080/stats

# 5. Bersihkan
docker compose down -v
```

---

## API Endpoints

### `POST /publish` — Kirim event (single, array, atau batch)

```bash
# Single event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "auth.user.login",
    "event_id": "evt-unique-001",
    "timestamp": "2024-05-07T10:30:00Z",
    "source": "web-api",
    "payload": {"user_id": 42, "ip": "192.168.1.1"}
  }'

# Batch (array langsung)
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '[
    {"topic":"auth.user.login","event_id":"evt-001","timestamp":"2024-05-07T10:30:00Z","source":"svc-a","payload":{}},
    {"topic":"payment.order.created","event_id":"evt-002","timestamp":"2024-05-07T10:31:00Z","source":"svc-b","payload":{}}
  ]'

# Batch wrapper
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events": [...]}'
```

**Response (202 Accepted):**
```json
{"accepted": 1, "message": "Accepted 1 event(s) for async processing."}
```

---

### `GET /events?topic=<topic>` — Ambil event unik per topic

```bash
curl "http://localhost:8080/events?topic=auth.user.login"
```

**Response:**
```json
{
  "topic": "auth.user.login",
  "count": 3,
  "events": [
    {
      "topic": "auth.user.login",
      "event_id": "evt-001",
      "source": "web-api",
      "timestamp": "2024-05-07T10:30:00Z",
      "payload": {"user_id": 42},
      "processed_at": "2024-05-07T10:30:00.123456+00:00"
    }
  ]
}
```

---

### `GET /stats` — Statistik sistem

```bash
curl http://localhost:8080/stats
```

**Response:**
```json
{
  "received": 6250,
  "unique_processed": 5000,
  "duplicate_dropped": 1250,
  "topics": ["auth.user.login", "infra.server.health", "payment.order.created"],
  "uptime_seconds": 42.5
}
```

---

### `GET /health` — Liveness probe

```bash
curl http://localhost:8080/health
# → {"status":"ok"}
```

---

## Simulasi Duplikasi (At-Least-Once)

```bash
# Kirim event yang sama 3 kali (simulasi at-least-once retry)
for i in 1 2 3; do
  curl -s -X POST http://localhost:8080/publish \
    -H "Content-Type: application/json" \
    -d '{"topic":"dedup.demo","event_id":"SAME-ID-001","timestamp":"2024-05-07T10:00:00Z","source":"demo","payload":{}}'
  echo ""
done

# Verifikasi: hanya 1 event tersimpan
curl "http://localhost:8080/events?topic=dedup.demo"
# → count: 1

# Stats menunjukkan 2 duplikat dibuang
curl http://localhost:8080/stats
# → duplicate_dropped: 2
```

---

## Demo Persistensi Dedup (Survive Restart)

```bash
# 1. Kirim event
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"persist.test","event_id":"PERSIST-001","timestamp":"2024-05-07T10:00:00Z","source":"demo","payload":{}}'

# 2. Restart container
docker restart uts-aggregator

# 3. Tunggu service ready
sleep 3 && curl http://localhost:8080/health

# 4. Kirim event YANG SAMA lagi — harus dibuang sebagai duplikat
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"topic":"persist.test","event_id":"PERSIST-001","timestamp":"2024-05-07T10:00:00Z","source":"demo","payload":{}}'

# 5. Stats: unique_processed=1, duplicate_dropped=1
curl http://localhost:8080/stats
```

---

## Menjalankan Unit Tests

```bash
# Install dependencies
pip install -r requirements.txt

# Jalankan semua tests
pytest tests/ -v

# Dengan coverage
pytest tests/ -v --tb=short
```

**Daftar Tests:**

| No. | Test | Cakupan |
|-----|------|---------|
| 1 | `test_schema_valid_event` | Validasi event valid → HTTP 202 |
| 2 | `test_schema_invalid_timestamp` | Timestamp non-ISO-8601 → HTTP 422 |
| 3 | `test_schema_missing_required_field` | Field wajib hilang → HTTP 422 |
| 4 | `test_dedup_duplicate_dropped` | Event sama 3x → hanya 1 di /events |
| 5 | `test_dedup_store_persistence` | Reinit SQLite → dedup tetap efektif |
| 6 | `test_dedup_different_topics` | event_id sama, topic berbeda → 2 event |
| 7 | `test_events_endpoint_consistency` | /events count == unique events dikirim |
| 8 | `test_stats_consistency` | received == unique + duplicate |
| 9 | `test_batch_publish` | Batch 50 events → semua diproses |
| 10 | `test_stress_batch_performance` | 1.000 events < 8 detik |

---

## Struktur Proyek

```
uts-aggregator/
├── src/
│   ├── __init__.py
│   ├── main.py          # FastAPI app factory + lifespan
│   ├── models.py        # Pydantic event models + validasi
│   ├── dedup_store.py   # SQLite dedup store (idempotency engine)
│   ├── queue_manager.py # asyncio.Queue + consumer worker
│   └── router.py        # API route handlers
├── publisher/
│   ├── __init__.py
│   └── publisher.py     # Standalone publisher (Docker Compose)
├── tests/
│   ├── __init__.py
│   ├── conftest.py      # pytest fixtures
│   └── test_aggregator.py  # 10 unit/integration tests
├── Dockerfile           # Aggregator image (non-root, slim)
├── Dockerfile.publisher # Publisher image (Docker Compose)
├── docker-compose.yml   # Bonus: dua service terpisah
├── requirements.txt
├── pytest.ini
├── README.md
└── report.md            # Laporan teori T1-T8 + arsitektur
```

---

## Asumsi Desain

1. **Ordering**: *Total ordering* tidak diperlukan untuk use case log aggregation. Event diurutkan berdasarkan `processed_at` (ingestion time) di SQLite.
2. **Queue loss on crash**: Event yang masih di `asyncio.Queue` saat aggregator crash akan hilang. Ini *acceptable* karena publisher dapat melakukan retry dan dedup store akan mencegah reprocessing event yang sudah tersimpan.
3. **received counter**: Counter `received` di `/stats` bersifat *per-session* (reset saat restart). Counter `unique_processed` bersifat *persistent* (diinisialisasi dari SQLite pada startup).
4. **Scope dedup**: `event_id` unik di scope `(topic, event_id)`. Event dengan `event_id` yang sama pada topic berbeda diperlakukan sebagai event independen.
5. **No external services**: Semua komponen berjalan lokal di dalam container. Tidak ada koneksi ke layanan eksternal.

---

## Referensi

Tanenbaum, A. S., & Van Steen, M. (2007). *Distributed systems: Principles and paradigms* (Edisi ke-2). Pearson Prentice Hall.
