# MediCore — Real-Time Clinical Backend System

A production-grade clinical data backend built to demonstrate real-world backend engineering: real-time data ingestion, anomaly detection, caching, and a secured REST API gateway.

## Architecture

```
Patient Monitor (Simulated)
         │
         ▼
   Kafka Producer
         │
         ▼
   Kafka Broker (topic: patient_vitals)
         │
         ▼
   Kafka Consumer (asyncio + ThreadPoolExecutor)
    ├── TimescaleDB        ← permanent time-series storage
    ├── Anomaly Detector   ← Z-score + hard thresholds
    └── Redis Alert Engine ← TTL deduplication + sorted set priority queue
         │
         ▼
   FastAPI Gateway (JWT auth, dependency injection)
    ├── REST:      /patients, /vitals, /alerts
    ├── WebSocket: /ws/vitals/{patient_id}     [in progress]
    └── LLM:       /ask  (Groq Llama 3)        [in progress]
```

## Stack

| Layer | Technology | Why |
|---|---|---|
| Message Queue | Apache Kafka | Durable, replayable, decoupled ingestion |
| Time-Series DB | PostgreSQL + TimescaleDB | Hypertable chunking for unbounded vitals data |
| Cache + Alerts | Redis | Sub-millisecond alert lookup, TTL-based auto-expiry |
| API Gateway | FastAPI + Pydantic v2 | Async, auto-documented, type-safe |
| Auth | JWT (HS256) + bcrypt | Stateless auth, secure password hashing |
| Concurrency | asyncio + ThreadPoolExecutor | I/O-bound async, CPU-bound threaded |
| Infrastructure | Docker + docker-compose | Single-command local deployment |
| LLM | Groq API (Llama 3) | Clinical Q&A assistant |

## Key Engineering Decisions

**Why Kafka instead of direct DB writes?**
Decouples the patient monitor from the processing pipeline. If the anomaly detector crashes, Kafka holds messages and replays them on restart. Zero data loss.

**Why TimescaleDB instead of plain PostgreSQL?**
Hypertables automatically partition vitals data by time into chunks. A query for "last 24 hours" only scans today's chunk, not billions of historical rows. Chunk exclusion makes time-range queries fast regardless of total data size.

**Why Redis for alerts instead of querying TimescaleDB?**
Active alerts need sub-millisecond lookup for a real-time dashboard. Redis TTL auto-expires stale alerts without a cleanup job. The sorted set enables O(log N) priority retrieval by recency.

**Why manual Kafka offset commits?**
Auto-commit commits on receive, not on successful processing. If the consumer crashes between receiving and writing to TimescaleDB, the message is lost. Manual commit after confirmed DB write guarantees at-least-once delivery — a vital reading is never silently dropped.

**Why asyncio + ThreadPoolExecutor?**
Database and Kafka operations are I/O-bound — asyncio handles them concurrently on one thread. Anomaly detection is CPU-bound — ThreadPoolExecutor offloads it to a worker thread so the event loop is never blocked.

## Project Structure

```
medicore/
├── producer/
│   └── vitals_producer.py     # Simulates patient monitor, sends to Kafka
├── consumer/
│   └── vitals_consumer.py     # Reads Kafka, stores to DB, detects anomalies
├── anomaly/
│   └── detector.py            # Z-score + threshold anomaly detection
├── redis_engine/
│   └── alert_engine.py        # Redis alert storage, deduplication, retrieval
├── api/
│   ├── main.py                # FastAPI app, endpoints
│   ├── auth.py                # JWT creation, verification, bcrypt
│   ├── dependencies.py        # Dependency injection (auth guard, DB pool)
│   └── models.py              # Pydantic request/response models
├── schema.sql                 # TimescaleDB schema and hypertable setup
├── docker-compose.yml         # Kafka, Zookeeper, Redis, TimescaleDB
├── .env.example               # Environment variable reference
└── requirements.txt
```

## Getting Started

### Prerequisites

- Docker Desktop with WSL2 backend enabled
- Python 3.11+

### 1. Clone and configure

```bash
git clone https://github.com/Simarbir2112006/medicore.git
cd medicore
cp .env.example .env
```

Edit `.env` with your values:

```
DB_USER=medicore
DB_PASSWORD=your_password
DB_NAME=medicore_db
JWT_SECRET_KEY=your-secret-key
```

### 2. Start infrastructure

```bash
docker compose up -d
```

Starts Kafka, Zookeeper, Redis, and TimescaleDB in Docker.

### 3. Set up Python environment

```bash
python3 -m venv backend
source backend/bin/activate
pip install -r requirements.txt
```

### 4. Initialize the database schema

```bash
docker exec -i medicore_timescaledb psql -U medicore -d medicore_db < schema.sql
```

### 5. Run the pipeline

```bash
# Terminal 1 — Vitals producer (simulates patient monitor)
python -m producer.vitals_producer

# Terminal 2 — Kafka consumer + anomaly detection + Redis alerts
python -m consumer.vitals_consumer

# Terminal 3 — REST API gateway
uvicorn api.main:app --reload --port 8000
```

API docs available at: `http://localhost:8000/docs`

## API Reference

### Authentication

```bash
curl -X POST "http://localhost:8000/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=doctor01&password=password123"
```

Returns a JWT token. Include it in all subsequent requests:

```
Authorization: Bearer YOUR_TOKEN
```

### Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/auth/login` | Login, receive JWT token |
| GET | `/health` | Health check (no auth required) |
| GET | `/alerts` | Get active alerts from Redis |
| POST | `/patients` | Register a new patient |
| GET | `/patients/{id}/vitals` | Get vitals history (default: last 24h) |
| GET | `/patients/{id}/vitals?hours=48` | Get vitals for custom time range |

### Example: Get Active Alerts

```bash
curl -X GET "http://localhost:8000/alerts" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

```json
[
  {
    "patient_id": "bda59fef-f612-4d83-81d9-ebd22c0520e6",
    "metric": "heart_rate",
    "value": 200.0,
    "severity": "critical",
    "reason": "threshold_breach",
    "timestamp": "2026-10-16T17:30:38+00:00"
  }
]
```

### Example: Get Patient Vitals

```bash
curl -X GET "http://localhost:8000/patients/bda59fef-f612-4d83-81d9-ebd22c0520e6/vitals?hours=1" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## Anomaly Detection

Two-layer detection runs on every incoming vital reading:

**Layer 1 — Hard Thresholds** (catches physically dangerous values)

| Metric | Min | Max |
|---|---|---|
| Heart Rate | 20 bpm | 300 bpm |
| Oxygen Level | 50% | 100% |
| BP Systolic | 80 mmHg | 180 mmHg |
| BP Diastolic | 50 mmHg | 110 mmHg |

**Layer 2 — Z-Score** (catches deviations from a patient's personal baseline)

Uses a rolling window of the last 300 readings per patient. A reading more than 3 standard deviations from the patient's own baseline triggers a `statistical_anomaly` warning — even if it falls within population-normal ranges.

This catches patient-specific anomalies that fixed thresholds miss: a resting heart rate of 95 bpm is normal for most people but anomalous for a patient whose baseline is 55 bpm.

## Default Test Credentials

```
Username: doctor01
Password: password123
```

These are for local development only. Change before any deployment.

## What's In Progress

- **Step 9:** WebSocket endpoint for live vitals feed to doctor dashboard
- **Step 10:** Groq LLM endpoint for clinical Q&A (`/ask`)
- **Step 11:** Full Docker containerisation of the Python services with health checks
- **Step 12:** Nurse dashboard — patient status, care events, pre-entry briefing