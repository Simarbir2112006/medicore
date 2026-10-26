<div align="center">

# 🏥 MediCore

### Real-Time Clinical Backend System

![Python](https://img.shields.io/badge/Python-3.13-blue?style=flat-square&logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.141-009688?style=flat-square&logo=fastapi)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.5-231F20?style=flat-square&logo=apachekafka)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=flat-square&logo=redis)
![PostgreSQL](https://img.shields.io/badge/TimescaleDB-PG15-336791?style=flat-square&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)

A production-grade backend for real-time patient vitals monitoring — built with Kafka, Redis, TimescaleDB, FastAPI, and an LLM-powered clinical assistant.

**[Demo Video](#)** · **[API Docs](http://localhost:8000/docs)**

</div>

---

## Overview

MediCore simulates a hospital's clinical data infrastructure. Patient monitors stream vitals data every second into a Kafka pipeline, where anomalies are detected in real time, alerts are cached in Redis, and doctors can query live data or ask an AI assistant for clinical insights — all through a JWT-secured REST and WebSocket API.

The entire system starts with a single command:

```bash
docker compose up
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Patient Monitor                        │
│              (Simulated Vitals Producer)                 │
└─────────────────────┬───────────────────────────────────┘
                      │  1 reading/sec per patient
                      ▼
┌─────────────────────────────────────────────────────────┐
│                  Apache Kafka                            │
│              topic: patient_vitals                       │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│              Kafka Consumer                              │
│         asyncio + ThreadPoolExecutor                     │
│                                                          │
│   ┌──────────────┐  ┌─────────────┐  ┌───────────────┐ │
│   │ TimescaleDB  │  │  Anomaly    │  │ Redis Alert   │ │
│   │  (vitals     │  │  Detector   │  │    Engine     │ │
│   │  history)    │  │ (Z-score +  │  │ (TTL + sorted │ │
│   │              │  │ thresholds) │  │    set)       │ │
│   └──────────────┘  └─────────────┘  └───────────────┘ │
└─────────────────────┬───────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────┐
│                 FastAPI Gateway                          │
│              JWT Auth · Pydantic v2                      │
│                                                          │
│   REST API  │  WebSocket Feed  │  LLM Assistant         │
│   /alerts   │  /ws/vitals/{id} │  /ask (Groq RAG)       │
│   /vitals   │                  │                         │
└─────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology |
|---|---|
| **Message Queue** | Apache Kafka + Zookeeper |
| **Time-Series Database** | PostgreSQL + TimescaleDB |
| **Cache & Alerts** | Redis |
| **API Framework** | FastAPI + Pydantic v2 |
| **Authentication** | JWT (HS256) + bcrypt |
| **Concurrency** | asyncio + ThreadPoolExecutor |
| **LLM** | Groq API (Llama 3) via RAG |
| **Containerization** | Docker + docker-compose |

---

## Features

- **Real-time vitals ingestion** via Kafka with per-patient partition ordering
- **Two-layer anomaly detection** — hard clinical thresholds + Z-score against each patient's personal rolling baseline
- **Redis alert engine** with TTL-based deduplication and sorted set priority queue
- **JWT-secured REST API** with FastAPI dependency injection
- **WebSocket endpoint** for live vitals push to doctor dashboards (no polling)
- **LLM clinical assistant** using RAG — fetches real patient data before calling Groq
- **Full Docker containerization** with health checks and proper service dependency ordering
- **TimescaleDB hypertables** for efficient time-range queries at scale

---

## Getting Started

### Prerequisites

- Docker Desktop (WSL2 backend on Windows)
- A free [Groq API key](https://console.groq.com)

### Setup

```bash
# Clone
git clone https://github.com/Simarbir2112006/medicore.git
cd medicore

# Configure environment
cp .env.example .env
# Edit .env and add your values (see .env.example)

# Start everything
docker compose up

# Initialize database schema (first run only)
docker exec -i medicore_timescaledb psql -U medicore -d medicore_db < schema.sql
```

### Verify

```bash
curl http://localhost:8000/health
# {"status": "ok", "service": "Medicore API"}
```

API docs available at `http://localhost:8000/docs`

---

## API Quick Reference

### Login

```bash
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=doctor01&password=password123"
```

### Get Active Alerts

```bash
curl http://localhost:8000/alerts \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### Get Patient Vitals (last 24h)

```bash
curl http://localhost:8000/patients/{patient_id}/vitals \
  -H "Authorization: Bearer YOUR_TOKEN"
```

### Ask the Clinical Assistant

```bash
curl -X POST http://localhost:8000/ask \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"patient_id": "...", "question": "Are there any concerning patterns?"}'
```

### Live Vitals WebSocket

```python
import asyncio, websockets, json

async def watch():
    async with websockets.connect("ws://localhost:8000/ws/vitals/{patient_id}") as ws:
        while True:
            print(json.loads(await ws.recv()))

asyncio.run(watch())
```

---

## Project Structure

```
medicore/
├── producer/
│   └── vitals_producer.py      # Kafka producer — simulates patient monitor
├── consumer/
│   └── vitals_consumer.py      # Kafka consumer — async DB writes + anomaly detection
├── anomaly/
│   └── detector.py             # Z-score + threshold anomaly detection
├── redis_engine/
│   └── alert_engine.py         # Redis alert storage and retrieval
├── api/
│   ├── main.py                 # FastAPI app — all endpoints
│   ├── auth.py                 # JWT + bcrypt
│   ├── dependencies.py         # Dependency injection
│   ├── models.py               # Pydantic models
│   └── llm.py                  # Groq LLM integration
├── schema.sql                  # TimescaleDB schema and hypertable
├── Dockerfile                  # Python image for all three services
├── docker-compose.yml          # Full stack with health checks
└── .env.example
```

---

## Default Credentials

```
username: doctor01
password: password123
```

For local development only.

---

<div align="center">
Built with Python · FastAPI · Kafka · Redis · TimescaleDB · Docker · Groq
</div>