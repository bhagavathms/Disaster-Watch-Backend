# Disaster Watch — Backend

A distributed, end-to-end disaster monitoring and analytics system built with Python, Kafka, MongoDB, and PostgreSQL.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Module Breakdown](#module-breakdown)
3. [Prerequisites](#prerequisites)
4. [Running the Project](#running-the-project)
5. [API Reference](#api-reference)
6. [Real-Time Frontend Integration](#real-time-frontend-integration)
7. [MongoDB Sharded Cluster](#mongodb-sharded-cluster)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  MODULE 1 — Synthetic Data Generation                                   │
│  generate_data.py  →  writer_service.py  →  data/*.json                 │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  JSON files
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  MODULE 2 — Kafka Producer                                              │
│  services/kafka-producer/producer.py                                    │
│  Reads JSON → publishes to 4 Kafka topics                               │
└──────────────┬──────────────────────────────────────────────────────────┘
               │  Kafka topics (earthquakes / fires / floods / storms)
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐  ┌──────────────────────────────────────────────────────┐
│  MODULE 3   │  │  MODULE 4 — MongoDB Writer                           │
│  Consumer   │  │  services/mongodb-writer/writer.py                   │
│  Processor  │  │  Kafka → batch upsert → MongoDB                     │
│             │  │                                                      │
│  Classifies │  │  disaster_db.disaster_events                         │
│  severity   │  │  (GeoJSON, 2dsphere index)                           │
│  LOW/MED/   │  └──────────────────┬───────────────────────────────────┘
│  HIGH/CRIT  │                     │
└─────────────┘                     ▼
                        ┌───────────────────────────────────────────────┐
                        │  MongoDB — Sharded Cluster (via mongos :28020)│
                        │                                               │
                        │  ┌── Shard 1 (rs1) ──┐  ┌── Shard 2 (rs2) ──┐│
                        │  │ :28017 :28018 :28019│  │ :28021 :28022 :28023││
                        │  └───────────────────┘  └───────────────────┘│
                        │  ┌── Shard 3 (rs3) ──┐  ┌── Config (configRS)┐│
                        │  │ :28024 :28025 :28026│  │       :28100       ││
                        │  └───────────────────┘  └───────────────────┘│
                        └──────────────────┬────────────────────────────┘
                                           │
                                           ▼
                        ┌──────────────────────────────────────────────┐
                        │  MODULE 5 — ETL Service                      │
                        │  services/etl-service/etl_runner.py          │
                        │                                              │
                        │  Extract (MongoDB) → Transform → Load        │
                        └──────────────────┬───────────────────────────┘
                                           │
                                           ▼
                        ┌──────────────────────────────────────────────┐
                        │  PostgreSQL — Star Schema (disaster_dw)      │
                        │                                              │
                        │  dim_event_type   dim_location               │
                        │  dim_time         fact_disaster_events        │
                        └──────────────────┬───────────────────────────┘
                                           │
                                           ▼
                        ┌──────────────────────────────────────────────┐
                        │  MODULE 6 — Query API (FastAPI)              │
                        │  services/query-api/api.py  :8000            │
                        │                                              │
                        │  /events/*    ──→  MongoDB  (operational)    │
                        │  /analytics/* ──→  PostgreSQL (analytical)   │
                        └──────────────────────────────────────────────┘
```

### Data Flow Summary

| Step | From | To | What moves |
|------|------|----|------------|
| 1 | `generate_data.py` | `data/*.json` | 400–1M synthetic events |
| 2 | JSON files | Kafka topics | Raw event stream |
| 3 | Kafka | MongoDB | Processed + classified events |
| 4 | MongoDB | PostgreSQL | ETL → star schema facts |
| 5 | Both DBs | REST API | Query responses |

---

## Module Breakdown

### Module 1 — Synthetic Data Generation
**Files:** `generate_data.py`, `writer_service.py`, `main.py`

Generates realistic synthetic disaster events using real statistical distributions:
- **Earthquakes** — log-normal Richter magnitude, aftershock injection, clustered around Ring of Fire
- **Fires** — exponential intensity (mega-fires + brush fires), Amazon/California/Siberia clusters
- **Floods** — right-skewed water-level rise, Ganges/Mississippi/Rhine deltas
- **Storms** — Gaussian wind speed, Gulf of Mexico/Bay of Bengal/Pacific typhoon corridors

Outputs 4 JSON files to `data/`. Scale with `--scale N` to multiply event counts.

---

### Module 2 — Kafka Producer
**Files:** `services/kafka-producer/producer.py`, `config.py`

Reads the 4 JSON files, merges and sorts events by timestamp, then streams them to Kafka:
- Topics: `earthquakes-topic`, `fires-topic`, `floods-topic`, `storms-topic`
- Supports `--dry-run` (no Kafka needed), `--speed FAST|NORMAL|SLOW`
- Uses `event_id` as the Kafka message key for partitioning

---

### Module 3 — Kafka Consumer Processor
**Files:** `services/kafka-consumer/consumer.py`, `processor.py`, `config.py`

Subscribes to all 4 topics (consumer group: `disaster-processor-group`):
1. **Validate** — schema check
2. **Classify** — severity threshold lookup per event type
3. **Filter** — drop events below minimum threshold
4. **Enrich** — rename fields, add `processed_at` timestamp

Severity thresholds:
| Type | LOW | MEDIUM | HIGH | CRITICAL |
|------|-----|--------|------|----------|
| Earthquake (Richter) | 1.5 | 3.0 | 5.0 | 7.0 |
| Fire (MW power) | 5 | 20 | 50 | 200 |
| Flood (FMI) | 0.5 | 1.0 | 2.0 | 3.0 |
| Storm (km/h) | 35 | 62 | 120 | 200 |

---

### Module 4 — MongoDB Writer
**Files:** `services/mongodb-writer/writer.py`, `db.py`, `config.py`

Independent Kafka consumer (group: `disaster-storage-group`) — receives the full raw event stream separately from Module 3:
- Batches events (50 per batch) then bulk-upserts to MongoDB
- Upsert key: `event_id` — re-running is always safe, no duplicates
- Creates 4 indexes: `2dsphere` (geo), `event_time DESC`, compound type+severity+time, `source`
- Transforms lat/lon into GeoJSON Point objects for geospatial queries

---

### Module 5 — ETL Service
**Files:** `services/etl-service/etl_runner.py`, `extract.py`, `transform.py`, `load.py`, `db_mongo.py`, `db_postgres.py`

Three-stage pipeline:

**Extract** — pulls processed events from MongoDB (supports `--since DATE` for incremental loads)

**Transform** — maps lat/lon to country/region (bounding-box lookup), decomposes timestamps into star-schema columns

**Load** — upserts into PostgreSQL star schema:
```
dim_event_type (event_type_id, event_type_name)
dim_location   (location_id, country, region, latitude, longitude)
dim_time       (time_id, date, day, month, year, hour)
fact_disaster_events (fact_id, event_id FK→all dims, severity_level, source)
```

---

### Module 6 — Query API
**Files:** `services/query-api/api.py`, `routers/events.py`, `routers/analytics.py`, `mongo_client.py`, `postgres_client.py`

FastAPI service with two read paths:

**MongoDB endpoints** (operational — individual event queries):
- `GET /events/recent` — latest N events with optional type filter
- `GET /events/search` — filtered + paginated (type, severity, date range)
- `GET /events/by-location` — geospatial bounding-box query

**PostgreSQL endpoints** (analytical — aggregations):
- `GET /analytics/event-counts` — totals per disaster type
- `GET /analytics/monthly-trends` — monthly volume with optional filters
- `GET /analytics/locations/top` — top N regions by event count

---

## Prerequisites

| Dependency | Version | Purpose |
|------------|---------|---------|
| Python | 3.9+ | Runtime |
| MongoDB | 8.2 | Document store |
| PostgreSQL | 18 | Data warehouse |
| Apache Kafka | Any | Event streaming (optional — pipeline has simulate mode) |

**Python packages** (already installed):
```bash
pip install pymongo psycopg2-binary kafka-python fastapi "uvicorn[standard]" pydantic
```

---

## Running the Project

### Every Time (Start Databases)

**Option A — Standard single-node MongoDB (simpler, uses Windows service on :27017):**
```bash
# MongoDB Windows service is already running on :27017 — no start needed

# Start PostgreSQL (fix stale lock if needed, then start)
rm -f "/c/Program Files/PostgreSQL/18/data/postmaster.pid"
"/c/Program Files/PostgreSQL/18/bin/pg_ctl.exe" start -D "/c/Program Files/PostgreSQL/18/data"

# Run the pipeline
python run_pipeline.py
```

**Option B — Full sharded MongoDB cluster (:28020 mongos, see section below):**
```bash
# Start the sharded cluster (11 processes)
bash mongo_cluster/start_cluster.sh

# Start PostgreSQL
rm -f "/c/Program Files/PostgreSQL/18/data/postmaster.pid"
"/c/Program Files/PostgreSQL/18/bin/pg_ctl.exe" start -D "/c/Program Files/PostgreSQL/18/data"

# Run the pipeline
python run_pipeline.py
```

---

### Pipeline Commands

```bash
# Full pipeline (MongoDB + PostgreSQL must be running)
python run_pipeline.py

# Dry-run — no external services needed at all
python run_pipeline.py --dry-run

# Scale up data volume (big data demo)
python run_pipeline.py --scale 100     # ~40,000 events
python run_pipeline.py --scale 1000    # ~400,000 events

# Run pipeline but don't start the API
python run_pipeline.py --skip-api

# Skip pipeline, just start the API
python run_pipeline.py --api-only
```

---

### Run Individual Modules

```bash
# Module 1 — Generate data
python main.py
python main.py --scale 100

# Module 2 — Kafka Producer (dry-run, no Kafka needed)
cd services/kafka-producer
python producer.py --dry-run

# Module 3 — Consumer Processor (simulate, no Kafka needed)
cd services/kafka-consumer
python consumer.py --simulate

# Module 4 — MongoDB Writer (simulate + dry-run, no Kafka or Mongo needed)
cd services/mongodb-writer
python writer.py --simulate --dry-run

# Module 4 — MongoDB Writer (simulate, writes to real MongoDB)
python writer.py --simulate

# Module 5 — ETL (simulate + dry-run, no external DBs needed)
cd services/etl-service
python etl_runner.py --simulate --dry-run

# Module 5 — ETL (simulate, writes to real PostgreSQL)
python etl_runner.py --simulate --create-tables

# Module 6 — API Server
cd services/query-api
uvicorn api:app --host 127.0.0.1 --port 8000 --reload
```

---

### PostgreSQL Setup (one-time)

```bash
# If PostgreSQL won't start (stale lock file)
rm -f "/c/Program Files/PostgreSQL/18/data/postmaster.pid"
"/c/Program Files/PostgreSQL/18/bin/pg_ctl.exe" start -D "/c/Program Files/PostgreSQL/18/data"

# Create the database (one-time)
"/c/Program Files/PostgreSQL/18/bin/createdb.exe" -U postgres disaster_dw

# Stop PostgreSQL
"/c/Program Files/PostgreSQL/18/bin/pg_ctl.exe" stop -D "/c/Program Files/PostgreSQL/18/data"
```

PostgreSQL connection details:
- Host: `localhost` | Port: `5432`
- Database: `disaster_dw` | User: `postgres` | Password: `postgres`

---

### MongoDB Connection Details

- **Standard mode:** `mongodb://localhost:27017/` — database: `disaster_db`
- **Sharded cluster:** `mongodb://localhost:28020/` — database: `disaster_db` (via mongos)

> Note: Port `27017` is reserved by the MongoDB Windows service (standalone). The sharded cluster uses `28xxx` ports to avoid conflict.

---

## API Reference

Base URL: `http://127.0.0.1:8000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | MongoDB + PostgreSQL connectivity check |
| GET | `/docs` | Interactive Swagger UI |
| GET | `/events/recent` | Latest N events (`?limit=20&event_type=earthquake`) |
| GET | `/events/search` | Filtered search (`?event_type=flood&severity_level=CRITICAL&page=1`) |
| GET | `/events/by-location` | Bounding-box geo query (`?min_lat=30&max_lat=46&min_lon=60&max_lon=90`) |
| GET | `/analytics/event-counts` | Total events per disaster type |
| GET | `/analytics/monthly-trends` | Monthly volume (`?event_type=storm&year=2024`) |
| GET | `/analytics/locations/top` | Top regions by count (`?limit=10&event_type=fire`) |

---

## Real-Time Frontend Integration

The backend exposes two data paths for the frontend:

| Path | Source | Use |
|------|--------|-----|
| **Real-time stream** | MongoDB (live writes) | Live disaster feed, maps, counters |
| **Historical analytics** | PostgreSQL (star schema) | Charts, trends, top regions |

---

### Real-Time Stream (SSE)

Connect with the browser's native `EventSource` API — no library needed.

```js
const es = new EventSource("http://127.0.0.1:8000/stream/events");

es.onmessage = (event) => {
  const frame = JSON.parse(event.data);

  if (frame.type === "connected") {
    console.log("Stream live since", frame.timestamp);
  }

  if (frame.type === "event") {
    const ev = frame.data;
    // ev.event_id       — UUID string
    // ev.event_type     — "earthquake" | "fire" | "flood" | "storm"
    // ev.severity_level — "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    // ev.latitude       — float
    // ev.longitude      — float
    // ev.event_time     — ISO 8601 UTC string (original event timestamp)
    // ev.processed_at   — ISO 8601 UTC string (when it hit MongoDB)
    // ev.source         — string (data source label)
    console.log("New event:", ev);
  }
};

es.onerror = () => console.error("SSE connection dropped");
```

The stream polls MongoDB every 1 second for newly inserted documents. Each document is pushed immediately as it arrives — no batching.

---

### Simulation Control (start / stop realistic data feed)

The `event_streamer.py` script inserts events one-by-one with a configurable delay so the frontend sees a realistic drip-feed instead of a bulk dump.

**Start the simulation** (inserts one event every 0.5 seconds by default):

```bash
# From terminal
python services/realtime-service/event_streamer.py --delay 0.5

# Or via API (starts it as a background process)
curl -X POST "http://127.0.0.1:8000/stream/start?delay=0.5"
```

**Stop it:**

```bash
curl -X POST "http://127.0.0.1:8000/stream/stop"
```

**Check status:**

```bash
curl "http://127.0.0.1:8000/stream/status"
# {"simulation_running": true, "pid": 12345}
```

**Parameters:**

| Param | Default | Description |
|-------|---------|-------------|
| `delay` | `0.5` | Seconds between each event insertion |
| `scale` | `1` | Multiply the event dataset N times |

Examples:
```bash
# Fast demo — one event every 100ms
curl -X POST "http://127.0.0.1:8000/stream/start?delay=0.1"

# Slow, realistic — one event every 2 seconds
curl -X POST "http://127.0.0.1:8000/stream/start?delay=2.0"

# Scale up the dataset 5x (more events total)
curl -X POST "http://127.0.0.1:8000/stream/start?delay=0.5&scale=5"
```

---

### Historical Analytics Endpoints

All return JSON. Use these to populate charts and summary stats.

**Event counts per disaster type:**
```
GET /analytics/event-counts
```
```json
[
  {"event_type": "earthquake", "total": 8241},
  {"event_type": "fire",       "total": 7903},
  {"event_type": "flood",      "total": 7844},
  {"event_type": "storm",      "total": 7102}
]
```

**Monthly volume (for line/bar charts):**
```
GET /analytics/monthly-trends?event_type=storm&year=2024
```
```json
[
  {"year": 2024, "month": 1, "total": 312},
  {"year": 2024, "month": 2, "total": 289},
  ...
]
```

**Top regions by event count:**
```
GET /analytics/locations/top?limit=10&event_type=fire
```
```json
[
  {"country": "Brazil",        "region": "South America", "total": 1204},
  {"country": "United States", "region": "North America", "total": 987},
  ...
]
```

---

### All REST Endpoints at a Glance

Base URL: `http://127.0.0.1:8000`

| Method | Endpoint | Query params | Description |
|--------|----------|--------------|-------------|
| GET | `/health` | — | MongoDB + PostgreSQL connectivity check |
| GET | `/docs` | — | Interactive Swagger UI (auto-generated) |
| GET | `/stream/events` | — | **SSE** — live event stream from MongoDB |
| POST | `/stream/start` | `delay`, `scale` | Start realistic simulation streamer |
| POST | `/stream/stop` | — | Stop simulation streamer |
| GET | `/stream/status` | — | Is simulation running? PID? |
| GET | `/events/recent` | `limit`, `event_type` | Latest N events from MongoDB |
| GET | `/events/search` | `event_type`, `severity_level`, `date_from`, `date_to`, `page`, `page_size` | Filtered + paginated events |
| GET | `/events/by-location` | `min_lat`, `max_lat`, `min_lon`, `max_lon`, `limit` | Bounding-box geo query |
| GET | `/analytics/event-counts` | `event_type` | Totals per disaster type |
| GET | `/analytics/monthly-trends` | `event_type`, `year` | Monthly volume |
| GET | `/analytics/locations/top` | `limit`, `event_type` | Top regions by count |

---

### Event Object Schema

Every event returned by `/events/*` endpoints and the SSE stream has this shape:

```json
{
  "event_id":       "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
  "event_type":     "earthquake",
  "severity_level": "HIGH",
  "latitude":       35.6892,
  "longitude":      139.6917,
  "event_time":     "2024-03-15T14:22:00Z",
  "processed_at":   "2024-03-15T14:22:03Z",
  "source":         "USGS-synthetic"
}
```

---

### CORS

The API allows `GET` and `POST` from any origin (`*`). No special headers needed from the frontend.

---

## MongoDB Sharded Cluster

Architecture: **3 shards × 3 replica-set members = 9 mongod processes**

```
mongos router        :28020   ← app connects here
config server        :28100   (configRS — 1 node)
Shard 1 (rs1)        :28017, :28018, :28019
Shard 2 (rs2)        :28021, :28022, :28023
Shard 3 (rs3)        :28024, :28025, :28026
```

> Ports use `28xxx` (not `27xxx`) to avoid conflict with the MongoDB Windows service permanently occupying port `27017`.

**One-time setup:**
```bash
bash mongo_cluster/setup_cluster.sh
```

**Daily startup:**
```bash
bash mongo_cluster/start_cluster.sh
```

**Shutdown:**
```bash
bash mongo_cluster/stop_cluster.sh
```

Shard key: `{_id: "hashed"}` — distributes documents evenly across all 3 shards based on UUID hash.

> **RAM requirement:** ~2–3 GB for all 11 processes. Close other apps before running.
