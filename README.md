# Disaster Watch — Backend

A distributed, end-to-end disaster monitoring and analytics system built with Python, Kafka, MongoDB (sharded cluster), and PostgreSQL. Covers the full data lifecycle: synthetic generation → stream processing → dual-database storage → REST + SSE query API.

---

## Table of Contents

1. [System Architecture](#system-architecture)
2. [Module Breakdown](#module-breakdown)
3. [Data Schemas](#data-schemas)
4. [Database Design](#database-design)
5. [Prerequisites](#prerequisites)
6. [Running the Project](#running-the-project)
7. [API Reference](#api-reference)
8. [Real-Time Integration](#real-time-integration)
9. [Key Design Decisions](#key-design-decisions)
10. [MongoDB Sharded Cluster](#mongodb-sharded-cluster)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  MODULE 1 — Data Generation                                                  │
│  generate_data.py  →  normalizer.py  →  data/*.json                          │
│  Raw source schemas (USGS / NASA FIRMS / USGS HWM / IBTrACS)                │
│  → normalised unified Kafka schema                                           │
└───────────────────────────────┬─────────────────────────────────────────────┘
                                │  4 × JSON files
                                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  MODULE 2 — Kafka Producer                                                   │
│  services/kafka-producer/producer.py                                         │
│  Reads JSON → publishes to 4 Kafka topics (sorted by timestamp)              │
└──────────────┬──────────────────────────────────────────────────────────────┘
               │  earthquakes-topic / fires-topic / floods-topic / storms-topic
       ┌───────┴────────┐
       ▼                ▼
┌─────────────┐  ┌─────────────────────────────────────────────────────────────┐
│  MODULE 3   │  │  MODULE 4 — MongoDB Writer                                  │
│  Consumer   │  │  services/mongodb-writer/writer.py                          │
│  Processor  │  │  Kafka → classify → batch upsert → MongoDB                  │
│             │  │  Consumer group: disaster-storage-group                      │
│  group:     │  └──────────────────────┬──────────────────────────────────────┘
│  disaster-  │                         │
│  processor- │                         ▼
│  group      │      ┌──────────────────────────────────────────────────────┐
└─────────────┘      │  MongoDB — Sharded Cluster (mongos @ :28020)         │
                     │  disaster_db.disaster_events (2dsphere indexed)       │
                     │  disaster_db.live_events     (TTL 8h)                │
                     │                                                        │
                     │  Shard 1 (rs1): :28017 :28018 :28019                  │
                     │  Shard 2 (rs2): :28021 :28022 :28023                  │
                     │  Shard 3 (rs3): :28024 :28025 :28026                  │
                     │  Config Server:  :28100                               │
                     └──────────────────────┬───────────────────────────────┘
                                            │
                                            ▼
                     ┌──────────────────────────────────────────────────────┐
                     │  MODULE 5 — ETL Service                              │
                     │  services/etl-service/etl_runner.py                  │
                     │  Extract (MongoDB) → Transform → Load (PostgreSQL)    │
                     │  Batch commits every 10,000 rows                      │
                     └──────────────────────┬───────────────────────────────┘
                                            │
                                            ▼
                     ┌──────────────────────────────────────────────────────┐
                     │  PostgreSQL — Star Schema (disaster_dw @ :5432)      │
                     │  dim_event_type  dim_location  dim_time              │
                     │  fact_disaster_events                                 │
                     └──────────────────────┬───────────────────────────────┘
                                            │
                                            ▼
                     ┌──────────────────────────────────────────────────────┐
                     │  MODULE 6 — Query API (FastAPI @ :8000)              │
                     │                                                        │
                     │  /events/*      →  MongoDB  (operational queries)     │
                     │  /analytics/*   →  PostgreSQL (aggregations)          │
                     │  /events/map    →  PostgreSQL (H3 clustered map)      │
                     │  /stream/events →  SSE ← MongoDB (real-time feed)    │
                     │  /live/*        →  SSE ← live_events (TTL window)    │
                     └──────────────────────────────────────────────────────┘
```

### Read Path Split

| Endpoint group | Database | Why |
|---|---|---|
| `/events/*` | MongoDB | Low-latency individual event lookups, geospatial queries, variable predicates |
| `/analytics/*` | PostgreSQL | GROUP BY aggregations, star-schema joins, monthly trends |
| `/events/map` | PostgreSQL | Spatial clustering (H3), date-range filtering at scale |
| `/stream/*`, `/live/*` | MongoDB | Real-time polling by `processed_at` timestamp |

---

## Module Breakdown

### Module 1 — Synthetic Data Generation

**Files:** `generate_data.py`, `normalizer.py`, `main.py`

Two-stage pipeline:

**Stage 1 — Raw generation** (`generate_data.py`): produces events in the native schema of each real-world data provider:

| Disaster | Source schema | Key fields |
|---|---|---|
| Earthquake | USGS GeoJSON Feature | `properties.mag`, `properties.time` (epoch ms), `geometry.coordinates` |
| Fire | NASA FIRMS MODIS | `frp` (fire radiative power MW), `acq_date`, `acq_time`, `brightness` |
| Flood | USGS High Water Marks | `height_above_gnd_ft`, `peak_date`, `hwm_environment` |
| Storm | IBTrACS Best Track | `wmo_wind` (knots), `iso_time`, `basin`, `nature` |

Geographic realism:
- **Earthquakes** — Ring of Fire clusters (Japan, Chile, Indonesia, Pacific NW), aftershock injection
- **Fires** — Amazon, California, SE Asia, Siberia, Mediterranean
- **Floods** — Ganges delta, Mississippi, Mekong, Rhine, Yangtze
- **Storms** — Gulf of Mexico, Bay of Bengal, Western Pacific typhoon corridor
- Seasonal monthly weights (e.g. storms peak August–October)
- Log-normal year variability across 1980–2025

**Stage 2 — Normalisation** (`normalizer.py`): converts raw schemas to unified Kafka schema:

```json
{
  "event_id":    "uuid4",
  "event_type":  "earthquake | fire | flood | storm",
  "severity_raw": 6.2,
  "latitude":    35.123,
  "longitude":   139.456,
  "timestamp":   "2023-08-15T01:12:00+00:00",
  "source":      "USGS | NASA-FIRMS | USGS-HWM | IBTrACS"
}
```

Normalisation rules:
- Earthquake: `severity_raw = mag`; timestamp from epoch ms ÷ 1000
- Fire: `severity_raw = frp`; timestamp from `acq_date + acq_time` (HHMM)
- Flood: `severity_raw = height_ft × 30.48` (→ cm); timestamp from `peak_date`
- Storm: `severity_raw = wmo_wind × 1.852` (knots → km/h); timestamp from `iso_time`

Raw files written to `data/raw/`, unified files to `data/`.

---

### Module 2 — Kafka Producer

**Files:** `services/kafka-producer/producer.py`, `config.py`

- Loads all 4 unified JSON files, merges and sorts by ISO-8601 timestamp
- Publishes to 4 topics: `earthquakes-topic`, `fires-topic`, `floods-topic`, `storms-topic`
- Uses `event_id` as the Kafka message key (ensures same event always hits same partition)
- Supports `--dry-run` (validates without Kafka), `--speed FAST|NORMAL|SLOW`
- Non-blocking sends with a single flush at end for throughput

Replay speeds: `FAST` = 0.1s, `NORMAL` = 0.5s, `SLOW` = 2.0s between events.

---

### Module 3 — Kafka Consumer Processor

**Files:** `services/kafka-consumer/consumer.py`, `processor.py`, `config.py`

Consumer group: `disaster-processor-group`. Processes every event through a 4-step pipeline:

1. **Validate** — required fields present, event_type known, severity_raw numeric
2. **Classify** — threshold lookup → `LOW | MEDIUM | HIGH | CRITICAL`
3. **Filter** — drop events below minimum severity (noise reduction)
4. **Enrich** — add `processed_at` timestamp, rename fields to internal format

Severity thresholds (single source of truth, reused by all downstream modules):

| Type | Min (drop below) | LOW | MEDIUM | HIGH | CRITICAL |
|---|---|---|---|---|---|
| Earthquake (Richter) | 1.5 | 1.5 | 3.0 | 5.0 | 7.0 |
| Fire (FRP MW) | 5 | 5 | 100 | 2 000 | 7 000 |
| Flood (cm) | 5 | 5 | 50 | 200 | 800 |
| Storm (km/h) | 35 | 35 | 62 | 120 | 200 |

Manual offset commits — offsets only advance after successful processing.

---

### Module 4 — MongoDB Writer

**Files:** `services/mongodb-writer/writer.py`, `db.py`, `config.py`

Consumer group: `disaster-storage-group` (independent from Module 3 — both get the full stream).

Key guarantees:
- **Idempotent upserts** — `event_id` is the MongoDB `_id`; re-runs never create duplicates
- **Offset discipline** — Kafka offsets committed only after MongoDB batch flush (no data loss on crash)
- **Batch size** — 50 events per bulk write for throughput
- **GeoJSON transform** — lat/lon stored as `{"type": "Point", "coordinates": [lon, lat]}` for 2dsphere queries

Indexes created on `disaster_events`:
1. `2dsphere` on `location` — geospatial bounding-box queries
2. `event_time DESC` — recency queries
3. Compound `(event_type, severity_level, event_time)` — filtered searches
4. `source` — provenance queries

---

### Module 5 — ETL Service

**Files:** `services/etl-service/etl_runner.py`, `extract.py`, `transform.py`, `load.py`

Batch ETL job: MongoDB → PostgreSQL star schema.

**Extract** (`extract.py`):
- Pulls flat documents from `disaster_db.disaster_events`
- Supports `--since DATE` for incremental loads
- Deduplicates against existing PostgreSQL `event_id` set before transform

**Transform** (`transform.py`):
- Geographic classification: lat/lon → (country, region) via a pure-Python grid-based nearest-city lookup
- Built from `reverse_geocoder`'s bundled `rg_cities1000.csv` (~144k cities in a 1°×1° grid)
- No subprocess spawning — avoids Windows paging-file exhaustion on large batches
- Falls back to hemisphere labels if lookup unavailable
- Country → region mapping via 200-entry ISO alpha-2 dictionaries (`_CC_TO_COUNTRY`, `_CC_TO_REGION`)
- Timestamp decomposition into `(date, day, month, year, hour)` star-schema columns

**Load** (`load.py`):
- Dimension get-or-create pattern: `INSERT ... ON CONFLICT DO NOTHING` + `SELECT` fallback
- Per-event savepoints so one bad record never aborts the batch
- Batch commits every **10,000 rows** — keeps PostgreSQL WAL manageable on memory-constrained machines
- Fact table upsert on `event_id` UNIQUE constraint — fully idempotent

Full load of 456,538 events: ~20 minutes.

---

### Module 6 — Query API

**Files:** `services/query-api/api.py`, `routers/`, `mongo_client.py`, `postgres_client.py`

FastAPI service. Auto-generated docs at `http://127.0.0.1:8000/docs`.

**Connection management:**
- MongoDB: single `MongoClient` instance shared across all workers (internally thread-safe)
- PostgreSQL: `ThreadedConnectionPool` (1–10 connections) — each FastAPI thread gets its own connection

**Map endpoint** (`routers/map.py`):
- `zoom ≤ 9` → clustered mode: SQL pre-aggregation into grid cells + H3 hexagonal re-clustering
- `zoom ≥ 10` → raw mode: up to 1,000 individual events in viewport
- Default date range: 1980-01-01 → today (full 45-year history)

H3 clustering details (`postgres_client.py`):

| Zoom | H3 resolution | Cell area | SQL pre-cell |
|---|---|---|---|
| 2–4 | 2 | ~86,000 km² | 5.0° |
| 5–6 | 3 | ~12,000 km² | 0.5° |
| 7 | 3 | ~12,000 km² | 0.1° |
| 8–9 | 4 | ~1,700 km² | 0.05° |

Falls back to square-grid clustering when `h3` package not installed.

**Search endpoint** (`routers/events.py`):
- Backed by PostgreSQL (not MongoDB) — country/region fields populated by ETL
- `ORDER BY f.time_id DESC` — uses `idx_fact_time_id` index, no dim_time sort join
- Supports: date range, multi-type (`?type=earthquake&type=fire`), country (ILIKE), region, proximity radius (km)

**Real-time endpoints:**

| Endpoint | Description |
|---|---|
| `GET /stream/events` | SSE stream; polls `disaster_events.processed_at` every 1s |
| `POST /stream/start` | Launches `event_streamer.py` subprocess (replay historical data) |
| `GET /live/snapshot` | Last 100 events from `live_events` TTL collection |
| `GET /live/stream` | SSE stream from `live_events` |
| `POST /live/start` | Launches `live_generator.py` subprocess (synthetic continuous feed) |

---

## Data Schemas

### Unified Kafka / MongoDB Schema

```json
{
  "event_id":       "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
  "event_type":     "earthquake | fire | flood | storm",
  "severity_level": "LOW | MEDIUM | HIGH | CRITICAL",
  "severity_raw":   6.2,
  "location": {
    "type": "Point",
    "coordinates": [139.691, 35.689]
  },
  "event_time":   "2024-03-15T14:22:00Z",
  "processed_at": "2024-03-15T14:22:03Z",
  "source":       "USGS | NASA-FIRMS | USGS-HWM | IBTrACS"
}
```

### API Event Response

```json
{
  "event_id":       "3f2504e0-4f89-11d3-9a0c-0305e82c3301",
  "event_type":     "earthquake",
  "severity_level": "HIGH",
  "latitude":       35.689,
  "longitude":      139.691,
  "event_time":     "2024-03-15T14:22:00Z",
  "processed_at":   "2024-03-15T14:22:03Z",
  "source":         "USGS"
}
```

---

## Database Design

### MongoDB — `disaster_db.disaster_events`

Operational store for individual event queries and real-time streaming.

| Field | Type | Notes |
|---|---|---|
| `_id` | string | = `event_id` (UUID) |
| `event_type` | string | earthquake / fire / flood / storm |
| `severity_level` | string | LOW / MEDIUM / HIGH / CRITICAL |
| `severity_raw` | float | raw numeric value pre-classification |
| `location` | GeoJSON Point | `{type, coordinates: [lon, lat]}` |
| `event_time` | Date | original event timestamp (UTC) |
| `processed_at` | Date | MongoDB insert time — drives SSE polling |
| `source` | string | originating data provider |

Indexes: `2dsphere(location)`, `event_time DESC`, `(event_type, severity_level, event_time)`, `source`

### MongoDB — `disaster_db.live_events`

TTL-windowed collection for the live map feed.

- TTL index on `event_time` — documents expire after **8 hours**
- Index on `processed_at` for SSE polling
- Populated by `live_generator.py` (synthetic continuous feed)
- API caps snapshot at **100 most recent events** to avoid flooding the map

### PostgreSQL — `disaster_dw` Star Schema

Analytical store for aggregations, trends, and the search/map endpoints.

```
dim_event_type          dim_location              dim_time
──────────────          ────────────────────      ────────────────
event_type_id  PK       location_id  PK           time_id   PK
event_type_name         latitude                  date
                        longitude                 day
                        country                   month
                        region                    year
                        UNIQUE(latitude,longitude)hour
                                                  UNIQUE(date,hour)

fact_disaster_events
─────────────────────────────────────────────────────────
fact_id          PK
event_id         UNIQUE  ← idempotent upserts
event_type_id    FK → dim_event_type
location_id      FK → dim_location
time_id          FK → dim_time
severity_level
source
count_metric     (always 1 — reserved for aggregation)
```

Indexes: `idx_fact_time_id` (for `ORDER BY f.time_id DESC`), `idx_dim_location_lat_lon`, `idx_dim_time_date`

---

## Prerequisites

| Dependency | Version | Purpose |
|---|---|---|
| Python | 3.9+ | Runtime |
| MongoDB | 8.x | Operational document store |
| PostgreSQL | 18 | Analytical data warehouse |
| Apache Kafka | Any | Event streaming (optional — simulate mode bypasses it) |

Install Python packages:
```bash
pip install pymongo psycopg2-binary kafka-python fastapi "uvicorn[standard]" pydantic h3 reverse_geocoder
```

---

## Running the Project

### Start PostgreSQL

```bash
"/c/Program Files/PostgreSQL/18/bin/pg_ctl.exe" start -D "/c/Program Files/PostgreSQL/18/data" -l "/c/Program Files/PostgreSQL/18/data/log/startup.log"
```

If it fails with a stale lock:
```bash
# Kill leftover postgres processes first
taskkill //F //IM postgres.exe 2>/dev/null
# Then start again
"/c/Program Files/PostgreSQL/18/bin/pg_ctl.exe" start -D "/c/Program Files/PostgreSQL/18/data" -l "/c/Program Files/PostgreSQL/18/data/log/startup.log"
```

PostgreSQL connection: `postgresql://postgres:postgres@localhost:5432/disaster_dw`

### Start MongoDB Sharded Cluster

```bash
bash mongo_cluster/start_cluster.sh
```

MongoDB connection: `mongodb://localhost:28020/` (mongos router)

### Load the Data Warehouse (ETL)

Only needed when the DW is empty or after a full reset:
```bash
cd services/etl-service
python etl_runner.py --simulate
```

Takes ~20 minutes for 456,538 events. To reload from scratch:
```python
# In Python / pgAdmin:
TRUNCATE fact_disaster_events, dim_location, dim_time, dim_event_type RESTART IDENTITY CASCADE;
```
Then re-run the ETL.

### Start the Query API

```bash
cd services/query-api
python -m uvicorn api:app --host 127.0.0.1 --port 8000
```

API docs: `http://127.0.0.1:8000/docs`

### Start the Real-Time Feed (optional)

```bash
# Replay historical events into MongoDB at 2s/event
curl -X POST "http://127.0.0.1:8000/stream/start?delay=2.0"

# OR: continuous synthetic live feed (one new event every 10s)
curl -X POST "http://127.0.0.1:8000/live/start?rate=10"
```

---

### Run Individual Modules

```bash
# Module 1 — Generate data
python main.py

# Module 2 — Kafka Producer (dry-run, no Kafka needed)
cd services/kafka-producer && python producer.py --dry-run

# Module 3 — Consumer (simulate, no Kafka needed)
cd services/kafka-consumer && python consumer.py --simulate

# Module 4 — MongoDB Writer (simulate + dry-run)
cd services/mongodb-writer && python writer.py --simulate --dry-run

# Module 4 — MongoDB Writer (simulate, writes to MongoDB)
cd services/mongodb-writer && python writer.py --simulate

# Module 5 — ETL (simulate + dry-run, no DBs needed)
cd services/etl-service && python etl_runner.py --simulate --dry-run

# Module 5 — ETL (simulate, writes to PostgreSQL)
cd services/etl-service && python etl_runner.py --simulate

# Module 5 — Incremental ETL (only events after a date)
cd services/etl-service && python etl_runner.py --since 2024-01-01
```

---

## API Reference

Base URL: `http://127.0.0.1:8000`

### Health & Docs

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | MongoDB + PostgreSQL connectivity status |
| GET | `/docs` | Interactive Swagger UI |

### Events (MongoDB)

| Method | Endpoint | Key params | Description |
|---|---|---|---|
| GET | `/events/recent` | `limit`, `event_type` | Latest N events |
| GET | `/events/search` | `type`, `country`, `region`, `start_date`, `end_date`, `lat`, `lng`, `proximity_km`, `limit`, `offset` | Filtered + paginated (PostgreSQL-backed) |
| GET | `/events/by-location` | `min_lat`, `max_lat`, `min_lon`, `max_lon`, `limit` | Geospatial bbox query |

### Analytics (PostgreSQL)

| Method | Endpoint | Key params | Description |
|---|---|---|---|
| GET | `/analytics/event-counts` | — | Total events per disaster type |
| GET | `/analytics/monthly-trends` | `event_type`, `year`, `start_date`, `end_date`, `country` | Monthly volume time series |
| GET | `/analytics/locations/top` | `limit`, `event_type` | Top regions by event count |

### Map (PostgreSQL + H3)

| Method | Endpoint | Key params | Description |
|---|---|---|---|
| GET | `/events/map` | `min_lat`, `max_lat`, `min_lon`, `max_lon`, `zoom`, `start_date`, `end_date` | Clustered (zoom ≤ 9) or raw (zoom ≥ 10) |

### Real-Time Stream (MongoDB SSE)

| Method | Endpoint | Key params | Description |
|---|---|---|---|
| GET | `/stream/events` | — | SSE stream of new events from `disaster_events` |
| POST | `/stream/start` | `delay`, `scale` | Start event_streamer.py replay |
| POST | `/stream/stop` | — | Stop replay |
| GET | `/stream/status` | — | Running state + PID |

### Live Feed (TTL-windowed SSE)

| Method | Endpoint | Key params | Description |
|---|---|---|---|
| GET | `/live/snapshot` | `limit` (default 100) | Recent events from `live_events` collection |
| GET | `/live/stream` | — | SSE stream from `live_events` |
| POST | `/live/start` | `rate` (seconds/event, default 10) | Start live_generator.py |
| POST | `/live/stop` | — | Stop generator |
| GET | `/live/status` | — | Running state + PID |

---

## Real-Time Integration

### SSE Stream

Connect with the browser's native `EventSource` API:

```js
const es = new EventSource("http://127.0.0.1:8000/stream/events");

es.onmessage = (e) => {
  const frame = JSON.parse(e.data);
  if (frame.type === "connected") console.log("Live since", frame.timestamp);
  if (frame.type === "event") {
    const ev = frame.data;
    // ev.event_id, ev.event_type, ev.severity_level
    // ev.latitude, ev.longitude, ev.event_time, ev.source
  }
};
```

The backend polls MongoDB every **1 second** for documents with `processed_at > last_seen` and pushes each one as a separate frame.

### Simulation Control

```bash
# Replay historical events at 2s/event
curl -X POST "http://127.0.0.1:8000/stream/start?delay=2.0"

# Fast demo — 100ms/event
curl -X POST "http://127.0.0.1:8000/stream/start?delay=0.1"

# Continuous synthetic generator — one new event every 10s
curl -X POST "http://127.0.0.1:8000/live/start?rate=10"

# Stop
curl -X POST "http://127.0.0.1:8000/stream/stop"
curl -X POST "http://127.0.0.1:8000/live/stop"
```

---

## Key Design Decisions

**Idempotent everywhere** — `event_id` as MongoDB `_id` and PostgreSQL UNIQUE constraint means every module can be re-run safely without creating duplicates.

**Independent consumer groups** — Module 3 (`disaster-processor-group`) and Module 4 (`disaster-storage-group`) both subscribe to all 4 Kafka topics independently. This means classification logic and storage logic can evolve separately, and a crash in one doesn't block the other.

**Offset-after-flush discipline** — Module 4 commits Kafka offsets only after the MongoDB batch write succeeds. If the process crashes mid-batch, events are re-consumed and re-upserted (idempotent), guaranteeing no data loss.

**Read path split (MongoDB vs PostgreSQL)** — MongoDB handles operational queries (individual events, geospatial lookups, real-time polling) where document flexibility and low latency matter. PostgreSQL handles all aggregations (monthly trends, country counts, map clustering) where the star schema and query planner shine.

**Batch commits in ETL** — Loading 456k rows in a single PostgreSQL transaction exceeds available virtual memory on Windows with a constrained paging file. Committing every 10,000 rows keeps WAL pressure bounded and makes the process resumable.

**Pure-Python geocoding** — `reverse_geocoder` uses scipy's KDTree internally, which spawns worker processes that fail on Windows when the paging file is too small. The ETL instead loads `rg_cities1000.csv` directly and builds a 1°×1° grid dict for O(1) nearest-city lookups — no subprocess spawning, no scipy dependency at query time.

**H3 hexagonal clustering** — At low zoom levels, the map pre-aggregates events into SQL grid cells then applies H3 hexagonal binning for visually consistent clusters. Weighted centroids ensure the displayed dot is the true centre of mass of events in each hexagon. Falls back to square-grid if the `h3` package isn't installed.

**Live feed TTL cap** — `live_events` documents expire after 8 hours via a MongoDB TTL index. The `/live/snapshot` endpoint additionally caps the response at 100 events to prevent flooding the map on initial load.

---

## MongoDB Sharded Cluster

Architecture: **3 shards × 3 replica-set members + 1 config server + 1 mongos router = 11 processes**

```
mongos router (app connects here)   :28020
config server  (configRS)           :28100
Shard 1 (rs1)                       :28017  :28018  :28019
Shard 2 (rs2)                       :28021  :28022  :28023
Shard 3 (rs3)                       :28024  :28025  :28026
```

Ports use `28xxx` to avoid conflict with the MongoDB Windows service on `:27017`.

Shard key: `{_id: "hashed"}` — distributes documents evenly across all 3 shards by UUID hash.

```bash
# One-time cluster initialisation
bash mongo_cluster/setup_cluster.sh

# Daily startup
bash mongo_cluster/start_cluster.sh

# Shutdown
bash mongo_cluster/stop_cluster.sh
```

> **RAM:** all 11 processes together use ~2–3 GB. Close other heavy applications before starting the cluster.
