# Module 5 — ETL Service
## Distributed NoSQL-Based Disaster Monitoring and Analytics System

---

## 1. Architecture Overview

This module implements a **classic batch ETL pipeline** that bridges the NoSQL
operational store (MongoDB) and a relational analytical store (PostgreSQL).

```
[MongoDB]  disaster_db.disaster_events
      |
      |  db_mongo.py  +  extract.py
      |  Pull flat docs, dedup against PG event_ids
      v
[Python]   list[dict]  (raw extraction records)
      |
      |  transform.py
      |  Classify location, decompose timestamp,
      |  build TransformedEvent dataclass
      v
[Python]   list[TransformedEvent]
      |
      |  load.py  +  db_postgres.py
      |  Upsert dimension tables, insert fact rows
      v
[PostgreSQL]  disaster_dw
      ├── dim_event_type
      ├── dim_location
      ├── dim_time
      └── fact_disaster_events
```

---

## 2. Star Schema Design

### Why Star Schema?

| Goal | How star schema helps |
|------|-----------------------|
| Fast aggregation queries | Denormalised dimensions avoid joins |
| Slice-and-dice analysis | Each dim is an independent filter axis |
| BI tool compatibility | Standard pattern for Metabase, Tableau, etc. |
| Additive metrics | `count_metric` and `COUNT(fact_id)` compose cleanly |

### Tables

#### `dim_event_type`
| Column | Type | Notes |
|--------|------|-------|
| event_type_id | SERIAL PK | Surrogate key |
| event_type_name | VARCHAR(50) UNIQUE | earthquake, fire, flood, storm |

#### `dim_location`
| Column | Type | Notes |
|--------|------|-------|
| location_id | SERIAL PK | Surrogate key |
| latitude | DOUBLE PRECISION | Extracted from GeoJSON |
| longitude | DOUBLE PRECISION | Extracted from GeoJSON |
| country | VARCHAR(100) | Approximate, from bounding-box classifier |
| region | VARCHAR(100) | Sub-region label |
| — | UNIQUE(latitude, longitude) | One row per unique coordinate pair |

#### `dim_time`
| Column | Type | Notes |
|--------|------|-------|
| time_id | SERIAL PK | Surrogate key |
| date | DATE | Calendar date |
| day | SMALLINT | 1–31 |
| month | SMALLINT | 1–12 |
| year | SMALLINT | e.g. 2024 |
| hour | SMALLINT | 0–23 |
| — | UNIQUE(date, hour) | Events on same date+hour share a time_id |

#### `fact_disaster_events`
| Column | Type | Notes |
|--------|------|-------|
| fact_id | SERIAL PK | Surrogate key |
| event_id | VARCHAR(36) UNIQUE | Natural key from MongoDB (dedup gate) |
| event_type_id | INTEGER FK | → dim_event_type |
| location_id | INTEGER FK | → dim_location |
| time_id | INTEGER FK | → dim_time |
| severity_level | VARCHAR(10) | LOW / MEDIUM / HIGH / CRITICAL |
| source | VARCHAR(50) | Originating agency (USGS, NHC, …) |
| count_metric | SMALLINT DEFAULT 1 | Additive measure for SUM aggregations |

---

## 3. Document → Star Schema Example

### Source MongoDB document
```json
{
  "_id":            "047b5a02-f449-4b22-bcb4-683282dced0b",
  "event_type":     "earthquake",
  "severity_level": "CRITICAL",
  "location":       {"type": "Point", "coordinates": [-65.885635, -30.727968]},
  "event_time":     ISODate("2024-10-12T00:06:47Z"),
  "processed_at":   ISODate("2024-12-15T09:44:22Z"),
  "source":         "USGS"
}
```

### After extraction (flat dict)
```python
{
  "event_id":       "047b5a02-f449-4b22-bcb4-683282dced0b",
  "event_type":     "earthquake",
  "severity_level": "CRITICAL",
  "latitude":       -30.727968,
  "longitude":      -65.885635,
  "event_time":     datetime(2024, 10, 12, 0, 6, 47, tzinfo=UTC),
  "source":         "USGS"
}
```

### After transformation (TransformedEvent)
```
dim_event_type row  →  event_type_name = "earthquake"

dim_location row    →  latitude  = -30.727968
                        longitude = -65.885635
                        country   = "Chile"
                        region    = "South America"

dim_time row        →  date  = 2024-10-12
                        day   = 12, month = 10, year = 2024, hour = 0

fact row            →  event_id       = "047b5a02-..."
                        severity_level = "CRITICAL"
                        source         = "USGS"
                        count_metric   = 1
```

---

## 4. Deduplication

Two complementary layers prevent double-loading:

| Layer | Mechanism |
|-------|-----------|
| **Extract** | Queries `fact_disaster_events` for existing `event_id`s; matched records are filtered before transformation even starts |
| **Load (fact)** | `INSERT ... ON CONFLICT (event_id) DO NOTHING`; even if extraction misses a duplicate, the DB constraint catches it |
| **Load (dims)** | `INSERT ... ON CONFLICT DO NOTHING` + SELECT fallback; re-running never creates phantom dimension rows |

---

## 5. Local Setup

### Prerequisites
- Python 3.9+
- MongoDB running on `localhost:27017` with data from Module 4
- PostgreSQL running on `localhost:5432`

### Install dependencies
```bash
cd services/etl-service
pip install -r requirements.txt
```

### Create the PostgreSQL database (one-time)
```bash
createdb disaster_dw
# or via psql:
psql -U postgres -c "CREATE DATABASE disaster_dw;"
```

### Run ETL

```bash
# Zero-dependency check (no PostgreSQL write):
python etl_runner.py --dry-run

# Full ETL (requires MongoDB + PostgreSQL):
python etl_runner.py

# Incremental: only events on or after a date:
python etl_runner.py --since 2024-11-01

# Create tables only (no data movement):
python etl_runner.py --create-tables

# Custom connection strings:
python etl_runner.py \
  --mongo-uri mongodb://localhost:27017/ \
  --pg-dsn postgresql://postgres:postgres@localhost:5432/disaster_dw
```

---

## 6. Useful PostgreSQL Queries After Load

```sql
-- Total events per type
SELECT et.event_type_name, COUNT(*) AS total
FROM fact_disaster_events f
JOIN dim_event_type et ON f.event_type_id = et.event_type_id
GROUP BY et.event_type_name
ORDER BY total DESC;

-- CRITICAL events per country
SELECT l.country, COUNT(*) AS critical_count
FROM fact_disaster_events f
JOIN dim_location l ON f.location_id = l.location_id
WHERE f.severity_level = 'CRITICAL'
GROUP BY l.country
ORDER BY critical_count DESC;

-- Monthly event counts
SELECT t.year, t.month, COUNT(*) AS events
FROM fact_disaster_events f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.year, t.month
ORDER BY t.year, t.month;

-- Severity breakdown per event type
SELECT et.event_type_name, f.severity_level, COUNT(*) AS n
FROM fact_disaster_events f
JOIN dim_event_type et ON f.event_type_id = et.event_type_id
GROUP BY et.event_type_name, f.severity_level
ORDER BY et.event_type_name, f.severity_level;
```

---

## 7. File Responsibilities

| File | Role |
|------|------|
| `db_mongo.py` | MongoDB connection + document normalisation (read-only) |
| `db_postgres.py` | PostgreSQL connection + star-schema DDL |
| `extract.py` | Pull from MongoDB, apply dedup filter |
| `transform.py` | Location classification, timestamp decomposition, dataclass build |
| `load.py` | Dimension upserts, fact inserts, savepoint-based error isolation |
| `etl_runner.py` | CLI entry point, orchestrates all steps, prints banner + report |
