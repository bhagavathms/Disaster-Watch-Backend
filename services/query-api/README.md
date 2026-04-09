# Module 6 — Query API Service

Read-only FastAPI service exposing disaster event data from the MongoDB operational
store and the PostgreSQL analytics data warehouse.

---

## Directory Structure

```
services/query-api/
├── api.py                  # FastAPI app, lifespan, health endpoint
├── config.py               # Settings dataclasses (env-var overrides)
├── dependencies.py         # FastAPI dependency injection helpers
├── mongo_client.py         # MongoReadClient (get_recent, search, by_location)
├── postgres_client.py      # PostgresReadClient (event_counts, trends, locations)
├── requirements.txt
├── models/
│   ├── __init__.py
│   └── schemas.py          # Pydantic response models
└── routers/
    ├── __init__.py
    ├── events.py           # GET /events/* (MongoDB)
    └── analytics.py        # GET /analytics/* (PostgreSQL)
```

---

## Prerequisites

| Service    | Default connection                                      |
|------------|---------------------------------------------------------|
| MongoDB    | `mongodb://localhost:27017/` — db: `disaster_db`        |
| PostgreSQL | `postgresql://postgres:postgres@localhost:5432/disaster_dw` |

Both services must be populated by earlier pipeline modules before this API
returns real data.

---

## Installation

```bash
cd services/query-api
pip install -r requirements.txt
```

---

## Running the API

```bash
# Default (127.0.0.1:8000, auto-reload)
python api.py

# Or directly with uvicorn
uvicorn api:app --host 127.0.0.1 --port 8000 --reload
```

The interactive docs are available at **http://127.0.0.1:8000/docs**.

---

## Environment Variables

| Variable       | Default                                                    | Description                  |
|----------------|------------------------------------------------------------|------------------------------|
| `MONGO_URI`    | `mongodb://localhost:27017/`                               | MongoDB connection URI        |
| `MONGO_DB`     | `disaster_db`                                              | MongoDB database name         |
| `MONGO_COL`    | `disaster_events`                                          | MongoDB collection name       |
| `POSTGRES_DSN` | `postgresql://postgres:postgres@localhost:5432/disaster_dw`| PostgreSQL connection string  |
| `API_HOST`     | `127.0.0.1`                                                | Bind address                  |
| `API_PORT`     | `8000`                                                     | Bind port                     |

---

## Endpoints

### Events (MongoDB)

| Method | Path                    | Description                              |
|--------|-------------------------|------------------------------------------|
| GET    | `/events/recent`        | Latest N events (default 20, max 100)    |
| GET    | `/events/search`        | Filtered + paginated events              |
| GET    | `/events/by-location`   | Events within a geographic bounding box  |

**`GET /events/recent`**
```
?limit=20          # 1–100
&event_type=flood  # earthquake | fire | flood | storm
```

**`GET /events/search`**
```
?event_type=earthquake
&severity_level=HIGH    # LOW | MEDIUM | HIGH | CRITICAL
&date_from=2024-10-01T00:00:00Z
&date_to=2024-12-31T23:59:59Z
&page=1
&page_size=20           # 1–100
```

**`GET /events/by-location`**
```
?min_lat=30&max_lat=46&min_lon=129&max_lon=146   # Japan bounding box
&limit=100    # 1–500
```

---

### Analytics (PostgreSQL)

| Method | Path                        | Description                              |
|--------|-----------------------------|------------------------------------------|
| GET    | `/analytics/event-counts`   | Total events grouped by disaster type    |
| GET    | `/analytics/monthly-trends` | Monthly event volume with optional filter|
| GET    | `/analytics/locations/top`  | Top regions ranked by event count        |

**`GET /analytics/event-counts`**
```
# No parameters — returns all types
```

**`GET /analytics/monthly-trends`**
```
?event_type=storm
&year=2024
```

**`GET /analytics/locations/top`**
```
?limit=10          # 1–100
&event_type=flood
```

---

### Health

| Method | Path      | Description                                   |
|--------|-----------|-----------------------------------------------|
| GET    | `/health` | MongoDB + PostgreSQL connectivity status      |

```json
{
  "status": "ok",
  "mongodb":    { "status": "ok" },
  "postgresql": { "status": "ok" },
  "version": "1.0.0"
}
```

---

## Architecture Notes

- **MongoDB** is used for `/events/*` endpoints because they query *individual*
  processed events with variable predicates (type, severity, date range, location).
  MongoDB's document model and 2dsphere geospatial index are well-suited here.

- **PostgreSQL** is used for `/analytics/*` endpoints because they run GROUP BY
  aggregations over the full dataset.  The star schema written by Module 5 is
  purpose-built for these OLAP-style queries.

- DB clients are created **once** at startup (FastAPI lifespan) and stored on
  `app.state`.  Dependency injection (`dependencies.py`) retrieves them per
  request — no connection per-request overhead.

- FastAPI runs synchronous route handlers in a thread pool.  PostgreSQL access
  uses `psycopg2.pool.ThreadedConnectionPool` to safely serve concurrent requests.
