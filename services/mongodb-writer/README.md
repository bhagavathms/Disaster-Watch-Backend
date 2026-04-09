# MongoDB Writer Service — Module 4

Part of the **Distributed NoSQL-Based Disaster Monitoring and Analytics System**

Persists processed disaster events into MongoDB with geospatial indexing,
idempotent upserts, and bulk-write batching.

---

## Architecture

```
Raw Kafka Topics                  mongodb-writer service
────────────────                  ──────────────────────
earthquakes-topic ──┐
fires-topic       ──┤──► [KafkaConsumer]          group: disaster-storage-group
floods-topic      ──┤         │
storms-topic      ──┘         ▼
                         [EventProcessor]          (imported from Module 3)
                              │
                         ProcessedEvent
                              │
                         [MongoDBClient]
                              │
                         bulk_write(upsert)
                              │
                         MongoDB: disaster_db.disaster_events
```

---

## MongoDB Storage Strategy

### Document Schema

```json
{
  "_id":            "f23e295d-c0dc-47f9-9bb3-54422dbc88b9",
  "event_type":     "earthquake",
  "severity_level": "HIGH",
  "location": {
    "type":        "Point",
    "coordinates": [119.820412, -6.251812]
  },
  "event_time":   ISODate("2024-10-03T18:52:09Z"),
  "processed_at": ISODate("2024-12-15T09:44:22Z"),
  "source":       "GeoNet"
}
```

**Key design decisions:**

| Choice | Reason |
|--------|--------|
| `event_id` → `_id` | MongoDB primary key = natural deduplication key. Re-sending the same event is a no-op upsert, not a duplicate. |
| GeoJSON Point `[lon, lat]` | Required format for MongoDB's `2dsphere` index. Enables `$geoNear`, `$geoWithin`, and `$near` queries. Note: GeoJSON order is longitude first. |
| Timestamps as BSON Date | Stored as `ISODate`, not strings. Enables native date arithmetic, `$gte`/`$lte` filters, and `$dateTrunc` aggregations. |
| `source` retained | Supports data provenance queries: "how many events came from USGS vs JMA?" |

---

## Why Sharding and Replication Are NOT in Application Code

The application code connects to a URI:

```python
MongoClient("mongodb://localhost:27017/")
```

Whether that URI points to a **standalone node**, a **replica set**, or a
**sharded cluster** is transparent to the application. The PyMongo driver
handles routing, failover, and write distribution automatically.

Putting cluster management in application code would violate the principle
of **separation of concerns** — infrastructure is an operational concern,
not a business logic concern.

### Recommended Sharding Configuration (admin setup, not in code)

```javascript
// Run once in mongosh after starting a sharded cluster
sh.enableSharding("disaster_db")

sh.shardCollection(
  "disaster_db.disaster_events",
  { "event_type": 1, "event_time": 1 }
)
```

**Why this shard key?**
- `event_type` (4 values) provides good routing cardinality
- `event_time` distributes load across shards over time
- Prevents the "hot shard" problem that would occur with timestamp-only sharding
- Allows efficient per-type, time-bounded queries to be routed to a single shard

### Connecting to a Replica Set

```bash
python writer.py --mongo-uri "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
```

No code changes needed — the driver handles failover and read preference automatically.

---

## Indexes Created on Startup

| Index | Type | Purpose |
|-------|------|---------|
| `{ location: "2dsphere" }` | Geospatial | `$geoNear`, `$geoWithin` radius queries |
| `{ event_time: -1 }` | Single-field | "Most recent N events" queries |
| `{ event_type, severity_level, event_time }` | Compound | "All HIGH earthquakes this week" |
| `{ source }` | Single-field | Data provenance filtering |

The `_id` field is indexed automatically by MongoDB.

---

## Installation

```bash
cd services/mongodb-writer
pip install -r requirements.txt
```

---

## Starting MongoDB Locally

### Option A — MongoDB Community Server (direct)

```bash
# Download from: https://www.mongodb.com/try/download/community
# Then start the server:

# Linux / macOS
mongod --dbpath /data/db

# Windows (run as Administrator)
mongod --dbpath "C:\data\db"
```

### Option B — MongoDB as a Windows Service

```bash
# After installation, MongoDB often runs as a service:
net start MongoDB

# Check it's running:
mongosh --eval "db.adminCommand('ping')"
```

---

## Running the Service

### Dry-run (zero dependencies — test document format)

```bash
cd services/mongodb-writer
python writer.py --simulate --dry-run
```

No Kafka, no MongoDB required. Shows exactly what would be stored.

### Simulate mode (JSON files → MongoDB, no Kafka)

```bash
python writer.py --simulate
```

Requires MongoDB running at `localhost:27017`.

### Live mode (Kafka → MongoDB)

```bash
# Terminal 1: Kafka + ZooKeeper running
# Terminal 2: start the Kafka producer
cd services/kafka-producer && python producer.py

# Terminal 3: start this service
cd services/mongodb-writer && python writer.py
```

### With a custom MongoDB URI

```bash
python writer.py --simulate --mongo-uri "mongodb://localhost:27017/"
```

### Re-process from beginning (Kafka offset reset)

```bash
python writer.py --from-beginning
```

---

## CLI Reference

| Flag | Default | Description |
|------|---------|-------------|
| `--simulate` | off | Read from local JSON files instead of Kafka |
| `--dry-run` | off | Print documents without writing to MongoDB |
| `--broker HOST:PORT` | localhost:9092 | Kafka bootstrap server |
| `--mongo-uri URI` | mongodb://localhost:27017/ | MongoDB connection URI |
| `--from-beginning` | off | Reset Kafka offset to earliest |
| `--batch-size N` | 50 | MongoDB bulk-write batch size |
| `--data-dir PATH` | auto-resolved | Override data/ directory path |

---

## Verifying Data in MongoDB

After running the service, connect with `mongosh` and run:

```javascript
use disaster_db

// Count all events
db.disaster_events.countDocuments()

// Events by type
db.disaster_events.aggregate([
  { $group: { _id: "$event_type", count: { $sum: 1 } } }
])

// CRITICAL events
db.disaster_events.find({ severity_level: "CRITICAL" }).limit(5).pretty()

// Geospatial: events within 500km of Tokyo [139.6917, 35.6892]
db.disaster_events.find({
  location: {
    $geoWithin: {
      $centerSphere: [[139.6917, 35.6892], 500 / 6378.1]
    }
  }
})
```

---

## What Comes Next (Module 5)

The Analytics / ETL module will:
- Query `disaster_db.disaster_events` using the indexes created here
- Aggregate by region, time window, and severity
- Perform periodic ETL into PostgreSQL for relational analytics
- Expose a REST API or dashboard over the stored data
