# Kafka Consumer Processor — Module 3

Part of the **Distributed NoSQL-Based Disaster Monitoring and Analytics System**

This microservice consumes raw disaster events from Kafka, validates, filters,
and enriches them, then logs the cleaned output ready for MongoDB storage (Module 4).

---

## Architecture

```
Kafka Topics                 kafka-consumer-processor         Module 4
─────────────                ────────────────────────         ────────
earthquakes-topic ──┐
fires-topic       ──┤──► [consumer.py]
floods-topic      ──┤         │
storms-topic      ──┘         ▼
                          [processor.py]
                              │
                    ┌─────────┴─────────┐
                    │  validate schema  │
                    │  filter low-sev   │
                    │  classify level   │
                    │  enrich fields    │
                    └─────────┬─────────┘
                              │
                    ProcessedEvent (logged)
                    ──────────────────────► MongoDB (Module 4)
```

---

## Processing Pipeline

Each Kafka message goes through four stages:

### 1. Schema Validation
Drops events that are:
- Missing any required field (`event_id`, `event_type`, `severity_raw`, etc.)
- Using an unknown `event_type`
- Carrying a non-numeric `severity_raw`

### 2. Severity Classification
Maps `severity_raw` to a standard level using **per-type thresholds**:

| Type       | Unit          | DROP  | LOW       | MEDIUM       | HIGH          | CRITICAL  |
|------------|---------------|-------|-----------|--------------|---------------|-----------|
| earthquake | Richter       | < 1.5 | 1.5–2.9   | 3.0–4.9      | 5.0–6.9       | ≥ 7.0     |
| fire       | intensity idx | < 5   | 5–99      | 100–1 999    | 2 000–6 999   | ≥ 7 000   |
| flood      | cm rise       | < 5   | 5–49      | 50–199       | 200–799       | ≥ 800     |
| storm      | km/h wind     | < 35  | 35–61     | 62–119       | 120–199       | ≥ 200     |

**Why different units?** The raw data uses intentionally inconsistent scales
(Module 1 design). The threshold table is the normalisation contract.

### 3. Filtering
Events with `severity_raw` below `min_threshold` are silently dropped.
This removes sensor nulls (0.0), micro-earthquakes, brush fires, and
tropical depressions that are below meaningful monitoring thresholds.

### 4. Schema Enrichment
Transforms the raw schema into the processed schema:

```
Raw (Kafka message)              Processed (output)
───────────────────              ──────────────────
event_id       ──────────────►  event_id
event_type     ──────────────►  event_type
severity_raw   ──── classify ►  severity_level  (LOW|MEDIUM|HIGH|CRITICAL)
               ──── dropped ──  (severity_raw removed)
latitude       ──────────────►  latitude
longitude      ──────────────►  longitude
timestamp      ── renamed ───►  event_time
               ── injected ──►  processed_at    (UTC time of this processing)
source         ──────────────►  source
```

---

## Consumer Group Strategy

```
group_id = "disaster-processor-group"
```

- All instances sharing this group_id form a Kafka consumer group
- Kafka distributes partitions between group members (horizontal scaling)
- Each partition is consumed by exactly one group member at a time
- **Manual offset commit**: offsets are committed after each message,
  not auto-committed — prevents losing an event if the service crashes mid-process
- **Offset reset**: `earliest` — on first run (no committed offset),
  start from the oldest available message

---

## Installation

```bash
cd services/kafka-consumer
pip install -r requirements.txt
```

---

## Running the Service

### Mode 1 — Simulate (no Kafka required)

Tests the processing logic against your local JSON files.
Use this to verify transformations before running real Kafka.

```bash
python consumer.py --simulate
```

### Mode 2 — Live Kafka stream

Requires Kafka to be running and the producer to be publishing messages.
See `services/kafka-producer/README.md` for Kafka setup.

```bash
# Terminal 1: start Kafka (see kafka-producer README)
# Terminal 2: start the producer
cd services/kafka-producer && python producer.py

# Terminal 3: start this consumer
cd services/kafka-consumer && python consumer.py
```

---

## CLI Reference

| Flag              | Default         | Description                                              |
|-------------------|-----------------|----------------------------------------------------------|
| `--simulate`      | off             | Process local JSON files, no Kafka connection needed     |
| `--broker`        | localhost:9092  | Kafka bootstrap server address                           |
| `--from-beginning`| off             | Reset group offset to earliest (re-process all messages) |
| `--log-every N`   | 10              | Log a progress line every N processed events             |
| `--data-dir PATH` | auto-resolved   | Override data/ path (simulate mode only)                 |

---

## Example Input vs Output

### Input (raw Kafka message)

```json
{
  "event_id":    "f23e295d-c0dc-47f9-9bb3-54422dbc88b9",
  "event_type":  "earthquake",
  "severity_raw": 6.6,
  "latitude":    -6.251812,
  "longitude":   119.820412,
  "timestamp":   "2024-10-03T18:52:09Z",
  "source":      "GeoNet"
}
```

### Output (ProcessedEvent)

```json
{
  "event_id":       "f23e295d-c0dc-47f9-9bb3-54422dbc88b9",
  "event_type":     "earthquake",
  "severity_level": "HIGH",
  "latitude":       -6.251812,
  "longitude":      119.820412,
  "event_time":     "2024-10-03T18:52:09Z",
  "processed_at":   "2024-12-15T09:44:22Z",
  "source":         "GeoNet"
}
```

**Changes:**
- `severity_raw: 6.6` → `severity_level: "HIGH"` (Richter 5.0–6.9 = HIGH)
- `timestamp` → renamed to `event_time`
- `processed_at` added (UTC time this service ran the event through the pipeline)
- `severity_raw` removed (not needed downstream)

### Dropped event example

```json
{
  "event_id":    "a36ec509-...",
  "event_type":  "storm",
  "severity_raw": 0.0,
  "...": "..."
}
```

→ **Dropped**: `severity_raw=0.0 < min_threshold=35.0` for storm (sensor null)

---

## Log Files

All processed events are written to:

```
logs/kafka_consumer.log
```

- **INFO level**: progress summary every `--log-every` events
- **WARNING level**: every CRITICAL severity event (always logged)
- **DEBUG level**: full JSON of every processed event (file only, not console)

---

## What Comes Next (Module 4)

The MongoDB Storage Service will:
- Import `EventProcessor` from this module's `processor.py`
- Call `processor.process(raw)` in its own consumption loop
- Write `result.to_dict()` to the appropriate MongoDB collection
- Use `event_id` as the MongoDB `_id` (natural deduplication key)
