# Kafka Producer Service — Module 2

Part of the **Distributed NoSQL-Based Disaster Monitoring and Analytics System**

This microservice reads synthetic disaster event datasets (Module 1 output) and
streams them to Apache Kafka topics, simulating a real-time event pipeline.

---

## What This Service Does

```
data/earthquakes.json ──┐
data/fires.json        ──┤  [producer.py]  ──►  earthquakes-topic
data/floods.json       ──┤                 ──►  fires-topic
data/storms.json       ──┘                 ──►  floods-topic
                                           ──►  storms-topic
```

1. Loads all four JSON datasets from `data/`
2. Merges them into one time-ordered stream (sorted by `timestamp`)
3. Validates every event against the unified schema before publishing
4. Publishes each event to its dedicated Kafka topic as a JSON message
5. Sleeps between messages to simulate real-time arrival

---

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Python      | >= 3.9  |
| Apache Kafka | >= 2.8 |
| Java (for Kafka) | >= 11 |

---

## Installation

```bash
# From the service directory
cd services/kafka-producer

pip install -r requirements.txt
```

---

## Starting Kafka Locally

### Option A — Kafka binary (Windows / Linux / macOS)

**Step 1: Download Kafka**

```bash
# Download from https://kafka.apache.org/downloads
# Extract to a folder, e.g. C:/kafka or ~/kafka

# The commands below assume Kafka is on your PATH.
# On Windows (Git Bash / MINGW64), use the .sh scripts.
# On Windows CMD/PowerShell, use the .bat scripts under bin\windows\
```

**Step 2: Start ZooKeeper** (required by Kafka 2.x / 3.x KRaft-disabled)

```bash
# Terminal 1
zookeeper-server-start.sh config/zookeeper.properties
```

**Step 3: Start Kafka Broker**

```bash
# Terminal 2
kafka-server-start.sh config/server.properties
```

**Step 4: Create the four disaster topics**

```bash
# Terminal 3 — run once, safe to re-run
kafka-topics.sh --create --topic earthquakes-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

kafka-topics.sh --create --topic fires-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

kafka-topics.sh --create --topic floods-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

kafka-topics.sh --create --topic storms-topic \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

Or let the producer create topics automatically (see `--create-topics` below).

**Verify topics exist:**

```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## Running the Producer

```bash
cd services/kafka-producer
```

### Default mode (NORMAL speed, 0.5s delay)

```bash
python producer.py
```

### Fast mode (stress test, 0.1s delay)

```bash
python producer.py --speed FAST
```

### Slow mode (educational / debugging, 2.0s delay)

```bash
python producer.py --speed SLOW
```

### Dry run — no Kafka needed

Validates data and prints the exact message payloads that would be sent.
Useful for testing Module 1 output before Kafka is set up.

```bash
python producer.py --dry-run
```

### Auto-create topics then stream

```bash
python producer.py --create-topics
```

### Custom broker address

```bash
python producer.py --broker localhost:9093
```

### Custom data directory

```bash
python producer.py --data-dir /absolute/path/to/data
```

### Full example

```bash
python producer.py --speed FAST --create-topics --broker localhost:9092
```

---

## CLI Reference

| Flag             | Default           | Description                                    |
|------------------|-------------------|------------------------------------------------|
| `--speed`        | `NORMAL`          | `FAST` (0.1s) / `NORMAL` (0.5s) / `SLOW` (2s) |
| `--broker`       | `localhost:9092`  | Kafka bootstrap server address                 |
| `--data-dir`     | auto-resolved     | Override path to `data/` directory             |
| `--dry-run`      | off               | Print messages without connecting to Kafka     |
| `--create-topics`| off               | Create topics on broker before streaming       |

---

## Topic Design

| Event Type   | Kafka Topic        | Message Key  |
|--------------|--------------------|--------------|
| `earthquake` | `earthquakes-topic`| `event_id`   |
| `fire`       | `fires-topic`      | `event_id`   |
| `flood`      | `floods-topic`     | `event_id`   |
| `storm`      | `storms-topic`     | `event_id`   |

**Why one topic per event type?**

- Consumers (Module 3) can subscribe to specific disaster types independently
- Allows per-type throughput scaling (more partitions for high-volume types)
- Simplifies the consumer-side schema (all messages in a topic share the same structure)
- Aligns with the MongoDB collection-per-type design in Module 4

**Why `event_id` as the message key?**

Kafka routes messages with the same key to the same partition.
Using `event_id` ensures that if a duplicate event arrives (e.g., during a replay),
it lands on the same partition — making deduplication easier for the consumer.

---

## Example Kafka Message

This is the exact JSON payload written to the Kafka topic value:

```json
{
  "event_id": "3f4a1b2c-8e5d-4a9f-b123-7c6d9e0f1a2b",
  "event_type": "earthquake",
  "severity_raw": 6.4,
  "latitude": 35.6892,
  "longitude": 139.6917,
  "timestamp": "2024-10-22T14:33:07Z",
  "source": "JMA"
}
```

Kafka message envelope:

```
Topic   : earthquakes-topic
Key     : 3f4a1b2c-8e5d-4a9f-b123-7c6d9e0f1a2b   (UTF-8 string)
Value   : { ...JSON above... }                     (UTF-8 JSON bytes)
Headers : (none — added in later modules)
```

---

## Verifying Messages Reach Kafka

Use the built-in console consumer to spot-check messages:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic earthquakes-topic \
  --from-beginning \
  --max-messages 5
```

---

## Streaming Order

Events are merged across all four disaster types and sorted by `timestamp`
before publishing. This means the Kafka stream reflects the chronological
order in which events occurred — not the order they appear in the JSON files.

Example sequence:

```
[  1 / 424   0.2%]  type=flood         sev=102.34    ts=2024-10-02T00:04:11Z
[  2 / 424   0.5%]  type=earthquake    sev=3.20      ts=2024-10-02T00:08:45Z
[  3 / 424   0.7%]  type=storm         sev=87.50     ts=2024-10-02T00:11:22Z
...
```

---

## What Comes Next (Module 3)

The Kafka Consumer Service will:
- Subscribe to these four topics
- Deserialize the JSON payload
- Apply transformations (severity normalization, region tagging)
- Forward enriched events to MongoDB (Module 4)


All 5 modules ran successfully. The pipeline works end-to-end with a single command.

Usage:

Command	What it does
python run_pipeline.py	Full pipeline + start API (needs MongoDB + PostgreSQL)
python run_pipeline.py --dry-run	Full pipeline, no external services needed
python run_pipeline.py --skip-api	Run Modules 1-5, don't start API
python run_pipeline.py --api-only	Skip pipeline, just start the API server