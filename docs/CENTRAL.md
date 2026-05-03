# Central Station — Architecture & Design

## Overview

The Central Station is a Spring Boot service that acts as the **data aggregation hub** of the
distributed weather observability platform. It consumes raw telemetry from Kafka, stores the
latest reading per station in a custom key-value engine (Bitcask), detects rain conditions via
a Kafka Streams pipeline, and archives historical readings to Parquet files.

---

## System Context

```
Weather Stations (x10)
        │  1 msg/sec each
        ▼
  [Kafka Topic: weather.telemetry.v1]
        │
        ├──► RainDetectionProcessor (Kafka Streams)
        │         │ humidity > 70%
        │         ▼
        │    [Kafka Topic: weather.alerts.rain.v1]
        │
        └──► WeatherStreamConsumer (Kafka Consumer)
                  │
                  ├──► BitcaskEngine (latest status per station)
                  │         │
                  │         └── /data/bitcask  (Docker volume)
                  │
                  └──► ParquetArchiver (batch archiving, 10k records)
                            │
                            └── /data/parquet  (Docker volume)
```

---

## Package Structure

```
src/main/java/org/example/centralstation/
│
├── CentralStationApplication.java     Main entry point. Enables scheduling for compaction.
│
├── config/                            Spring configuration classes
│
├── model/
│   ├── WeatherStatus.java             Immutable record (stationId, sNo, batteryStatus, timestamp, weather)
│   ├── Weather.java                   Nested record (humidity, temperature, windSpeed)
│   └── BatteryStatus.java             Enum: low | medium | high
│
├── kafka/
│   └── WeatherStreamConsumer.java     @KafkaListener — routes each message to BitcaskEngine
│
├── processor/
│   └── RainDetectionProcessor.java    Kafka Streams DSL — filters humidity > 70%, publishes rain alerts
│
├── bitcask/                           Custom LSM-style key-value store
│   ├── BitcaskEngine.java             Public API: put(WeatherStatus), get(stationId), getAll()
│   ├── KeyDir.java                    In-memory ConcurrentHashMap: stationId → RecordPointer
│   ├── RecordPointer.java             Immutable record: fileId, valuePosition, valueSize, timestamp
│   ├── SegmentManager.java            All file I/O: append, read, rotate, hint files, recovery
│   ├── BitcaskSerializer.java         WeatherStatus ↔ JSON bytes (Jackson)
│   └── CompactionJob.java             @Scheduled background compaction — merges immutable segments
│
├── parquet/
│   ├── ParquetArchiver.java           Buffers incoming records; flushes when batch hits 10,000
│   └── ParquetBatchWriter.java        Writes a Parquet file to the partitioned output directory
│
└── api/
    └── ClientRequestHandler.java      REST endpoints: GET /view-all, GET /view-key/{stationId}
```

---

## Bitcask Storage Engine

### Core Idea

Bitcask is an append-only key-value store. Every write goes to the **end** of a flat binary file.
An in-memory hash table (`KeyDir`) maps each key to its latest location on disk, so reads are
always a single seek + read — O(1) regardless of how many historical versions exist on disk.

### On-Disk Binary Format

**Data file record** (`.data`):

```
┌──────────────┬──────────┬───────────┬────────────┬──────────────┐
│ timestamp 8B │ keyLen 4B│ valueLen 4B│ key (UTF-8)│ value (JSON) │
└──────────────┴──────────┴───────────┴────────────┴──────────────┘
```

**Hint file record** (`.hint`) — a lightweight index used for fast startup:

```
┌──────────────┬──────────┬──────────────┬──────────────────┬────────────┐
│ timestamp 8B │ keyLen 4B│ valueSize 8B │ valuePosition 8B │ key (UTF-8)│
└──────────────┴──────────┴──────────────┴──────────────────┴────────────┘
```

### Segment File Naming

Files are named by their creation epoch-millisecond timestamp:

```
/data/bitcask/
  1714600000000.data   ← sealed (immutable)
  1714600000000.hint   ← index for the sealed segment above
  1714600012345.data   ← active (append-only, being written to)
```

> The file with **no matching `.hint`** is always the active segment.

### Write Flow

```
WeatherStreamConsumer.consumeWeatherStatus(status)
        │
        ▼
BitcaskEngine.put(status)
        │
        ├─ 1. BitcaskSerializer.serialize(status)  →  byte[]
        ├─ 2. SegmentManager.append(key, bytes)    →  RecordPointer
        │         └─ acquires writeLock
        │         └─ writes binary record to active FileChannel
        │         └─ rotates segment if size ≥ 8 MB
        └─ 3. KeyDir.put(stationId, pointer)
```

### Read Flow

```
BitcaskEngine.get(stationId)
        │
        ├─ 1. KeyDir.get(stationId)          →  RecordPointer (or null)
        ├─ 2. SegmentManager.read(pointer)   →  byte[]  (positional FileChannel read)
        └─ 3. BitcaskSerializer.deserialize(bytes)  →  WeatherStatus
```

### Segment Rotation

When the active `.data` file reaches **8 MB**, `SegmentManager` automatically:

1. Flushes and closes the active `FileChannel`
2. Writes a `.hint` file for the sealed segment (scanning `KeyDir` for entries pointing to it)
3. Opens a new `.data` file (named with the current epoch-millis)

### Recovery on Startup

`SegmentManager.init()` runs at Spring startup (`@PostConstruct`):

```
1. List all .data files, sorted chronologically by filename
2. For each file that HAS a .hint file → load hint file into KeyDir  (fast)
3. For the file that has NO .hint file  → scan it record-by-record  (active segment)
```

This ensures the `KeyDir` is fully rebuilt before any Kafka messages are processed.

### Compaction

`CompactionJob` is a `@Scheduled` background task (default: every **5 minutes**).

**Algorithm:**

```
1. Get list of all immutable segment IDs (have a .hint file, not the active segment)
2. Snapshot KeyDir — collect entries whose pointer lives in an immutable segment
3. Read the raw bytes for each such key from disk
4. Write all entries into a single new compacted .data file
5. Write a .hint file for the compacted segment
6. Update KeyDir: replace old pointer with new one only if it still points to the old segment
   (guards against a concurrent fresh write arriving during compaction)
7. Delete the old .data and .hint files
```

**Why it's safe:**
- Only immutable segments are touched — the active segment is never read or written by the compaction job
- Reads proceed unblocked during compaction; they use the KeyDir which is updated atomically per-key at the end
- The `replaceIfSameFile()` guard prevents overwriting a newer pointer that arrived mid-compaction

### Thread Safety Summary

| Operation | Mechanism |
|---|---|
| Writes (`put`) | `ReentrantLock` in `SegmentManager.append()` |
| Reads (`get`, `getAll`) | Lock-free positional `FileChannel.read(buf, pos)` |
| KeyDir updates | `ConcurrentHashMap` (lock-free reads, atomic per-key puts) |
| Compaction KeyDir swap | `replaceIfSameFile()` — conditional put |

---

## Kafka Streams — Rain Detection

`RainDetectionProcessor` uses the Kafka Streams DSL to run a stateless filter pipeline:

```
[weather.telemetry.v1]
    │  filter: humidity > 70%
    │  mapValues: format alert string
    ▼
[weather.alerts.rain.v1]
```

This runs as a separate consumer group from the `WeatherStreamConsumer`, so both pipelines receive every message independently.

---

## Configuration Reference

All values can be overridden via environment variables (Docker Compose / Kubernetes).

| Property | Env Var | Default |
|---|---|---|
| `spring.kafka.bootstrap-servers` | `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `app.topic.input` | `APP_TOPIC_INPUT` | `weather.telemetry.v1` |
| `app.topic.alerts` | `APP_TOPIC_ALERTS` | `weather.alerts.rain.v1` |
| `bitcask.data-path` | `BITCASK_DATA_PATH` | `/data/bitcask` |
| `bitcask.compaction.interval-ms` | `BITCASK_COMPACTION_INTERVAL_MS` | `300000` (5 min) |

---

## Docker Volume Mounts

```yaml
central-station:
  volumes:
    - bitcask_data:/data/bitcask
    - parquet_data:/data/parquet
```

Data survives container restarts. On restart, `SegmentManager` rebuilds `KeyDir` from `.hint` files automatically.