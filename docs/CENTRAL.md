# Central Station Architecture
## Responsibilities
- Kafka Consumer
- Kafka processor (if humidity is > 70% produce to a new kafka topic)
- BitCask (Key value store with compaction and implementation of hint files)
- Parquet file generation (in batches of 10k)


## Packages and Classes
```
src/main/java/com/ddia/central/
│
├── CentralStationApp.java            // Main entry point. Wires dependencies and starts threads.
│
├── config/
│   └── AppConfig.java                // Loads environment variables (Kafka brokers, storage paths).
│
├── model/
│   └── WeatherStatus.java            // The immutable Java Record we created earlier.
│
├── kafka/
│   └── WeatherStreamConsumer.java    // The Kafka poll loop. Routes data to Bitcask and Parquet.
│
├── bitcask/                          // The custom LSM Engine
│   ├── BitcaskEngine.java            // The main public interface (put, get, getAll).
│   ├── KeyDir.java                   // The in-memory HashMap (Key -> FileId, ValueSize, ValuePos, Timestamp).
│   ├── SegmentManager.java           // Manages the active append-only file and older read-only segments.
│   ├── BitcaskSerializer.java        // Encodes/decodes the byte array (Timestamp, Key Size, Value Size, Key, Value).
│   └── CompactionTask.java           // Background thread for merging files and creating hint files.
│
├── parquet/                          // The batch archiving subsystem
│   ├── ParquetArchiver.java          // Receives single statuses and holds them in a memory buffer.
│   └── ParquetBatchWriter.java       // Triggers when buffer hits 10K and writes to partitioned files.
│
└── api/                              // For the Bitcask bash client
    └── ClientRequestHandler.java     // A lightweight HTTP server exposing /view-all and /view-key endpoints.
```