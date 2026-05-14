"""
parquet-es-bridge — watches a Parquet archive directory and bulk-indexes
new files into Elasticsearch.

Environment variables:
  PARQUET_DIR  — root directory to watch  (default: /data/parquet)
  ES_HOST      — Elasticsearch base URL   (default: http://elasticsearch:9200)
  ES_INDEX     — target index name        (default: weather-telemetry)
  POLL_INTERVAL — seconds between scans when watchdog misses an event (default: 30)
"""

import os
import time
import logging
import threading
from pathlib import Path

import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PARQUET_DIR   = os.getenv("PARQUET_DIR", "/data/parquet")
ES_HOST       = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX      = os.getenv("ES_INDEX", "weather-telemetry")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("parquet-es-bridge")

# ---------------------------------------------------------------------------
# Elasticsearch client
# ---------------------------------------------------------------------------

def connect_es(host: str, retries: int = 20, delay: int = 5) -> Elasticsearch:
    """Block until Elasticsearch is reachable."""
    es = Elasticsearch(host, request_timeout=10)
    for attempt in range(1, retries + 1):
        try:
            info = es.info()
            log.info("Connected to Elasticsearch %s", info["version"]["number"])
            return es
        except Exception as exc:
            log.warning("ES not ready (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Elasticsearch at {host} after {retries} attempts")


def ensure_index(es: Elasticsearch, index: str) -> None:
    """Create the index with a sensible mapping if it does not exist yet."""
    if es.indices.exists(index=index):
        return
    mapping = {
        "mappings": {
            "properties": {
                "station_id":       {"type": "long"},
                "s_no":             {"type": "long"},
                "battery_status":   {"type": "keyword"},
                "status_timestamp": {"type": "date", "format": "epoch_millis"},
                "humidity":         {"type": "integer"},
                "temperature":      {"type": "integer"},
                "wind_speed":       {"type": "integer"},
            }
        }
    }
    es.indices.create(index=index, body=mapping)
    log.info("Created Elasticsearch index '%s'", index)

# ---------------------------------------------------------------------------
# Parquet ingestion
# ---------------------------------------------------------------------------

# Thread-safe set of already-processed absolute file paths
_processed: set[str] = set()
_lock = threading.Lock()


def index_file(es: Elasticsearch, filepath: str) -> None:
    """Read a single Parquet file and bulk-index its rows into ES."""
    with _lock:
        if filepath in _processed:
            return
        # Mark early to prevent a racing second event from re-processing
        _processed.add(filepath)

    try:
        table = pq.read_table(filepath)
        df = table.to_pydict()  # column-oriented dict

        n_rows = len(next(iter(df.values()), []))
        if n_rows == 0:
            log.debug("Skipping empty file: %s", filepath)
            return

        def _actions():
            # Avro ReflectData stores Weather as a nested struct column named "weather".
            # pyarrow returns nested structs as Python dicts in the column list.
            weather_col = df.get("weather", [None] * n_rows)

            for i in range(n_rows):
                # Extract nested weather fields
                w = weather_col[i] if weather_col[i] is not None else {}
                humidity    = w.get("humidity")    if isinstance(w, dict) else None
                temperature = w.get("temperature") if isinstance(w, dict) else None
                wind_speed  = w.get("windSpeed")   if isinstance(w, dict) else None  # camelCase from Avro

                battery_raw = df.get("batteryStatus", [None] * n_rows)[i]
                # Enum may be stored as string or int ordinal; normalise to string
                battery_str = str(battery_raw).lower() if battery_raw is not None else None

                doc = {
                    "station_id":       df.get("stationId",        [None] * n_rows)[i],
                    "s_no":             df.get("sNo",               [None] * n_rows)[i],
                    "battery_status":   battery_str,
                    "status_timestamp": df.get("statusTimestamp",   [None] * n_rows)[i],
                    "humidity":         humidity,
                    "temperature":      temperature,
                    "wind_speed":       wind_speed,
                }
                # Drop None-valued keys to keep documents clean
                doc = {k: v for k, v in doc.items() if v is not None}
                yield {"_index": ES_INDEX, "_source": doc}

        success, errors = helpers.bulk(es, _actions(), raise_on_error=False)
        if errors:
            log.warning("Indexed %d docs from %s with %d errors", success, filepath, len(errors))
        else:
            log.info("Indexed %d docs from %s", success, filepath)

    except Exception as exc:
        log.error("Failed to index %s: %s", filepath, exc)
        # Remove from processed so we retry next scan
        with _lock:
            _processed.discard(filepath)


def scan_directory(es: Elasticsearch, root: str) -> None:
    """Walk root and index any .parquet file not yet processed."""
    for path in Path(root).rglob("*.parquet"):
        index_file(es, str(path))

# ---------------------------------------------------------------------------
# Watchdog handler
# ---------------------------------------------------------------------------

class ParquetHandler(FileSystemEventHandler):
    def __init__(self, es: Elasticsearch):
        self._es = es

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".parquet"):
            log.debug("New file detected: %s", event.src_path)
            # Small sleep so the writer finishes before we read
            time.sleep(2)
            index_file(self._es, event.src_path)

    def on_moved(self, event):
        # Some writers use atomic rename (tmp → final)
        if not event.is_directory and event.dest_path.endswith(".parquet"):
            log.debug("Moved/renamed file: %s", event.dest_path)
            time.sleep(2)
            index_file(self._es, event.dest_path)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info("Starting parquet-es-bridge | dir=%s es=%s index=%s", PARQUET_DIR, ES_HOST, ES_INDEX)

    # Wait for the parquet directory to exist (central-station may create it later)
    while not Path(PARQUET_DIR).exists():
        log.info("Waiting for parquet directory %s …", PARQUET_DIR)
        time.sleep(5)

    es = connect_es(ES_HOST)
    ensure_index(es, ES_INDEX)

    # Initial scan for files that arrived before we started
    log.info("Initial directory scan …")
    scan_directory(es, PARQUET_DIR)

    # Set up watchdog for real-time detection
    handler  = ParquetHandler(es)
    observer = Observer()
    observer.schedule(handler, PARQUET_DIR, recursive=True)
    observer.start()
    log.info("Watchdog observer started on %s", PARQUET_DIR)

    try:
        while True:
            # Periodic fallback scan in case watchdog misses inotify events
            time.sleep(POLL_INTERVAL)
            scan_directory(es, PARQUET_DIR)
    except KeyboardInterrupt:
        log.info("Shutting down …")
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()
