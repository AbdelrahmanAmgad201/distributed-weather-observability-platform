# 📄 Weather Stations Monitoring System

## 🧠 Project Overview

This project is a **distributed IoT data-intensive system** that simulates a network of weather stations producing continuous telemetry data. The system is designed to demonstrate **stream processing, data ingestion, storage, and analytics pipelines** similar to real-world large-scale data systems.

The architecture is inspired by modern **event-driven systems** and focuses on handling **high-frequency data streams efficiently**.

---

## 🏗️ System Architecture

The system is composed of three main stages:

### 1. 📡 Data Acquisition Layer

A set of distributed **weather station services** act as data producers.

* Each station generates a weather status message every second
* Some stations simulate real data using external APIs (Open-Meteo)
* Data is published to **Apache Kafka topics**

Key characteristics:

* Randomized battery status distribution:

  * 30% low
  * 40% medium
  * 30% high
* 10% message drop rate (simulating unreliable IoT networks)

---

### 2. ⚙️ Data Processing & Archiving (Central Station)

A single **central processing service** consumes data from Kafka and performs multiple responsibilities:

#### a. Stream Processing

* Consumes weather events from Kafka
* Detects weather conditions (e.g., rain detection if humidity > 70%)
* Produces alert events to a separate Kafka topic

#### b. Key-Value Store (Bitcask-style LSM)

* Maintains latest weather status per station
* Implements:

  * Hint files for recovery
  * Compaction of segment files
  * High-performance key-value access

#### c. Data Archiving

* Stores **all weather events in Parquet files**
* Data is partitioned by:

  * Station ID
  * Time
* Writes are batched (e.g., 10,000 records per batch) for efficiency

---

### 3. 📊 Indexing & Analytics Layer

This layer enables querying and visualization:

#### a. Elasticsearch Indexing

* Weather data is indexed into **Elasticsearch**
* Enables fast querying and aggregation

#### b. Kibana Visualization

Used for analytics dashboards such as:

* Battery status distribution per station
* Message drop rate validation
* Weather trends (humidity, temperature, wind)

---

## 🧩 Core Components

### 🌦️ Weather Station Services

* Simulated IoT devices
* Produce structured JSON messages:

```json
{
  "station_id": 1,
  "s_no": 1,
  "battery_status": "low",
  "status_timestamp": 1681521224,
  "weather": {
    "humidity": 35,
    "temperature": 100,
    "wind_speed": 13
  }
}
```

* Implement Kafka producers using Java API

---

### 📬 Kafka Layer (Event Bus)

* Central message broker for all system communication
* Topics:

  * `weather.telemetry.v1` (main stream)
  * `weather.alerts.rain.v1` (alerts)

---

### 🧠 Central Station Responsibilities

* Kafka consumer + processor
* Bitcask LSM key-value store
* Parquet archiver
* Optional integration with Open-Meteo API
* Optional Kafka processor for stream rules

---

### 💾 Storage Systems

#### Bitcask (LSM Key-Value Store)

* Stores latest state per station
* Optimized for fast reads/writes
* Includes:

  * Hint files for recovery
  * Compaction mechanism

#### Parquet Storage

* Stores full historical dataset
* Partitioned by:

  * station_id
  * time
* Used for batch analytics and offline processing

---

### 📈 Analytics Layer

#### Elasticsearch + Kibana

Used for:

* Querying historical data
* Aggregations and dashboards
* Validating simulation constraints:

  * battery distribution correctness
  * message drop rates

---

## 🚀 Deployment Model

The system is deployed using **Docker (and Kubernetes)**:

Services include:

* 10 Weather Stations
* Kafka 
* Central Station
* Elasticsearch + Kibana
* Kafka UI

---
