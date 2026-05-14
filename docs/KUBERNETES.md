# Weather Observability Platform — Kubernetes Guide

A complete reference for operating the distributed weather observability platform on Kubernetes (Minikube). Covers architecture, deployment, accessing data, debugging, and day-to-day usage.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Folder Structure](#folder-structure)
3. [Deploying the System](#deploying-the-system)
4. [Accessing Services from Your Browser](#accessing-services-from-your-browser)
5. [Working with Kafka UI](#working-with-kafka-ui)
6. [Accessing Central Station Data](#accessing-central-station-data)
   - [Viewing Parquet Files](#viewing-parquet-files)
   - [Reading the Bitcask Store](#reading-the-bitcask-store)
7. [Useful kubectl Commands](#useful-kubectl-commands)
8. [How Data Flows](#how-data-flows)
9. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌──────────────────────────────────── Kubernetes namespace: weather ────────────────────────────────────┐
│                                                                                                       │
│  ┌──────────────────── Producers ─────────────────────┐                                              │
│  │                                                     │                                              │
│  │  weather-station-1   (MODE=random)                  │                                              │
│  │  weather-station-2   (MODE=random)                  │                                              │
│  │  ...                                 ───────────────┼──► kafka:9092 (ClusterIP)                   │
│  │  weather-station-10  (MODE=random)                  │         │                                    │
│  │  weather-station-11  (MODE=open-meteo, real API)    │         │                                    │
│  │                                                     │         │                                    │
│  └─────────────────────────────────────────────────────┘         │                                    │
│                                                                   │                                    │
│  ┌──────────────────── Consumer ──────────────────────┐           │                                    │
│  │                                                     │◄──────────                                    │
│  │  central-station (Spring Boot + Kafka Streams)      │                                              │
│  │    │                                                │                                              │
│  │    ├── writes to /data/bitcask  (station status)    │                                              │
│  │    └── writes to /data/parquet  (time-series data)  │                                              │
│  │                       │                             │                                              │
│  └───────────────────────┼─────────────────────────────┘                                              │
│                          │ PersistentVolumeClaim (5Gi)                                               │
│                                                                                                       │
│  ┌──────────────────── Debug & Analytics ────────────┐                                              │
│  │  kafka-ui  (Provectus)  — browse topics, messages  │                                              │
│  │  elasticsearch & kibana — search and dashboards    │                                              │
│  └─────────────────────────────────────────────────────┘                                              │
│                                                                                                       │
└──────────────────────────┬────────────────────────────┬───────────────────────────────────────────────┘
                           │ NodePort                   │ NodePort
                    central-station                   kafka-ui
                   (your machine)                  (your machine)
```

### Components at a glance

| Component | Kind | Replicas | Image | Purpose |
|---|---|---|---|---|
| `kafka` | Deployment | 1 | `apache/kafka:latest` | Message broker (KRaft, no ZooKeeper) |
| `weather-station-1..5` | Deployment × 5 | 1 each | `weather-station-mock:latest` | Synthetic random data producers |
| `weather-station-6..10` | Deployment × 5 | 1 each | `weather-station-mock:latest` | Real data from Open-Meteo API |
| `central-station` | Deployment | 1 | `central-station:latest` | Kafka consumer + data storage |
| `parquet-es-bridge` | Deployment | 1 | `parquet-es-bridge:latest` | Syncs Parquet to Elasticsearch |
| `elasticsearch` | Deployment | 1 | `elasticsearch:9.4.0` | Analytics data store |
| `kibana` | Deployment | 1 | `kibana:9.4.0` | Analytics UI |
| `kafka-ui` | Deployment | 1 | `provectuslabs/kafka-ui:latest` | Web UI to browse Kafka |

### Services

| Service | Type | Port | Who uses it |
|---|---|---|---|
| `kafka` | ClusterIP | 9092 | All stations + central-station (internal only) |
| `central-station` | NodePort | 8080 | You, from your browser/curl |
| `kafka-ui` | NodePort | 8080 | You, from your browser |

---

## Folder Structure

```
k8s/
├── 00-namespace.yaml            # Creates the 'weather' namespace
├── deploy.sh                    # One-shot build + deploy script
│
├── kafka/
│   ├── configmap.yaml           # KRaft broker config (env vars)
│   ├── deployment.yaml          # Kafka pod
│   ├── service.yaml             # ClusterIP — internal DNS: kafka.weather.svc.cluster.local:9092
│   └── pvc.yaml                 # 2Gi volume for Kafka log segments
│
├── kafka-ui/
│   └── deployment.yaml          # Kafka UI pod + NodePort service (combined)
│
├── weather-stations/
│   ├── configmap.yaml           # Shared: KAFKA_BROKER, TOPIC_NAME, MODE=random
│   └── deployments.yaml         # Stations 1–5 (random mode)
│
├── weather-stations-api/
│   ├── configmap.yaml           # MODE=open-meteo config
│   └── deployments.yaml         # Stations 6–10 (Open-Meteo API mode)
│
└── central-station/
    ├── configmap.yaml           # Kafka + data path config
    ├── deployment.yaml          # Spring Boot consumer pod
    ├── service.yaml             # NodePort — your entry point
    └── pvc.yaml                 # 2Gi volume for Bitcask
    
├── parquet-es-bridge/
│   └── deployment.yaml          # Python service + 5Gi shared parquet-pvc
│
├── elasticsearch/ & kibana/     # Analytics stack manifests
```

---

## Deploying the System

### First time (or after a clean wipe)

```bash
# From the project root
./k8s/deploy.sh
```

This script:
1. Points Docker at Minikube's internal daemon (`eval $(minikube docker-env)`)
2. Builds `weather-station-mock:latest` and `central-station:latest` inside Minikube
3. Applies manifests in dependency order: namespace → kafka → central-station → stations → kafka-ui
4. Waits up to 300s for Kafka to be `Ready` before continuing

### Tear everything down

```bash
./k8s/deploy.sh --delete
```

### Applying a single change (without full redeploy)

```bash
# After editing a manifest, just re-apply that folder
kubectl apply -f k8s/central-station/
kubectl apply -f k8s/parquet-es-bridge/

# After rebuilding an image
eval $(minikube docker-env)
docker build -t central-station:latest ./central-station
docker build -t parquet-es-bridge:latest ./parquet-es-bridge
kubectl rollout restart deployment/central-station -n weather
```

---

## Accessing Services from Your Browser

Minikube does not assign real external IPs. Use the `minikube service` command to get a tunnelled URL:

```bash
# Central Station HTTP API
minikube service central-station -n weather --url
# → e.g. http://192.168.49.2:31451

# Kibana
minikube service kibana -n weather --url
# → e.g. http://192.168.49.2:32080

# Kafka UI
minikube service kafka-ui -n weather --url
# → e.g. http://192.168.49.2:32087
```

> **Tip:** Run these in a separate terminal — on some systems `minikube service` opens a tunnel that must stay alive.

You can also get the NodePort without opening a tunnel:
```bash
kubectl get svc -n weather
# Look at the PORT(S) column, e.g. 8080:31451/TCP
# Then use: http://$(minikube ip):31451
```

---

## Working with Kafka UI

Once you have the URL from `minikube service kafka-ui -n weather --url`, open it in your browser.

### What you can do

| Tab | What it shows |
|---|---|
| **Topics** | All topics (`weather.telemetry.v1`, `weather.alerts.rain.v1`). See partition count, message count, replication. |
| **Topics → Messages** | Browse individual messages. Filter by offset, timestamp, or key. See the raw JSON weather payloads. |
| **Consumers** | Active consumer groups (your `central-station` consumer). See lag — how far behind it is. |
| **Brokers** | Health of the Kafka broker, controller info, KRaft metadata. |

### What to look for

```
Topic: weather.telemetry.v1
  └── Partition 0: messages arriving ~every 1s per station (11 stations = ~11 msg/s)

Consumer Group: <your central-station group-id>
  └── Lag = 0  ✅ (central-station is keeping up)
  └── Lag > 0  ⚠️ (central-station is falling behind — check its logs)
```

---

## Accessing Central Station Data

The central station writes two types of data to the `/data` directory inside its pod, backed by a **PersistentVolumeClaim**. Data survives pod restarts because it's on the PVC.

### Shell into the container

```bash
kubectl exec -n weather deploy/central-station -it -- sh
```

Once inside, the data is under:
```
/data/
├── bitcask/    ← key-value store (station status, latest reading per station)
└── parquet/    ← columnar time-series data (all historical readings)
```

---

### Viewing Parquet Files

Parquet is a columnar binary format — you can't `cat` it. Use one of these approaches:

#### Option 1 — From inside the pod using Python (if available)

```bash
# Check if python is available in the container
kubectl exec -n weather deploy/central-station -- python3 --version

# If yes, run an inline inspection
kubectl exec -n weather deploy/central-station -- python3 -c "
import pandas as pd, glob, os
files = glob.glob('/data/parquet/**/*.parquet', recursive=True)
print('Files found:', files)
if files:
    df = pd.read_parquet(files[0])
    print(df.head(10))
    print('Schema:', df.dtypes)
"
```

#### Option 2 — Copy files out to your machine and inspect locally

```bash
# Copy the entire parquet directory to your machine
kubectl cp weather/$(kubectl get pod -n weather -l app=central-station -o jsonpath='{.items[0].metadata.name}'):/data/parquet ./parquet-data

# Then locally with Python
python3 -c "
import pandas as pd
df = pd.read_parquet('./parquet-data')
print(df.head(20))
print(df.dtypes)
"
```

#### Option 3 — DuckDB (easiest, no Python needed)

```bash
# Install duckdb CLI if you don't have it
# https://duckdb.org/docs/installation

kubectl cp weather/$(kubectl get pod -n weather -l app=central-station -o jsonpath='{.items[0].metadata.name}'):/data/parquet ./parquet-data

duckdb -c "SELECT * FROM read_parquet('./parquet-data/**/*.parquet') LIMIT 20;"
```

#### Option 4 — Port-forward and use the central-station API (if it exposes endpoints)

```bash
# If central-station has HTTP endpoints for data access:
curl http://$(minikube ip):$(kubectl get svc central-station -n weather -o jsonpath='{.spec.ports[0].nodePort}')/api/stations
```

---

### Reading the Bitcask Store

Bitcask is a custom append-only key-value store. The files in `/data/bitcask` are binary segment files.

#### List the raw files

```bash
kubectl exec -n weather deploy/central-station -- ls -lh /data/bitcask/
```

You'll see files like:
```
bitcask.data.0       ← append-only data segment
bitcask.hint.0       ← index (key → offset) for fast recovery
bitcask.data.1       ← next segment after compaction
```

#### Read via the central-station's built-in script (if provided)

```bash
# If the project ships a bitcask_client.sh:
kubectl cp bitcask_client.sh weather/$(kubectl get pod -n weather -l app=central-station -o jsonpath='{.items[0].metadata.name}'):/tmp/
kubectl exec -n weather deploy/central-station -- sh /tmp/bitcask_client.sh
```

#### Read via the HTTP API (simplest)

The central-station exposes its Bitcask data through HTTP. Get the NodePort URL first:

```bash
BASE_URL=$(minikube service central-station -n weather --url 2>/dev/null | head -1)
# or manually: BASE_URL=http://$(minikube ip):<nodeport>

# Get status of all stations (reads from Bitcask)
curl $BASE_URL/api/stations

# Get status of a specific station
curl $BASE_URL/api/stations/1
```

---

## Useful kubectl Commands

### Cluster overview

```bash
# All pods in the weather namespace
kubectl get pods -n weather

# All resources (pods, services, deployments, PVCs)
kubectl get all -n weather

# PersistentVolumeClaims — check bound status
kubectl get pvc -n weather
```

### Logs

```bash
# Central station (Kafka consumer output, processing, alerts)
kubectl logs -n weather deploy/central-station -f

# Specific weather station
kubectl logs -n weather deploy/weather-station-3 -f

# Kafka broker
kubectl logs -n weather deploy/kafka -f

# Last 200 lines without streaming
kubectl logs -n weather deploy/central-station --tail=200
```

### Debugging a stuck or crashing pod

```bash
# Describe shows Events (scheduling failures, probe failures, image errors)
kubectl describe pod -n weather -l app=central-station

# Get previous container's logs (if the pod restarted)
kubectl logs -n weather deploy/central-station --previous

# Check resource usage
kubectl top pods -n weather   # requires metrics-server
```

### Exec into a pod

```bash
# Shell into central-station (inspect /data, run curl, etc.)
kubectl exec -n weather deploy/central-station -it -- sh

# Shell into a weather station
kubectl exec -n weather deploy/weather-station-1 -it -- sh

# Run a one-off command without interactive shell
kubectl exec -n weather deploy/central-station -- ls -lh /data/parquet/
```

### Restarting a deployment

```bash
# Rolling restart (zero downtime — pulls fresh config/image)
kubectl rollout restart deployment/central-station -n weather
kubectl rollout restart deployment/weather-station-1 -n weather

# Check rollout status
kubectl rollout status deployment/central-station -n weather
```

### Scaling (e.g., temporarily stop all stations)

```bash
# Scale down all weather stations to 0 (stop producing data)
for i in $(seq 1 11); do
  kubectl scale deployment/weather-station-$i -n weather --replicas=0
done

# Scale back up
for i in $(seq 1 11); do
  kubectl scale deployment/weather-station-$i -n weather --replicas=1
done
```

---

## How Data Flows

```
weather-station-N
  │
  │  every ~1 second, publishes JSON to:
  │  topic: weather.telemetry.v1
  │  key:   station_id
  │  value: { station_id, s_no, battery_status, weather: { humidity, temperature, wind_speed } }
  │
  ▼
kafka (broker)
  │  topic: weather.telemetry.v1   ← all raw readings
  │  topic: weather.alerts.rain.v1 ← filtered alerts (high rain/humidity)
  │
  ▼
central-station (Kafka Streams consumer)
  │
  ├── Bitcask write  → /data/bitcask/
  │     key:   "station:<id>"
  │     value: latest reading (JSON)  ← overwrites on each message
  │     purpose: fast O(1) lookup of "what is station X doing right now?"
  │
  └── Parquet write  → /data/parquet/
        partitioned by date/hour
        purpose: historical time-series (queryable with DuckDB, Pandas, Spark)
```

---

## Troubleshooting

### Pod stuck in `Pending`
```bash
kubectl describe pod -n weather <pod-name>
# Look at Events — usually one of:
# - PVC not bound yet (wait a few seconds, Minikube provisions slowly)
# - Insufficient resources (try: minikube start --memory=4096 --cpus=4)
```

### `ImagePullBackOff` on weather-station or central-station
You need to build inside Minikube's Docker daemon:
```bash
eval $(minikube docker-env)
docker build -t weather-station-mock:latest ./weather-station-mock
docker build -t central-station:latest ./central-station
kubectl rollout restart deployment -n weather -l app=weather-station
kubectl rollout restart deployment/central-station -n weather
```

### Central-station keeps restarting (CrashLoopBackOff)
```bash
# Check what error it's hitting
kubectl logs -n weather deploy/central-station --previous

# Common causes:
# 1. Kafka not ready yet — stations retry, central-station may not. Wait and restart:
kubectl rollout restart deployment/central-station -n weather

# 2. PVC mount failure — check PVC is Bound:
kubectl get pvc -n weather
```

### Kafka UI shows no topics or "connection refused"
Kafka takes ~20-30s to fully start. Refresh the UI after 30s. If it still fails:
```bash
kubectl logs -n weather deploy/kafka --tail=20
# Should end with: "Kafka Server started"
```

### Weather stations can't connect to Kafka
The stations retry automatically. Check that the Kafka service DNS resolves correctly:
```bash
kubectl exec -n weather deploy/weather-station-1 -- nslookup kafka.weather.svc.cluster.local
# Should return: Address: 10.x.x.x (the ClusterIP)
```

### Minikube runs out of memory or pods crash with OOMKilled
The analytics stack (Elasticsearch) is very memory heavy. You must allocate enough resources:
```bash
# Stop and restart with 10 CPUs and 8GB RAM
minikube stop
minikube start --cpus=10 --memory=8192
```
