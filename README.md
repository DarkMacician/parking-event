# Parking Realtime Streaming

This project simulates a real-time parking lot system with three main components:

- Producer: simulates multiple cameras sending parking events to Kafka (topic `raw-data`).
- Spark Processor: processes streaming data with PySpark, builds parking sessions, calculates duration and cost, detects `CLOSED` / `TIMEOUT`, and writes to Kafka (topic `processed-data`).[^1][^2]
- API + WebSocket: Flask + Socket.IO reads `processed-data`, manages parking state, and exposes REST API + WebSocket for a frontend.[^3][^4]

***

## 1. Prerequisites

You need:

- Docker and Docker Compose
- Python 3 (for running producer and API)
- A Kafka container (started with your own `docker-compose` file)[^1]
- A Spark cluster with Docker (spark-master + spark-worker-1 + spark-worker-2)

Create a `config.json` file in the project root (same level as `task.py`, `producer.py`, `app.py`). Example:

```json
{
  "kafka_servers": "xxx.xxx.xxx.xxx:xxxx",
  "num_cameras": 1,
  "duration_minutes": 1,
  "kafka_topic": "x",
  "output_topic_sessions": "x",
  "block_price": 1,
  "block_seconds": 1
}
```

For submissions, you can replace real IPs with placeholders like `KAFKA_HOST:PORT`.[^5]

***

## 2. Start Kafka

In the folder that contains your Kafka `docker-compose` file (for example `kafka-compose.yaml`):

```bash
docker compose -f kafka-compose.yaml up -d
```

Kafka will listen on the IP/port configured in the compose file (for example `xxx.xxx.xxx.xxx:xxxx`).[^1]

And you need to create topics for streaming:

```bash
docker run --rm -it confluentinc/cp-kafka:latest kafka-topics --create --topic NAME --partitions 1 --replication-factor 1 --bootstrap-server xxx.xxx.xxx.xxx:xxxx
```

***

## 3. Run Spark Streaming (task.py)

### 3.1 Start the Spark cluster

In the `spark-docker` directory (where the Spark master/worker compose file is):

```bash
docker compose up -d
```


### 3.2 Install pandas and pyarrow inside Spark containers

Do this once for each container: `spark-master`, `spark-worker-1`, `spark-worker-2`:

```bash
docker exec -it spark-master bash
mkdir -p /tmp/pylibs
pip3 install --target /tmp/pylibs "pandas>=1.0.5"
pip3 install --target /tmp/pylibs "pyarrow>=4.0.0"
exit
```

Repeat the same for `spark-worker-1` and `spark-worker-2`.[^6]

### 3.3 Submit the Spark job (task.py)

Assuming `task.py` is mounted into `/opt/spark-apps`:

```bash
docker exec -it spark-master bash -c "PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --conf spark.executorEnv.PYTHONPATH=/tmp/pylibs --conf spark.driverEnv.PYTHONPATH=/tmp/pylibs --conf spark.driver.extraJavaOptions='-Divy.cache.dir=/tmp -Divy.home=/tmp' --conf spark.jars.ivy=/tmp --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.executor.memory=2g --conf spark.executor.cores=2 /opt/spark-apps/task.py"
```

The `task.py` job will:

- Read the Kafka topic `raw-data` using `kafka_input_servers` from `config.json`.[^7][^1]
- Use `applyInPandasWithState` grouped by `(license_plate, location)` to maintain sessions and timeouts.[^8][^9]
- Write results to the `processed-data` topic with fields:
    - `license_plate`, `location`, `start_time`, `end_time`, `duration`, `cost`, `status` (`ACTIVE`, `CLOSED`, `TIMEOUT`), `last_updated`.

***

## 4. Run the Producer (producer.py)

The producer simulates multiple cameras sending events to the `raw-data` topic.

### 4.1 Install dependencies

```bash
cd producer
pip install kafka-python
```


### 4.2 Run the producer

```bash
python producer.py
```

The producer will:

- Read configuration from `config.json`:
    - `producer_num_cameras`
    - `producer_duration_minutes`
    - `producer_topic`
    - `producer_bootstrap_servers`
- Create multiple camera simulators (threads) and send events:
    - `ENTERING`, `PARKED`, `MOVING`, `EXITING`
- Use `license_plate` as the Kafka message key so that all events for the same car go to the same partition.[^2][^6]

As the producer runs, the Spark job will start printing `ACTIVE`, `CLOSED`, and `TIMEOUT` sessions to the console and writing them into Kafka.

***

## 5. Run the API + WebSocket (app.py)

The API consumes from `processed-data` and exposes REST + WebSocket.

### 5.1 Install dependencies

```bash
cd api
pip install flask flask-cors flask-socketio kafka-python
```


### 5.2 Start the API

```bash
python api.py
```

Defaults:

- REST API base URL: `http://localhost:5000/api/`
- WebSocket namespace: `/parking`[^4][^3]

Main REST endpoints:

- `GET /api/status` – system status and summary statistics.
- `GET /api/locations/free` – list of currently free locations.
- `GET /api/locations/busy` – list of occupied locations.
- `GET /api/sessions/active` – list of active sessions.
- `GET /api/sessions/closed?limit=50` – history of closed sessions.
- `GET /api/location/<location_id>` – details of a specific parking location.
- `GET /api/statistics` – global statistics (total spots, busy/free count, active/closed sessions).

WebSocket events:

- `parking_update` – emitted whenever a new session event is processed.
- `exit_notification` – emitted when a car leaves (`CLOSED` or `TIMEOUT`).
- `statistics_update` – updated stats for dashboards.
- `initial_data` – initial snapshot when a client connects.
- `duration_update` – periodic real-time duration/cost updates.
- `location_info` – detailed info for one location on demand.[^10][^3]

***

## 6. Full run order

1. Start Kafka:
```bash
docker compose -f kafka-compose.yaml up -d
```

2. Start the Spark cluster:
```bash
docker compose up -d   # in the spark-docker directory
```

3. (First time only) Install `pandas` and `pyarrow` in `spark-master`, `spark-worker-1`, `spark-worker-2`.[^6]
4. Run the Spark job:
```bash
docker exec -it spark-master bash -c "... spark-submit ... /opt/spark-apps/task.py"
```

5. Run the producer:
```bash
python producer/producer.py
```

6. Run the API:
```bash
python api/app.py
```

Once all three parts are running:

- The producer continuously sends simulated camera events into Kafka.
- Spark Structured Streaming aggregates them into parking sessions in real time.[^11][^1]
- The API exposes the current parking lot state and history via REST and WebSocket for any frontend client.


