from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.streaming.state import GroupStateTimeout  # ← SỬA IMPORT PATH
import math

# ==========================================================
# CONFIGURATION
# ==========================================================
KAFKA_INPUT_SERVERS = "192.168.1.117:9092"
KAFKA_OUTPUT_SERVERS = "192.168.1.117:9092"
INPUT_TOPIC = "raw-data"
OUTPUT_TOPIC_SESSIONS = "processed_data"

CHECKPOINT_BASE = "parking-stateful-checkpoint"

BLOCK_PRICE = 10000
BLOCK_SECONDS = 600

# ==========================================================
# Spark Session
# ==========================================================
spark = (
    SparkSession.builder
    .appName("ParkingStatefulProcessing")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ==========================================================
# Schema
# ==========================================================
input_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("timestamp_unix", IntegerType(), True),
    StructField("license_plate", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status_code", StringType(), True)
])

# Kafka Input
parking_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_INPUT_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parking_events = (
    parking_raw
    .selectExpr("CAST(value AS STRING) as json_value")
    .select(from_json(col("json_value"), input_schema).alias("data"))
    .select("data.*")
)

# ==========================================================
# STATE SCHEMA
# ==========================================================
state_schema = StructType([
    StructField("license_plate", StringType(), False),
    StructField("location", StringType(), True),
    StructField("start_time", IntegerType(), True),
    StructField("last_time", IntegerType(), True),
    StructField("status", StringType(), True)
])

output_schema = StructType([
    StructField("license_plate", StringType(), False),
    StructField("location", StringType(), False),
    StructField("start_time", IntegerType(), False),
    StructField("end_time", IntegerType(), False),
    StructField("duration", IntegerType(), False),
    StructField("cost", IntegerType(), False),
    StructField("status", StringType(), False),
    StructField("last_updated", IntegerType(), False)
])


# ==========================================================
# Stateful Function for Spark 3.4
# ==========================================================
def process_parking_batch(key, pdf_iter, state):
    import pandas as pd

    license_plate = key[0]

    # Load current state
    if state.exists:
        st = state.get
        current_state = {
            "plate": st["license_plate"][0],
            "location": st["location"][0],
            "start_time": st["start_time"][0],
            "last_time": st["last_time"][0],
            "status": st["status"][0]
        }
    else:
        current_state = None

    results = []

    # CRITICAL: Must iterate through ALL batches in the iterator
    for events_pdf in pdf_iter:
        events_pdf = events_pdf.sort_values("timestamp_unix")

        for idx, row in events_pdf.iterrows():
            status_code = row["status_code"]
            location = row["location"]
            ts_unix = row["timestamp_unix"]

            # PARKED
            if status_code == "PARKED":
                if current_state is None or current_state["status"] == "CLOSED":
                    current_state = {
                        "plate": license_plate,
                        "location": location,
                        "start_time": ts_unix,
                        "last_time": ts_unix,
                        "status": "ACTIVE"
                    }
                else:
                    current_state["last_time"] = ts_unix

                duration = ts_unix - current_state["start_time"]
                cost = math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE

                results.append({
                    "license_plate": license_plate,
                    "location": location,
                    "start_time": current_state["start_time"],
                    "end_time": ts_unix,
                    "duration": duration,
                    "cost": cost,
                    "status": "ACTIVE",
                    "last_updated": ts_unix
                })

            # EXITING
            elif status_code == "EXITING" and current_state is not None:
                if current_state["status"] == "ACTIVE":
                    duration = ts_unix - current_state["start_time"]
                    cost = math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE

                    results.append({
                        "license_plate": license_plate,
                        "location": current_state["location"],
                        "start_time": current_state["start_time"],
                        "end_time": ts_unix,
                        "duration": duration,
                        "cost": cost,
                        "status": "CLOSED",
                        "last_updated": ts_unix
                    })

                    current_state["status"] = "CLOSED"

    # Set timeout BEFORE updating state (required for ProcessingTimeTimeout)
    state.setTimeoutDuration(3600000)  # 1 hour

    # Update or remove state
    if current_state is not None and current_state["status"] == "ACTIVE":
        state.update(pd.DataFrame([{
            "license_plate": current_state["plate"],
            "location": current_state["location"],
            "start_time": current_state["start_time"],
            "last_time": current_state["last_time"],
            "status": current_state["status"]
        }]))
    elif current_state is not None and current_state["status"] == "CLOSED":
        state.remove()

    # CRITICAL: Must YIELD, not RETURN
    if results:
        yield pd.DataFrame(results)
    else:
        yield pd.DataFrame(columns=[
            "license_plate", "location", "start_time", "end_time",
            "duration", "cost", "status", "last_updated"
        ])


# ==========================================================
# APPLY STATEFUL
# ==========================================================
stateful_sessions = (
    parking_events
    .groupBy("license_plate")
    .applyInPandasWithState(
        func=process_parking_batch,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="update",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
    )
)

# ==========================================================
# Write to Kafka
# ==========================================================
sessions_to_kafka = (
    stateful_sessions
    .select(
        col("license_plate").alias("key"),
        to_json(struct("*")).alias("value")
    )
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_OUTPUT_SERVERS)
    .option("topic", OUTPUT_TOPIC_SESSIONS)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/sessions")
    .outputMode("update")
    .start()
)

# Console Output
console_query = (
    stateful_sessions
    .writeStream
    .format("console")
    .option("truncate", False)
    .outputMode("update")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/console")
    .start()
)

spark.streams.awaitAnyTermination()