from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.streaming.state import GroupStateTimeout
import math
import uuid

import os

KAFKA_INPUT_SERVERS = os.getenv("KAFKA_INPUT_SERVERS")
KAFKA_OUTPUT_SERVERS = os.getenv("KAFKA_OUTPUT_SERVERS")
INPUT_TOPIC = os.getenv("INPUT_TOPIC")
OUTPUT_TOPIC_SESSIONS = os.getenv("OUTPUT_TOPIC_SESSIONS")

BLOCK_PRICE = int(os.getenv("BLOCK_PRICE"))
BLOCK_SECONDS = int(os.getenv("BLOCK_SECONDS"))
SESSION_TIMEOUT_SECONDS = int(os.getenv("SESSION_TIMEOUT_SECONDS"))

CHECKPOINT_DIR = f"/tmp/spark-checkpoint-{uuid.uuid4().hex[:8]}"

# ==================== SCHEMAS ====================
# Input event schema
event_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("timestamp_unix", LongType(), True),
    StructField("license_plate", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status_code", StringType(), True)
])

# State schema
state_schema = StructType([
    StructField("license_plate", StringType(), False),
    StructField("location", StringType(), False),
    StructField("start_time", LongType(), True),
    StructField("last_time", LongType(), True),
    StructField("status", StringType(), True)
])

# Output schema
output_schema = StructType([
    StructField("license_plate", StringType(), False),
    StructField("location", StringType(), False),
    StructField("start_time", LongType(), True),
    StructField("end_time", LongType(), True),
    StructField("duration", LongType(), True),
    StructField("cost", LongType(), True),
    StructField("status", StringType(), True),
    StructField("last_updated", LongType(), True)
])

# ==================== STATE FUNCTION ====================
def process_parking_batch(key, pdf_iter, state):
    """
    Stateful processing function
    key: (license_plate, location)
    pdf_iter: Iterator of pandas DataFrames with events
    state: GroupState object
    """
    import pandas as pd

    license_plate, location = key

    # Load current state
    if state.exists:
        st = state.get
        if st[2] is not None:  # start_time is not None
            current_state = {
                "plate": st[0],
                "location": st[1],
                "start_time": st[2],
                "last_time": st[3],
                "status": st[4]
            }
        else:
            current_state = None
            state.remove()
    else:
        current_state = None

    results = []

    # Handle timeout
    if state.hasTimedOut:
        if current_state is not None and current_state["status"] != "CLOSED":
            duration = current_state["last_time"] - current_state["start_time"]
            cost = int(math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE)

            results.append({
                "license_plate": license_plate,
                "location": location,
                "start_time": current_state["start_time"],
                "end_time": current_state["last_time"],
                "duration": duration,
                "cost": cost,
                "status": "TIMEOUT",
                "last_updated": current_state["last_time"]
            })

            state.remove()

            if results:
                yield pd.DataFrame(results)
            return

    # Process events
    for pdf in pdf_iter:
        for row in pdf.itertuples(index=False):
            ts_unix = row.timestamp_unix
            status_code = row.status_code

            # Initialize state for new session
            if current_state is None or current_state["status"] == "CLOSED":
                current_state = {
                    "plate": license_plate,
                    "location": location,
                    "start_time": ts_unix,
                    "last_time": ts_unix,
                    "status": "ACTIVE"
                }
            else:
                # Update existing session
                current_state["last_time"] = ts_unix

            # Handle EXITING
            if status_code == "EXITING":
                current_state["status"] = "CLOSED"
                duration = current_state["last_time"] - current_state["start_time"]
                cost = int(math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE)

                results.append({
                    "license_plate": license_plate,
                    "location": location,
                    "start_time": current_state["start_time"],
                    "end_time": current_state["last_time"],
                    "duration": duration,
                    "cost": cost,
                    "status": "CLOSED",
                    "last_updated": current_state["last_time"]
                })

                state.remove()
                break

    # Output active session (realtime update)
    if current_state and current_state["status"] == "ACTIVE":
        duration = current_state["last_time"] - current_state["start_time"]
        cost = int(math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE)

        results.append({
            "license_plate": license_plate,
            "location": location,
            "start_time": current_state["start_time"],
            "end_time": current_state["last_time"],
            "duration": duration,
            "cost": cost,
            "status": "ACTIVE",
            "last_updated": current_state["last_time"]
        })

        # Update state
        state.update((
            current_state["plate"],
            current_state["location"],
            current_state["start_time"],
            current_state["last_time"],
            current_state["status"]
        ))

        # Set timeout
        state.setTimeoutDuration(SESSION_TIMEOUT_SECONDS*1000)

    # Yield results
    if results:
        yield pd.DataFrame(results)
    else:
        yield pd.DataFrame(columns=[
            "license_plate", "location", "start_time", "end_time",
            "duration", "cost", "status", "last_updated"
        ])


# ==================== SPARK SESSION ====================
def create_spark_session():
    spark = SparkSession.builder \
        .appName("ParkingSessionProcessor-Stateful") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ==================== MAIN ====================
def main():
    print("""
    ╔═══════════════════════════════════════════════════════════╗
    ║     PYSPARK PARKING SESSION PROCESSOR                     ║
    ║     With applyInPandasWithState (Realtime Updates)        ║
    ╚═══════════════════════════════════════════════════════════╝
    """)

    spark = create_spark_session()

    print(f"✓ Spark Session created")
    print(f"✓ Kafka Bootstrap: {KAFKA_INPUT_SERVERS}")
    print(f"✓ Input Topic: {INPUT_TOPIC}")
    print(f"✓ Output Topic: {OUTPUT_TOPIC_SESSIONS}")
    print(f"✓ Session Timeout: {SESSION_TIMEOUT_SECONDS}s")
    print(f"✓ Checkpoint Dir: {CHECKPOINT_DIR}")
    print(f"✓ Mode: applyInPandasWithState for realtime updates\n")

    # Read from Kafka
    parking_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_INPUT_SERVERS) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse events
    parking_events = parking_raw.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

    # Apply stateful processing
    stateful_sessions = parking_events \
        .groupBy("license_plate", "location") \
        .applyInPandasWithState(
            func=process_parking_batch,
            outputStructType=output_schema,
            stateStructType=state_schema,
            outputMode="update",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout
        )

    # Console output
    console_query = stateful_sessions \
        .writeStream \
        .format("console") \
        .option("truncate", False) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    # Kafka output
    kafka_query = stateful_sessions \
        .selectExpr(
            "license_plate as key",
            "to_json(struct(*)) as value"
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_OUTPUT_SERVERS) \
        .option("topic", OUTPUT_TOPIC_SESSIONS) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("update") \
        .trigger(processingTime="2 seconds") \
        .start()

    print("🚀 Streaming started with applyInPandasWithState!")
    print("📊 Realtime updates every batch")
    print(f"⏰ Timeout: {SESSION_TIMEOUT_SECONDS}s\n")
    print("Status meanings:")
    print("  - ACTIVE: Xe đang đỗ (update liên tục)")
    print("  - CLOSED: Xe đã ra (EXITING event)")
    print("  - TIMEOUT: Session timeout (30s không có event)\n")
    print("Press Ctrl+C to stop...\n")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()