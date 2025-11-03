from typing import Literal
from pyspark.sql import DataFrame, SparkSession

def build_value_df(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    *,
    starting_offsets: Literal["latest", "earliest"] = "latest",
    subscribe_type: Literal["subscribe", "subscribePattern"] = "subscribe",
) -> DataFrame:

    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("startingOffsets", starting_offsets)
    )
    if subscribe_type == "subscribe":
        reader = reader.option("subscribe", topic)
    else:
        reader = reader.option("subscribePattern", topic)

    df = reader.load()
    only_value = df.selectExpr("CAST(value AS STRING) AS value")
    return only_value