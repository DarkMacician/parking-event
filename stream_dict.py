import queue
import threading
from typing import Iterator, List, Optional
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType

PARKING_EVENT_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("timestamp_unix", LongType(), True),
    StructField("license_plate", StringType(), True),
    StructField("location", StringType(), True),
    StructField("status_code", StringType(), True),
])

def parse_values_df(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.from_json(F.col("value").cast("string"), PARKING_EVENT_SCHEMA).alias("data")
        )
        .select("data.*")
    )

def batch_to_dicts(batch_df: DataFrame) -> List[dict]:
    parsed = parse_values_df(batch_df)
    return [row.asDict(recursive=True) for row in parsed.collect()]

class DictStream:
    def __init__(self, max_queue_size: int = 100):
        self._q: "queue.Queue[List[dict]]" = queue.Queue(maxsize=max_queue_size)
        self._query = None
        self._stopped = threading.Event()
        self._error: Optional[BaseException] = None

    def _on_batch(self, batch_df: DataFrame, batch_id: int):
        try:
            dicts = batch_to_dicts(batch_df)
            self._q.put(dicts)
        except BaseException as e:
            self._error = e
            # dừng query ở thread driver
            if self._query is not None:
                self._query.stop()

    def start(self, value_df: DataFrame):
        self._query = (
            value_df.writeStream
            .foreachBatch(self._on_batch)
            .outputMode("append")
            .start()
        )
        return self

    def iter_dicts(self, timeout: Optional[float] = None) -> Iterator[List[dict]]:
        try:
            while True:
                if self._error:
                    raise self._error
                try:
                    items = self._q.get(timeout=timeout)
                    yield items
                except queue.Empty:
                    if self._query and self._query.isActive:
                        continue
                    break
        finally:
            self.stop()

    def stop(self):
        if self._query and self._query.isActive:
            self._query.stop()
        self._stopped.set()
