from pyspark.sql import SparkSession
from kafka_data import build_value_df
from stream_dict import parse_values_df
from processor import ParkingStateManager, enrich_session_with_realtime_duration

all_locations = {
    f"{row}{col}"
    for row in "ABCDEF"
    for col in range(1, 11)   # Từ 1 đến 10
}


def main():
    spark = (
        SparkSession.builder
        .appName("ParkingManagementSystem")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3")
        .getOrCreate()
    )
    value_df = build_value_df(
        spark, "192.168.1.117:9092", "test-topic"
    )
    events_df = parse_values_df(value_df)
    manager = ParkingStateManager(all_locations=all_locations)  # Tùy danh sách chỗ cố định

    def handle_batch(batch_df, batch_id):
        for row in batch_df.collect():
            manager.process_event(row.asDict())

        print("----ACTIVE SESSIONS:")
        # KHÔNG in trực tiếp get_active_sessions, mà enrich realtime!
        for sess in manager.get_active_sessions():
            s = enrich_session_with_realtime_duration(sess)
            print(s)

        print("----BUSY LOCATIONS:", manager.get_busy_locations())
        print("----FREE LOCATIONS:", manager.get_free_locations())
        print("----CLOSED SESSIONS:")
        print("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
        for sess in manager.get_closed_sessions():
            print(sess)

    # Listen event stream và cập nhật theo batch
    query = events_df.writeStream.foreachBatch(handle_batch).outputMode("append").start()
    query.awaitTermination()

if __name__ == "__main__":
    main()
