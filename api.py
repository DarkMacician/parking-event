from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import threading
import time
import queue
import json
from pyspark.sql import SparkSession
from kafka_data import build_value_df
from stream_dict import parse_values_df
from parking_logic import ParkingStateManager, enrich_session_with_realtime_duration

app = Flask(__name__)
CORS(app)  # Enable CORS nếu cần truy cập từ frontend

# Global manager instance
manager = None

# Queue để push thông báo EXITTING
exit_notifications = queue.Queue()

all_locations = {
    f"{row}{col}"
    for row in "ABCDEF"
    for col in range(1, 11)
}


def run_spark_streaming():
    """Chạy Spark Streaming trong background thread"""
    global manager

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
    manager = ParkingStateManager(all_locations=all_locations)

    def handle_batch(batch_df, batch_id):
        for row in batch_df.collect():
            event_dict = row.asDict()

            # Kiểm tra event EXITTING và push notification
            if event_dict.get('event_type') == 'EXITTING':
                notification = {
                    "event_type": "EXITTING",
                    "license_plate": event_dict.get('license_plate'),
                    "location": event_dict.get('location'),
                    "timestamp": event_dict.get('timestamp'),
                    "message": f"Xe {event_dict.get('license_plate')} đang rời vị trí {event_dict.get('location')}"
                }
                exit_notifications.put(notification)
                print(f"[NOTIFICATION] {notification['message']}")

            manager.process_event(event_dict)

        # print(f"\n[Batch {batch_id}] Processed events")
        # print(f"Active sessions: {len(manager.get_active_sessions())}")
        # print(f"Busy locations: {len(manager.get_busy_locations())}")
        # print(f"Free locations: {len(manager.get_free_locations())}")

    query = events_df.writeStream.foreachBatch(handle_batch).outputMode("append").start()
    query.awaitTermination()


@app.route('/api/location/<location_id>', methods=['GET'])
def get_location_info(location_id):
    """
    API 1: Truy xuất thông tin location
    GET /api/location/B5
    Trả về thông tin xe đang đỗ hoặc "Free"
    """
    if manager is None:
        return jsonify({"error": "System initializing"}), 503

    location_id = location_id.upper()

    # Kiểm tra location có hợp lệ không
    if location_id not in all_locations:
        return jsonify({"error": "Invalid location"}), 404

    # Kiểm tra trong active sessions
    active_sessions = manager.get_active_sessions()
    for sess in active_sessions:
        if sess.get('location') == location_id:
            # Enrich với realtime duration
            enriched = enrich_session_with_realtime_duration(sess)
            return jsonify({
                "license_plate": enriched.get('license_plate'),
                "location": enriched.get('location'),
                "start_time": enriched.get('start_time'),
                "duration": enriched.get('duration'),
                "cost": enriched.get('cost')
            })

    # Location trống
    return jsonify({"status": "Free", "location": location_id})


@app.route('/api/locations/free', methods=['GET'])
def get_free_locations():
    """
    API 2: Lấy danh sách location còn trống
    GET /api/locations/free
    """
    if manager is None:
        return jsonify({"error": "System initializing"}), 503

    free_locs = manager.get_free_locations()
    return jsonify({
        "free_locations": sorted(list(free_locs)),
        "count": len(free_locs)
    })


@app.route('/api/locations/busy', methods=['GET'])
def get_busy_locations():
    """
    API bổ sung: Lấy danh sách location đang có xe
    GET /api/locations/busy
    """
    if manager is None:
        return jsonify({"error": "System initializing"}), 503

    busy_locs = manager.get_busy_locations()
    return jsonify({
        "busy_locations": sorted(list(busy_locs)),
        "count": len(busy_locs)
    })


# @app.route('/api/sessions/active', methods=['GET'])
# def get_active_sessions():
#     """
#     API bổ sung: Lấy tất cả sessions đang hoạt động
#     GET /api/sessions/active
#     """
#     if manager is None:
#         return jsonify({"error": "System initializing"}), 503
#
#     sessions = []
#     for sess in manager.get_active_sessions():
#         enriched = enrich_session_with_realtime_duration(sess)
#         sessions.append({
#             "license_plate": enriched.get('license_plate'),
#             "location": enriched.get('location'),
#             "start_time": enriched.get('start_time'),
#             "duration": enriched.get('duration'),
#             "cost": enriched.get('cost')
#         })
#
#     return jsonify({
#         "active_sessions": sessions,
#         "count": len(sessions)
#     })


@app.route('/api/sessions/closed', methods=['GET'])
def get_closed_sessions():
    """
    API bổ sung: Lấy lịch sử sessions đã đóng
    GET /api/sessions/closed
    """
    if manager is None:
        return jsonify({"error": "System initializing"}), 503

    closed = manager.get_closed_sessions()
    if closed:
        return jsonify({
            "closed_sessions": closed[-1]
        })
    else:
        return jsonify({
            "closed_sessions": closed
        })


# @app.route('/api/status', methods=['GET'])
# def get_system_status():
#     """
#     API kiểm tra trạng thái hệ thống
#     GET /api/status
#     """
#     if manager is None:
#         return jsonify({"status": "initializing"}), 503
#
#     return jsonify({
#         "status": "running",
#         "total_locations": len(all_locations),
#         "free_locations": len(manager.get_free_locations()),
#         "busy_locations": len(manager.get_busy_locations()),
#         "active_sessions": len(manager.get_active_sessions()),
#         "closed_sessions": len(manager.get_closed_sessions())
#     })


# @app.route('/api/notifications/exit', methods=['GET'])
# def exit_notifications_stream():
#     """
#     API 3: Stream thông báo realtime khi có xe EXITTING
#     GET /api/notifications/exit
#
#     Sử dụng Server-Sent Events (SSE) để push realtime
#     """
#
#     def generate():
#         # Gửi comment để giữ kết nối
#         yield f": keepalive\n\n"
#
#         while True:
#             try:
#                 # Đợi notification từ queue (timeout 30s)
#                 notification = exit_notifications.get(timeout=30)
#
#                 # Format SSE message
#                 data = json.dumps(notification)
#                 yield f"data: {data}\n\n"
#
#             except queue.Empty:
#                 # Gửi keepalive message mỗi 30s
#                 yield f": keepalive\n\n"
#
#     return Response(
#         generate(),
#         mimetype='text/event-stream',
#         headers={
#             'Cache-Control': 'no-cache',
#             'X-Accel-Buffering': 'no',
#             'Connection': 'keep-alive'
#         }
#     )


# @app.route('/api/notifications/exit/latest', methods=['GET'])
# def get_latest_exit_notification():
#     """
#     API lấy thông báo EXITTING mới nhất (không realtime)
#     GET /api/notifications/exit/latest?timeout=5
#
#     Query params:
#     - timeout: số giây đợi notification (default: 5)
#     """
#     timeout = request.args.get('timeout', default=5, type=int)
#
#     try:
#         notification = exit_notifications.get(timeout=timeout)
#         return jsonify(notification)
#     except queue.Empty:
#         return jsonify({"message": "No exit notification available"}), 204


def main():
    # Khởi động Spark Streaming trong background thread
    spark_thread = threading.Thread(target=run_spark_streaming, daemon=True)
    spark_thread.start()

    # Đợi manager khởi tạo
    print("Waiting for Spark Streaming to initialize...")
    while manager is None:
        time.sleep(1)

    print("System ready! Starting Flask API server...")

    # Khởi động Flask API server
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()