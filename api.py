from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import threading
import time
import json
from kafka import KafkaConsumer
from collections import defaultdict
import math

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ==========================================================
# CONFIGURATION
# ==========================================================
KAFKA_SERVERS = "192.168.1.117:9092"
PROCESSED_TOPIC = "processed-data"
BLOCK_PRICE = 10000
BLOCK_SECONDS = 600

# Tất cả locations trong bãi
ALL_LOCATIONS = {
    f"{row}{col}"
    for row in "ABCDEF"
    for col in range(1, 11)
}


# ==========================================================
# GLOBAL STATE
# ==========================================================
class ParkingManager:
    def __init__(self):
        self.active_sessions = {}  # license_plate -> session_data
        self.closed_sessions = []  # Lịch sử sessions đã kết thúc
        self.occupancy = set()  # Set of busy locations
        self.lock = threading.Lock()

    def update_session(self, data):
        """Cập nhật session từ Kafka message"""
        with self.lock:
            plate = data.get("license_plate")
            status = data.get("status")

            if status == "ACTIVE":
                # Cập nhật session đang hoạt động
                self.active_sessions[plate] = data
                self.occupancy.add(data.get("location"))

            elif status == "CLOSED":
                # Di chuyển sang closed sessions
                if plate in self.active_sessions:
                    del self.active_sessions[plate]

                self.closed_sessions.append(data)
                self.occupancy.discard(data.get("location"))

                # Giữ tối đa 1000 closed sessions
                if len(self.closed_sessions) > 1000:
                    self.closed_sessions = self.closed_sessions[-1000:]

    def get_active_sessions(self):
        """Lấy danh sách sessions đang hoạt động với duration realtime"""
        with self.lock:
            results = []
            current_time = int(time.time())

            for plate, sess in self.active_sessions.items():
                # Tính duration và cost realtime
                start_time = sess.get("start_time", current_time)
                duration = current_time - start_time
                cost = math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE

                results.append({
                    "license_plate": plate,
                    "location": sess.get("location"),
                    "start_time": start_time,
                    "duration": duration,
                    "cost": cost,
                    "status": "ACTIVE"
                })

            return results

    def get_closed_sessions(self, limit=50):
        """Lấy lịch sử sessions đã kết thúc"""
        with self.lock:
            return self.closed_sessions[-limit:]

    def get_busy_locations(self):
        """Lấy danh sách vị trí đang bận"""
        with self.lock:
            return sorted(list(self.occupancy))

    def get_free_locations(self):
        """Lấy danh sách vị trí còn trống"""
        with self.lock:
            return sorted(list(ALL_LOCATIONS - self.occupancy))

    def get_location_info(self, location_id):
        """Lấy thông tin chi tiết của một vị trí"""
        with self.lock:
            current_time = int(time.time())

            # Tìm xe đang đỗ ở vị trí này
            for plate, sess in self.active_sessions.items():
                if sess.get("location") == location_id:
                    start_time = sess.get("start_time", current_time)
                    duration = current_time - start_time
                    cost = math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE

                    return {
                        "status": "OCCUPIED",
                        "license_plate": plate,
                        "location": location_id,
                        "start_time": start_time,
                        "duration": duration,
                        "cost": cost
                    }

            # Vị trí trống
            return {
                "status": "FREE",
                "location": location_id
            }

    def get_statistics(self):
        """Lấy thống kê tổng quan"""
        with self.lock:
            return {
                "total_locations": len(ALL_LOCATIONS),
                "busy_count": len(self.occupancy),
                "free_count": len(ALL_LOCATIONS) - len(self.occupancy),
                "active_sessions": len(self.active_sessions),
                "closed_sessions_total": len(self.closed_sessions)
            }


# Global manager instance
manager = ParkingManager()


# ==========================================================
# KAFKA CONSUMER THREAD
# ==========================================================
def kafka_consumer_thread():
    """Background thread để đọc dữ liệu từ Kafka processed-data topic"""
    print(f"🎧 Starting Kafka consumer for topic: {PROCESSED_TOPIC}")

    consumer = KafkaConsumer(
        PROCESSED_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='parking-websocket-api',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("✅ Kafka consumer connected!")

    for message in consumer:
        try:
            data = message.value

            # Cập nhật state
            manager.update_session(data)

            # Emit qua WebSocket
            socketio.emit('parking_update', data, namespace='/parking')

            # Nếu là CLOSED session, emit notification riêng
            if data.get('status') == 'CLOSED':
                notification = {
                    "type": "EXIT",
                    "license_plate": data.get("license_plate"),
                    "location": data.get("location"),
                    "duration": data.get("duration"),
                    "cost": data.get("cost"),
                    "timestamp": data.get("end_time"),
                    "message": f"Xe {data.get('license_plate')} đã rời vị trí {data.get('location')}"
                }
                socketio.emit('exit_notification', notification, namespace='/parking')
                print(f"🚗 [EXIT] {notification['message']}")

            # Emit statistics update
            stats = manager.get_statistics()
            socketio.emit('statistics_update', stats, namespace='/parking')

        except Exception as e:
            print(f"❌ Error processing message: {e}")
            continue


# ==========================================================
# REST API ENDPOINTS
# ==========================================================

@app.route('/api/status', methods=['GET'])
def get_system_status():
    """GET /api/status - Kiểm tra trạng thái hệ thống"""
    stats = manager.get_statistics()
    return jsonify({
        "status": "running",
        **stats
    })


@app.route('/api/location/<location_id>', methods=['GET'])
def get_location_info(location_id):
    """GET /api/location/A1 - Lấy thông tin vị trí cụ thể"""
    location_id = location_id.upper()

    if location_id not in ALL_LOCATIONS:
        return jsonify({"error": "Invalid location"}), 404

    info = manager.get_location_info(location_id)
    return jsonify(info)


@app.route('/api/locations/free', methods=['GET'])
def get_free_locations():
    """GET /api/locations/free - Lấy danh sách vị trí trống"""
    free_locs = manager.get_free_locations()
    return jsonify({
        "free_locations": free_locs,
        "count": len(free_locs)
    })


@app.route('/api/locations/busy', methods=['GET'])
def get_busy_locations():
    """GET /api/locations/busy - Lấy danh sách vị trí đang bận"""
    busy_locs = manager.get_busy_locations()
    return jsonify({
        "busy_locations": busy_locs,
        "count": len(busy_locs)
    })


@app.route('/api/sessions/active', methods=['GET'])
def get_active_sessions():
    """GET /api/sessions/active - Lấy tất cả sessions đang hoạt động"""
    sessions = manager.get_active_sessions()
    return jsonify({
        "active_sessions": sessions,
        "count": len(sessions)
    })


@app.route('/api/sessions/closed', methods=['GET'])
def get_closed_sessions():
    """GET /api/sessions/closed?limit=50 - Lấy lịch sử sessions"""
    limit = request.args.get('limit', default=50, type=int)
    closed = manager.get_closed_sessions(limit=limit)
    return jsonify({
        "closed_sessions": closed,
        "count": len(closed)
    })


@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    """GET /api/statistics - Lấy thống kê tổng quan"""
    stats = manager.get_statistics()
    return jsonify(stats)


# ==========================================================
# WEBSOCKET EVENTS
# ==========================================================

@socketio.on('connect', namespace='/parking')
def handle_connect():
    """Client kết nối WebSocket"""
    print(f"🔌 Client connected: {request.sid}")

    # Gửi dữ liệu khởi tạo
    emit('initial_data', {
        "active_sessions": manager.get_active_sessions(),
        "statistics": manager.get_statistics(),
        "free_locations": manager.get_free_locations(),
        "busy_locations": manager.get_busy_locations()
    })


@socketio.on('disconnect', namespace='/parking')
def handle_disconnect():
    """Client ngắt kết nối"""
    print(f"🔌 Client disconnected: {request.sid}")


@socketio.on('request_update', namespace='/parking')
def handle_request_update():
    """Client yêu cầu cập nhật dữ liệu"""
    emit('initial_data', {
        "active_sessions": manager.get_active_sessions(),
        "statistics": manager.get_statistics(),
        "free_locations": manager.get_free_locations(),
        "busy_locations": manager.get_busy_locations()
    })


@socketio.on('request_location', namespace='/parking')
def handle_request_location(data):
    """Client yêu cầu thông tin vị trí cụ thể"""
    location_id = data.get('location_id', '').upper()
    if location_id in ALL_LOCATIONS:
        info = manager.get_location_info(location_id)
        emit('location_info', info)


# ==========================================================
# REALTIME DURATION UPDATE THREAD
# ==========================================================
def realtime_duration_updater():
    """Thread cập nhật duration realtime mỗi 5 giây"""
    while True:
        time.sleep(5)

        # Lấy active sessions với duration mới nhất
        sessions = manager.get_active_sessions()
        stats = manager.get_statistics()

        # Emit qua WebSocket
        socketio.emit('duration_update', {
            "active_sessions": sessions,
            "statistics": stats
        }, namespace='/parking')


# ==========================================================
# MAIN
# ==========================================================
def main():
    print("=" * 60)
    print("🚗 PARKING MANAGEMENT WEBSOCKET API")
    print("=" * 60)
    print(f"📥 Kafka topic: {PROCESSED_TOPIC}")
    print(f"🔌 WebSocket namespace: /parking")
    print(f"🌐 REST API: http://0.0.0.0:5000/api/")
    print("=" * 60)

    # Start Kafka consumer thread
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()

    # Start realtime updater thread
    updater_thread = threading.Thread(target=realtime_duration_updater, daemon=True)
    updater_thread.start()

    # Wait for initialization
    print("\n⏳ Waiting for Kafka consumer to initialize...")
    time.sleep(3)
    print("✅ System ready!\n")

    # Start Flask-SocketIO server
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)


if __name__ == "__main__":
    main()