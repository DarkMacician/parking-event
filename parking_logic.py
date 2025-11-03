# parking_logic.py
import math
import time
from collections import defaultdict

BLOCK_PRICE = 10000   # Giá mỗi block 10 phút, tùy chỉnh
BLOCK_SECONDS = 600   # Một block = 10 phút

def enrich_session_with_realtime_duration(session):
    now_unix = int(time.time())
    duration = now_unix - session["start_time"]
    cost = math.ceil(duration/BLOCK_SECONDS)*BLOCK_PRICE
    session_realtime = dict(session)
    session_realtime["duration"] = duration
    session_realtime["cost"] = cost
    return session_realtime

class ParkingStateManager:
    """
    Quản lý trạng thái phiên đỗ xe và occupancy bãi dựa trên luồng dict từng event
    """
    def __init__(self, all_locations=None):
        self.sessions = dict()      # license_plate => phiên đang mở
        self.occupancy = set()      # location đang bị chiếm
        self.history = []           # Lưu log kết quả kết thúc phiên
        self.all_locations = set(all_locations) if all_locations else set()
        # Nếu cần thống kê bãi trống, nhập trước toàn bộ location

    def process_event(self, event: dict):
        plate = event["license_plate"]
        status = event["status_code"]
        loc = event["location"]
        ts_unix = event["timestamp_unix"]

        # Khi xe PARKED
        if status == "PARKED":
            if plate not in self.sessions:
                self.sessions[plate] = {
                    "plate": plate,
                    "location": loc,
                    "start": ts_unix,
                    "last": ts_unix,
                }
            else:
                # Heartbeat đang đỗ
                self.sessions[plate]["last"] = ts_unix
            self.occupancy.add(loc)
        # Khi xe EXITING
        elif status == "EXITING" and plate in self.sessions:
            sess = self.sessions.pop(plate)
            duration = ts_unix - sess["start"]
            cost = math.ceil(duration / BLOCK_SECONDS) * BLOCK_PRICE
            self.occupancy.discard(loc)
            # Lưu lại bản ghi hoàn thành phiên
            self.history.append({
                "license_plate": plate,
                "start_time": sess["start"],
                "end_time": ts_unix,
                "duration": duration,
                "location": loc,
                "cost": cost,
                "status": "CLOSED"
            })
        # Nếu cần xử lý MOVING/ENTERING, có thể bổ sung cho bãi thông minh

    def get_active_sessions(self):
        res = []
        for plate, sess in self.sessions.items():
            res.append({
                "license_plate": plate,
                "location": sess["location"],
                "start_time": sess["start"]
            })
        return res

    def get_busy_locations(self):
        return list(self.occupancy)

    def get_free_locations(self):
        if self.all_locations:
            return list(self.all_locations - self.occupancy)
        return []

    def get_closed_sessions(self):
        # Các phiên đã exit
        return self.history
