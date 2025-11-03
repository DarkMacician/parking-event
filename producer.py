import time
import random
import json
from datetime import datetime
from enum import Enum
from kafka import KafkaProducer


class ParkingStatus(Enum):
    """Các trạng thái của xe trong bãi đỗ"""
    ENTERING = "Đang vào"
    PARKED = "Đã đỗ"
    MOVING = "Đang di chuyển"
    EXITING = "Đang ra"


class ParkingEvent:
    """Class đại diện cho một sự kiện đỗ xe"""

    # Danh sách biển số xe có sẵn (mở rộng)
    LICENSE_PLATES = [
        "29A-12345", "29A-54321", "29A-67890", "29A-11111", "29A-99999",
        "30B-12345", "30B-67890", "30B-33333", "30B-88888", "30B-55555",
        "51C-11111", "51C-22222", "51C-44444", "51C-77777", "51C-12121",
        "59D-98765", "59D-45678", "59D-13579", "59D-24680", "59D-86420",
        "79D-99999", "79D-10101", "79D-20202", "79D-30303", "79D-40404",
        "92E-54321", "92E-65432", "92E-76543", "92E-87654", "92E-98765",
        "15F-88888", "15F-11122", "15F-33344", "15F-55566", "15F-77788",
        "43G-22222", "43G-12389", "43G-45612", "43G-78945", "43G-32165",
        "60H-10203", "60H-40506", "60H-70809", "60H-20406", "60H-50810"
    ]

    # Danh sách vị trí đỗ (mở rộng đến tầng F)
    PARKING_LOCATIONS = [
        # Tầng A
        "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10",
        # Tầng B
        "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10",
        # Tầng C
        "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10",
        # Tầng D
        "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10",
        # Tầng E
        "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "E10",
        # Tầng F (VIP)
        "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"
    ]

    def __init__(self, occupied_locations=None, active_license_plates=None):
        # Chọn biển số chưa được sử dụng
        if active_license_plates:
            available_plates = [plate for plate in self.LICENSE_PLATES if plate not in active_license_plates]
            if available_plates:
                self.license_plate = random.choice(available_plates)
            else:
                # Nếu hết biển số, chọn random (trường hợp này không nên xảy ra)
                self.license_plate = random.choice(self.LICENSE_PLATES)
        else:
            self.license_plate = random.choice(self.LICENSE_PLATES)

        # Chọn vị trí còn trống
        if occupied_locations:
            available_locations = [loc for loc in self.PARKING_LOCATIONS if loc not in occupied_locations]
            if available_locations:
                self.location = random.choice(available_locations)
            else:
                # Nếu hết chỗ, chọn random (trường hợp này không nên xảy ra)
                self.location = random.choice(self.PARKING_LOCATIONS)
        else:
            self.location = random.choice(self.PARKING_LOCATIONS)

        self.status = ParkingStatus.ENTERING
        self.parked_count = 0
        self.parked_duration = 0

    def next_status(self, occupied_locations=None, active_license_plates=None):
        """Chuyển sang trạng thái tiếp theo theo logic"""
        if self.status == ParkingStatus.ENTERING:
            self.status = ParkingStatus.PARKED
            self.parked_duration = random.randint(20, 200)
            self.parked_count = 0

        elif self.status == ParkingStatus.PARKED:
            self.parked_count += 1

            if self.parked_count >= self.parked_duration:
                self.status = ParkingStatus.MOVING

        elif self.status == ParkingStatus.MOVING:
            self.status = ParkingStatus.EXITING

        else:
            # Nếu đã ra, tạo xe mới với vị trí và biển số trống
            self.__init__(occupied_locations, active_license_plates)

    def get_event_info(self):
        """Lấy thông tin sự kiện dưới dạng dictionary"""
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_unix": int(time.time()),
            "license_plate": self.license_plate,
            "location": self.location,
            "status_code": self.status.name
        }


def parking_stream_realtime(duration_minutes=30, event_interval=3):
    """
    Mô phỏng streaming các sự kiện đỗ xe trong thời gian thực

    Args:
        duration_minutes (int): Thời gian chạy streaming (phút)
        event_interval (float): Thời gian trung bình giữa các sự kiện (giây)
    """
    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)

    # Theo dõi các vị trí và biển số đang được sử dụng
    occupied_locations = set()
    active_license_plates = set()

    # Tạo nhiều xe ngẫu nhiên để mô phỏng bãi đỗ thực tế
    active_vehicles = []
    for _ in range(5):
        vehicle = ParkingEvent(occupied_locations, active_license_plates)
        active_vehicles.append(vehicle)
        occupied_locations.add(vehicle.location)
        active_license_plates.add(vehicle.license_plate)

    try:
        while time.time() < end_time:
            # Chọn ngẫu nhiên một xe để cập nhật trạng thái
            vehicle = random.choice(active_vehicles)

            # Lưu trạng thái, vị trí và biển số cũ
            old_status = vehicle.status
            old_location = vehicle.location
            old_license_plate = vehicle.license_plate

            event_data = vehicle.get_event_info()
            print(json.dumps(event_data, ensure_ascii=False))

            # Chuyển sang trạng thái tiếp theo
            vehicle.next_status(occupied_locations, active_license_plates)

            # Quản lý occupied_locations và active_license_plates
            if old_status == ParkingStatus.EXITING and vehicle.status == ParkingStatus.ENTERING:
                # Xe tạo mới với vị trí và biển số mới
                occupied_locations.discard(old_location)
                occupied_locations.add(vehicle.location)
                active_license_plates.discard(old_license_plate)
                active_license_plates.add(vehicle.license_plate)
            elif vehicle.status == ParkingStatus.EXITING and old_status != ParkingStatus.EXITING:
                # Xe vừa chuyển sang EXITING - giải phóng vị trí (giữ biển số đến khi xe bị xóa)
                occupied_locations.discard(vehicle.location)

            # Chỉ in JSON khi xe KHÔNG ở trạng thái PARKED
            # hoặc khi xe vừa chuyển sang trạng thái PARKED (lần đầu)
            # if vehicle.status != ParkingStatus.PARKED or old_status != ParkingStatus.PARKED:
            #                 event_data = vehicle.get_event_info()
            #                 print(json.dumps(event_data, ensure_ascii=False))

            # Thêm xe mới ngẫu nhiên (mô phỏng xe mới vào bãi)
            if random.random() > 0.6 and len(active_vehicles) < 8:
                # Chỉ thêm nếu còn chỗ trống VÀ còn biển số
                if (len(occupied_locations) < len(ParkingEvent.PARKING_LOCATIONS) and
                        len(active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                    new_vehicle = ParkingEvent(occupied_locations, active_license_plates)
                    active_vehicles.append(new_vehicle)
                    occupied_locations.add(new_vehicle.location)
                    active_license_plates.add(new_vehicle.license_plate)

            # Xóa xe đã ra khỏi bãi
            if random.random() > 0.5:
                vehicles_to_remove = [v for v in active_vehicles if v.status == ParkingStatus.EXITING]
                for v in vehicles_to_remove:
                    active_vehicles.remove(v)
                    occupied_locations.discard(v.location)
                    active_license_plates.discard(v.license_plate)

            # Đảm bảo luôn có ít nhất 3 xe
            while (len(active_vehicles) < 3 and
                   len(occupied_locations) < len(ParkingEvent.PARKING_LOCATIONS) and
                   len(active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                new_vehicle = ParkingEvent(occupied_locations, active_license_plates)
                active_vehicles.append(new_vehicle)
                occupied_locations.add(new_vehicle.location)
                active_license_plates.add(new_vehicle.license_plate)

            # Delay ngẫu nhiên giữa các sự kiện
            delay = random.uniform(event_interval * 0.5, event_interval * 1.5)
            time.sleep(delay)

    except KeyboardInterrupt:
        pass


def parking_stream_realtime(duration_minutes=30, event_interval=3, kafka_topic="test-topic",
                            bootstrap_servers="192.168.1.117:9092"):
    """Streaming parking events vào Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )

    start_time = time.time()
    end_time = start_time + (duration_minutes * 60)

    occupied_locations = set()
    active_license_plates = set()
    active_vehicles = []

    for _ in range(5):
        vehicle = ParkingEvent(occupied_locations, active_license_plates)
        active_vehicles.append(vehicle)
        occupied_locations.add(vehicle.location)
        active_license_plates.add(vehicle.license_plate)

    try:
        while time.time() < end_time:
            vehicle = random.choice(active_vehicles)
            old_status = vehicle.status
            old_location = vehicle.location
            old_license_plate = vehicle.license_plate

            event_data = vehicle.get_event_info()

            # Gửi dữ liệu vào Kafka
            producer.send(kafka_topic, value=event_data)
            producer.flush()
            print(f"📤 Sent to Kafka: {event_data}")

            vehicle.next_status(occupied_locations, active_license_plates)

            if old_status == ParkingStatus.EXITING and vehicle.status == ParkingStatus.ENTERING:
                occupied_locations.discard(old_location)
                occupied_locations.add(vehicle.location)
                active_license_plates.discard(old_license_plate)
                active_license_plates.add(vehicle.license_plate)
            elif vehicle.status == ParkingStatus.EXITING and old_status != ParkingStatus.EXITING:
                occupied_locations.discard(vehicle.location)

            if random.random() > 0.6 and len(active_vehicles) < 8:
                if (len(occupied_locations) < len(ParkingEvent.PARKING_LOCATIONS) and
                        len(active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                    new_vehicle = ParkingEvent(occupied_locations, active_license_plates)
                    active_vehicles.append(new_vehicle)
                    occupied_locations.add(new_vehicle.location)
                    active_license_plates.add(new_vehicle.license_plate)

            if random.random() > 0.5:
                vehicles_to_remove = [v for v in active_vehicles if v.status == ParkingStatus.EXITING]
                for v in vehicles_to_remove:
                    active_vehicles.remove(v)
                    occupied_locations.discard(v.location)
                    active_license_plates.discard(v.license_plate)

            while (len(active_vehicles) < 3 and
                   len(occupied_locations) < len(ParkingEvent.PARKING_LOCATIONS) and
                   len(active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                new_vehicle = ParkingEvent(occupied_locations, active_license_plates)
                active_vehicles.append(new_vehicle)
                occupied_locations.add(new_vehicle.location)
                active_license_plates.add(new_vehicle.license_plate)

            delay = random.uniform(event_interval * 0.5, event_interval * 1.5)
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n🛑 Stopped streaming.")
    finally:
        producer.close()


if __name__ == "__main__":
    # Streaming 30 phút, sự kiện mỗi 3 giây
    parking_stream_realtime(duration_minutes=30, event_interval=3)