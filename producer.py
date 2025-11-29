import time
import random
from datetime import datetime
from enum import Enum
from kafka import KafkaProducer
import threading
from concurrent.futures import ThreadPoolExecutor
import signal
import sys
import json

def load_config(path="config.json"):
    with open(path, "r") as f:
        return json.load(f)

config = load_config()

NUM_CAMERAS = int(config.get("num_cameras"))
DURATION_MINUTES = int(config.get("duration_minutes"))
KAFKA_TOPIC = config.get("kafka_topic")
KAFKA_SERVERS = config.get("kafka_servers")


class ParkingStatus(Enum):
    ENTERING = "Đang vào"
    PARKED = "Đã đỗ"
    MOVING = "Đang di chuyển"
    EXITING = "Đang ra"


class ParkingEvent:
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

    PARKING_LOCATIONS = [
        "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "A10",
        "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "B10",
        "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "C10",
        "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10",
        "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "E10",
        "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"
    ]

    def __init__(self, occupied_locations=None, active_license_plates=None, allowed_locations=None):
        if active_license_plates:
            available_plates = [plate for plate in self.LICENSE_PLATES if plate not in active_license_plates]
            if available_plates:
                self.license_plate = random.choice(available_plates)
            else:
                self.license_plate = random.choice(self.LICENSE_PLATES)
        else:
            self.license_plate = random.choice(self.LICENSE_PLATES)

        location_pool = allowed_locations if allowed_locations else self.PARKING_LOCATIONS

        if occupied_locations:
            available_locations = [loc for loc in location_pool if loc not in occupied_locations]
            if available_locations:
                self.location = random.choice(available_locations)
            else:
                self.location = random.choice(location_pool)
        else:
            self.location = random.choice(location_pool)

        self.status = ParkingStatus.ENTERING
        self.parked_count = 0
        self.parked_duration = 0

    def next_status(self, occupied_locations=None, active_license_plates=None, allowed_locations=None):
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
            self.__init__(occupied_locations, active_license_plates, allowed_locations)

    def force_exit(self):
        if self.status == ParkingStatus.PARKED:
            self.status = ParkingStatus.EXITING

    def get_event_info(self):
        return {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timestamp_unix": int(time.time()),
            "license_plate": self.license_plate,
            "location": self.location,
            "status_code": self.status.name
        }


class CameraSimulator:
    """Giả lập 1 camera giám sát một khu vực trong bãi đỗ"""

    def __init__(self, camera_id, allowed_locations, producer, kafka_topic,
                 event_interval=3, exit_interval=30):
        self.camera_id = camera_id
        self.allowed_locations = allowed_locations
        self.producer = producer
        self.kafka_topic = kafka_topic
        self.event_interval = event_interval
        self.exit_interval = exit_interval

        self.occupied_locations = set()
        self.active_license_plates = set()
        self.active_vehicles = []
        self.all_vehicles = {}

        self.running = True
        self.lock = threading.Lock()

    def initialize_vehicles(self, num_initial=3):
        """Khởi tạo một số xe ban đầu"""
        with self.lock:
            for _ in range(num_initial):
                vehicle = ParkingEvent(
                    self.occupied_locations,
                    self.active_license_plates,
                    self.allowed_locations
                )
                self.active_vehicles.append(vehicle)
                self.occupied_locations.add(vehicle.location)
                self.active_license_plates.add(vehicle.license_plate)
                self.all_vehicles[vehicle.license_plate] = vehicle

    def send_event(self, event_data, event_type="NORMAL"):
        """Gửi event lên Kafka với KEY = license_plate"""
        try:
            # QUAN TRỌNG: Gửi với key=license_plate để đảm bảo partition consistency
            future = self.producer.send(
                self.kafka_topic,
                key=event_data["license_plate"].encode('utf-8'),
                value=event_data
            )
            record_metadata = future.get(timeout=10)

            prefix = "📤" if event_type == "NORMAL" else "🔴"
            print(f"{prefix} [Camera {self.camera_id}] {event_type}: {event_data['license_plate']} "
                  f"@ {event_data['location']} - {event_data['status_code']}")
            print(f"   ↳ Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

        except Exception as e:
            print(f"❌ [Camera {self.camera_id}] Failed to send: {e}")

    def process_scheduled_exit(self, last_exit_time):
        """Xử lý xe ra theo lịch định kỳ"""
        with self.lock:
            parked_vehicles = [v for v in self.active_vehicles if v.status == ParkingStatus.PARKED]
            if parked_vehicles:
                vehicle_to_exit = random.choice(parked_vehicles)
                vehicle_to_exit.force_exit()
                event_data = vehicle_to_exit.get_event_info()
                self.send_event(event_data, "SCHEDULED_EXIT")
                return True
        return False

    def process_normal_event(self):
        """Xử lý sự kiện bình thường"""
        with self.lock:
            if not self.active_vehicles:
                return

            vehicle = random.choice(self.active_vehicles)
            old_status = vehicle.status
            old_location = vehicle.location
            old_license_plate = vehicle.license_plate

            event_data = vehicle.get_event_info()
            self.all_vehicles[vehicle.license_plate] = vehicle

            self.send_event(event_data)

            vehicle.next_status(
                self.occupied_locations,
                self.active_license_plates,
                self.allowed_locations
            )

            # Cập nhật occupied locations
            if old_status == ParkingStatus.EXITING and vehicle.status == ParkingStatus.ENTERING:
                self.occupied_locations.discard(old_location)
                self.occupied_locations.add(vehicle.location)
                self.active_license_plates.discard(old_license_plate)
                self.active_license_plates.add(vehicle.license_plate)
            elif vehicle.status == ParkingStatus.EXITING and old_status != ParkingStatus.EXITING:
                self.occupied_locations.discard(vehicle.location)

            # Thêm xe mới ngẫu nhiên
            if random.random() > 0.6 and len(self.active_vehicles) < 8:
                if (len(self.occupied_locations) < len(self.allowed_locations) and
                    len(self.active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                    new_vehicle = ParkingEvent(
                        self.occupied_locations,
                        self.active_license_plates,
                        self.allowed_locations
                    )
                    self.active_vehicles.append(new_vehicle)
                    self.occupied_locations.add(new_vehicle.location)
                    self.active_license_plates.add(new_vehicle.license_plate)

            # Xóa xe đã ra
            if random.random() > 0.5:
                vehicles_to_remove = [v for v in self.active_vehicles if v.status == ParkingStatus.EXITING]
                for v in vehicles_to_remove:
                    self.active_vehicles.remove(v)
                    self.occupied_locations.discard(v.location)
                    self.active_license_plates.discard(v.license_plate)

            # Duy trì số lượng xe tối thiểu
            while (len(self.active_vehicles) < 3 and
                   len(self.occupied_locations) < len(self.allowed_locations) and
                   len(self.active_license_plates) < len(ParkingEvent.LICENSE_PLATES)):
                new_vehicle = ParkingEvent(
                    self.occupied_locations,
                    self.active_license_plates,
                    self.allowed_locations
                )
                self.active_vehicles.append(new_vehicle)
                self.occupied_locations.add(new_vehicle.location)
                self.active_license_plates.add(new_vehicle.license_plate)

    def flush_all_vehicles(self):
        """Gửi EXITING cho tất cả xe còn đang đỗ"""
        print(f"\n[Camera {self.camera_id}] Flushing all parked vehicles...")
        with self.lock:
            for plate, vehicle in self.all_vehicles.items():
                if vehicle.status == ParkingStatus.PARKED:
                    vehicle.force_exit()
                    event_data = vehicle.get_event_info()
                    self.send_event(event_data, "FORCED_EXIT")

    def run(self, duration_seconds):
        """Chạy camera simulator"""
        print(f"🎥 [Camera {self.camera_id}] Started - Monitoring locations: {self.allowed_locations[:5]}...")

        self.initialize_vehicles()

        start_time = time.time()
        end_time = start_time + duration_seconds
        last_exit_time = start_time

        try:
            while time.time() < end_time and self.running:
                current_time = time.time()

                # Xử lý scheduled exit
                if current_time - last_exit_time >= self.exit_interval:
                    if self.process_scheduled_exit(last_exit_time):
                        last_exit_time = current_time

                # Xử lý event bình thường
                self.process_normal_event()

                # Sleep
                delay = random.uniform(self.event_interval * 0.5, self.event_interval * 1.5)
                time.sleep(delay)

            # Flush tất cả xe khi kết thúc
            if self.running:
                self.flush_all_vehicles()

        except Exception as e:
            print(f"❌ [Camera {self.camera_id}] Error: {e}")
        finally:
            print(f"🎥 [Camera {self.camera_id}] Stopped")

    def stop(self):
        """Dừng camera"""
        self.running = False


class MultiCameraProducer:
    """Quản lý nhiều cameras"""

    def __init__(self, num_cameras, kafka_topic, bootstrap_servers, duration_minutes=2):
        self.num_cameras = num_cameras
        self.kafka_topic = kafka_topic
        self.bootstrap_servers = bootstrap_servers
        self.duration_minutes = duration_minutes
        self.cameras = []
        self.producer = None
        self.running = True

        # Setup signal handler
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, sig, frame):
        """Xử lý Ctrl+C"""
        print("\n\n⚠️  Received stop signal, shutting down cameras...")
        self.stop_all_cameras()
        sys.exit(0)

    def setup_producer(self):
        """Khởi tạo Kafka producer"""
        print(f"🔌 Connecting to Kafka: {self.bootstrap_servers}")
        print(f"📍 Topic: {self.kafka_topic}")

        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k if isinstance(k, bytes) else k.encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            print("✅ Connected to Kafka successfully!\n")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False

    def divide_locations(self):
        """Chia locations cho từng camera"""
        all_locations = ParkingEvent.PARKING_LOCATIONS
        locations_per_camera = len(all_locations) // self.num_cameras

        camera_locations = []
        for i in range(self.num_cameras):
            start_idx = i * locations_per_camera
            if i == self.num_cameras - 1:  # Camera cuối nhận hết còn lại
                end_idx = len(all_locations)
            else:
                end_idx = (i + 1) * locations_per_camera

            camera_locations.append(all_locations[start_idx:end_idx])

        return camera_locations

    def create_cameras(self):
        """Tạo các camera simulators"""
        camera_locations = self.divide_locations()

        for i in range(self.num_cameras):
            camera = CameraSimulator(
                camera_id=i + 1,
                allowed_locations=camera_locations[i],
                producer=self.producer,
                kafka_topic=self.kafka_topic,
                event_interval=2,  # Mỗi camera gửi event mỗi ~2s
                exit_interval=30   # Xe ra mỗi 30s
            )
            self.cameras.append(camera)

        print(f"✅ Created {self.num_cameras} cameras")
        for i, camera in enumerate(self.cameras):
            print(f"   Camera {i+1}: {len(camera.allowed_locations)} locations "
                  f"({camera.allowed_locations[0]} - {camera.allowed_locations[-1]})")
        print()

    def run(self):
        """Chạy tất cả cameras"""
        if not self.setup_producer():
            return

        self.create_cameras()

        duration_seconds = self.duration_minutes * 60

        print(f"🚀 Starting {self.num_cameras} cameras for {self.duration_minutes} minutes...")
        print(f"{'='*80}\n")

        # Chạy cameras trong threads
        with ThreadPoolExecutor(max_workers=self.num_cameras) as executor:
            futures = [
                executor.submit(camera.run, duration_seconds)
                for camera in self.cameras
            ]

            # Đợi tất cả cameras hoàn thành
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ Camera error: {e}")

        print(f"\n{'='*80}")
        print("✅ All cameras finished")

        # Flush và đóng producer
        try:
            self.producer.flush()
            time.sleep(2)  # Đợi flush hoàn tất
        except Exception as e:
            print(f"⚠️  Error on flush: {e}")
        finally:
            self.producer.close()
            print("✅ Kafka producer closed")

    def stop_all_cameras(self):
        """Dừng tất cả cameras"""
        for camera in self.cameras:
            camera.stop()


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║          MULTI-CAMERA PARKING EVENT PRODUCER                 ║
    ║     Giả lập nhiều cameras trong bãi đỗ xe                    ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    print(f"📹 Number of cameras: {NUM_CAMERAS}")
    print(f"⏱  Duration: {DURATION_MINUTES} minutes")
    print(f"📍 Kafka topic: {KAFKA_TOPIC}")
    print(f"🔌 Kafka servers: {KAFKA_SERVERS}")
    print(f"{'='*80}\n")

    # Chạy multi-camera producer
    multi_producer = MultiCameraProducer(
        num_cameras=NUM_CAMERAS,
        kafka_topic=KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        duration_minutes=DURATION_MINUTES
    )

    multi_producer.run()