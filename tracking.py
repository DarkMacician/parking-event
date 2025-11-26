import json
from kafka import KafkaConsumer

# Khai báo kết nối Kafka và topic
KAFKA_SERVERS = '192.168.80.60:9092'
KAFKA_TOPIC = 'raw-data'

# Tạo consumer để đọc dữ liệu Kafka
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVERS,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id="parking-demo-001"
)

# Biến lưu trạng thái hiện tại: map biển số -> trạng thái cuối
current_status = {}
# Biến lưu vị trí đang bị chiếm
occupied_locations = set()

print("Bắt đầu đọc stream dữ liệu từ Kafka...")

try:
    for message in consumer:
        event = message.value
        license_plate = event.get('license_plate')
        location = event.get('location')
        status_code = event.get('status_code')

        # Cập nhật trạng thái biển số xe trong current_status
        current_status[license_plate] = {
            'location': location,
            'status_code': status_code
        }

        # Sau mỗi sự kiện, tính lại các vị trí đang bị chiếm
        occupied_locations.clear()
        for rec in current_status.values():
            if rec['status_code'] == 'PARKED':
                occupied_locations.add(rec['location'])

        # In danh sách vị trí đang bị chiếm mỗi lần nhận event
        print(f"Vị trí đang bị chiếm ({len(occupied_locations)}): {sorted(occupied_locations)}")

except KeyboardInterrupt:
    print("\nĐã dừng consumer.")
finally:
    consumer.close()
