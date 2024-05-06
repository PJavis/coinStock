from confluent_kafka import Consumer, KafkaError

# Thiết lập các tham số kết nối tới Kafka broker
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Tạo một Kafka consumer
consumer = Consumer(conf)

# Đăng ký consumer với Kafka topic
topic = 'coin_price'
consumer.subscribe([topic])

# Lặp để đọc và in ra các tin nhắn
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(f"Consumer error: {msg.error()}")
            break
    else:
        print(msg)

# Đóng kết nối Kafka consumer
consumer.close()
