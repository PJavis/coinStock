from confluent_kafka import Producer

import coinDesk
# Thiết lập các tham số kết nối tới Kafka broker

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Tạo một Kafka producer
producer = Producer(conf)

# Gửi một tin nhắn tới Kafka topic
topic = 'coin_price'
message = coinDesk.main()
print(message)
producer.produce(topic, value=message)

# Đảm bảo rằng tất cả các tin nhắn đã được gửi đi
producer.flush()
