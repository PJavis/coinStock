from confluent_kafka import Consumer, KafkaError

from dotenv import load_dotenv

from script.utils import load_environment_variables

load_dotenv()

env_vars = load_environment_variables()

conf = {
     # Pointing to brokers. Ensure these match the host and ports of your Kafka brokers.
     'bootstrap.servers': env_vars.get("KAFKA_BROKERS"),
     'group.id': "myGroup",  # Consumer group ID. Change as per your requirement.
     'auto.offset.reset': 'earliest'  # Start from the earliest messages if no offset is stored.
}

# Tạo một Kafka consumer
consumer = Consumer(conf)

# Đăng ký consumer với Kafka topic
consumer.subscribe([env_vars.get("STOCK_PRICE_KAFKA_TOPIC")])

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
