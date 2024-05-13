import json
import time as t

from dotenv import load_dotenv

from script.coinDesk import default

load_dotenv()


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def send_to_kafka(producer, topic, key, partition, message):
    # Sending a message to Kafka
    # producer.produce(topic, key=key, partition=0, value=json.dumps(message).encode("utf-8"))
    producer.produce(topic, key=key, value=json.dumps(message).encode("utf-8"), callback=delivery_report)
    producer.flush()


def retrieve_real_time_data(producer, stock_symbol, kafka_topic):
    stock_symbols = stock_symbol.split(",") if stock_symbol else []
    length = len(stock_symbols)
    if not stock_symbols:
        print(f"No stock symbols provided in the environment variable.")
        exit(1)
    while True:
        # Fetch real-time data for the last 1 minute
        real_time_data = default(stock_symbol)
        for i in range(0, length):
            real_time_data_point = real_time_data[i]
            send_to_kafka(producer, kafka_topic, stock_symbol, stock_symbols[i], real_time_data_point)
        else:
            print("Market is closing")
        t.sleep(5)
