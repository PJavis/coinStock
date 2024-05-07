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
    if not stock_symbols:
        print(f"No stock symbols provided in the environment variable.")
        exit(1)
    while True:
        # Fetch real-time data for the last 1 minute
        is_market_open_bool = True
        if is_market_open_bool:
            real_time_data = default(stock_symbols)
            for symbol_index, stock_symbol in enumerate(stock_symbols):
                if not real_time_data.empty:
                    real_time_data_point = real_time_data[symbol_index]
                    send_to_kafka(producer, kafka_topic, stock_symbol, symbol_index, real_time_data_point)
        else:
            print("Market is closing")
        t.sleep(5)
