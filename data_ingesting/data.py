import yfinance as yf
import pandas as pd
from datetime import datetime
import time
from kafka import KafkaProducer
from kafka.errors import TopicAlreadyExistsError
from confluent_kafka.admin import AdminClient, NewTopic
from requests import ReadTimeout

topic_name = 'stocks-lambda'


def get_yf_data():
    data = pd.read_csv("../data/tickers_sp500.csv")
    symbols = data['Symbol']
    for symbol in symbols:
        record = ''
        stock = yf.Ticker(symbol)
        record += stock.info['symbol']
        record += ';'
        try:
            record += stock.info['longName']
            record += ';'
            record += str(stock.info['currency'])
            record += ';'
            record += str(stock.info['bid'])
            record += ';'
            record += str(stock.info['ask'])
            record += ';'
        except KeyError as e:
            print('Key error: ', e)
            continue

        record += str((datetime.now().strftime('%d/%m/%Y')))
        record += ';'
        record += str((datetime.now().strftime('%H:%M:%S')))
        print(record)

        producer.send(topic_name, str.encode(record))


def periodic_update(interval):
    while True:
        get_yf_data()
        time.sleep(interval)


def create_topics():
    new_topic_list = []
    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })

    new_topic_list.append(NewTopic(topic_name, num_partitions=3, replication_factor=3))

    try:
        admin_client.create_topics(new_topic_list)
        print("topic created successfully")
    except TopicAlreadyExistsError as e:
        print("topic already exists: ", e)
        admin_client.delete_topics(new_topic_list)
        print("topic deleted successfully")
        admin_client.create_topics(new_topic_list)
        print("topic created successfully")


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    create_topics()
    interval = 5
    try:
        periodic_update(interval)  # simulate real-time data
    except ReadTimeout as e:
        print('Read timeout, re-run periodic update')
        periodic_update(interval)
