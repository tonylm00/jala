from pymongo import MongoClient
from datetime import datetime
from kafka import KafkaConsumer

# MongoDB configuration
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['jala']
collection = db['stocks']


def process_message(msg):
    try:
        # Process message
        data = msg.value.decode('utf-8').split(';')
        symbol = data[0]

        # Example processing: Just print for demonstration
        print(f"Received data for symbol {symbol} at {datetime.now().strftime('%H:%M:%S')}")

        # Example: Insert data into MongoDB
        document = {
            'symbol': symbol,
            'longName': data[1],
            'currency': data[2],
            'bid': float(data[3]),
            'ask': float(data[4]),
            'datainfo': data[5],
            'timeinfo': data[6]
        }
        collection.insert_one(document)
    except Exception as e:
        print(f"Error processing message: {e}")


def consume_messages():
    while True:
        try:
            for message in consumer:
                process_message(message)
        except Exception as e:
            print(f"Error consuming message: {e}")


if __name__ == '__main__':
    # Kafka consumer
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='latest',
                             group_id='jala-speed',
                             consumer_timeout_ms=10000)

    consumer.subscribe(['stocks-lambda'])

    # Consume messages from input topic
    consume_messages()

    # Clean up
    consumer.close()
    mongo_client.close()
