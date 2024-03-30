import time
from kafka import KafkaConsumer
from pyspark.sql import SparkSession


def compute_batch_layer(messages):

    # Creazione DataFrame Spark dai messaggi Kafka, split con ';'
    new_df = spark.createDataFrame([x.split(';') for x in messages],
                                   ['symbol', 'longName', 'currency', 'bid', 'ask', 'datainfo', 'timeinfo'])

    # Scrivi il DataFrame risultante nel file CSV esistente, facendo append
    hdfs_directory = "hdfs://localhost:9000/tmp/hadoop-tonylm/dfs/data/batch_view"
    print('Saving batch in HDFS directory: {}'.format(hdfs_directory))
    new_df.write.mode('append').csv(hdfs_directory)


def retrive_from_consumer():
    # Raccolta dei messaggi dal Kafka Consumer
    messages = []
    for message in consumer:
        messages.append(message.value.decode('utf-8'))
        print('Add in batch: ', message.value.decode('utf-8'))

        if len(messages) == 100:
            compute_batch_layer(messages)
            messages = []

    # save remaining message
    compute_batch_layer(messages)


def periodic_retrieve(interval):
    while True:
        retrive_from_consumer()
        time.sleep(interval)


if __name__ == "__main__":
    # inizializzazione di SparkSession
    spark = SparkSession.builder \
        .appName("jala-batch") \
        .config("spark.logConf", "true") \
        .config("spark.cores.max", 128) \
        .getOrCreate()

    # Configuring Kafka Consumer
    consumer = KafkaConsumer(bootstrap_servers='localhost:9092',
                             auto_offset_reset='latest',
                             group_id='jala-batch',
                             consumer_timeout_ms=10000)

    consumer.subscribe(['stocks-lambda'])

    # retrieve every 4 hours
    periodic_retrieve(5)
