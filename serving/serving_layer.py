import json
import os
import time
from hdfs import InsecureClient
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# cartella non esiste

# MongoDB's configuration - Speed Layer
mongo_client = MongoClient('localhost', 27017)
db = mongo_client['jala']
collection = db['stocks']

# HDFS' configuration - Batch Layer
hdfs_client = InsecureClient('http://localhost:9870')
batch_directory = "/tmp/hadoop-tonylm/dfs/data/batch_view"

cols = ['symbol', 'longName', 'currency', 'bid', 'ask', 'datainfo', 'timeinfo']


def batch_to_json(df):
    df.columns = cols
    # Converti l'intero DataFrame in JSON
    json_data = df.to_json(orient='records')

    # Salva il JSON risultante in un file
    with open("batch_to_json/batch_data.json", "w") as f:
        f.write(json_data)

    merge_batch_json_into_db()


def merge_batch_json_into_db():
    with open('batch_to_json/batch_data.json') as f:
        file_data = json.load(f)
    try:
        collection.insert_many(file_data)
        print('ok')
    except BulkWriteError as e:
        print('Duplicate key')

    os.remove('batch_to_json/batch_data.json')


def batch_indexing():
    hdfs_client.delete(f"{batch_directory}/_SUCCESS")

    file_list = hdfs_client.list(batch_directory)
    if len(file_list) == 0:
        return

    dfs = []
    for file_name in file_list:
        if file_name == '_temporary':
            time.sleep(5)
            continue
        else:
            if file_name == '_SUCCESS':
                continue

        with hdfs_client.read(f"{batch_directory}/{file_name}") as file:
            df = pd.read_csv(file, sep=',', names=cols)
            dfs.append(df)
            hdfs_client.delete(f"{batch_directory}/{file_name}")

    try:
        merged_df = pd.concat(dfs)
        batch_to_json(merged_df)
    except ValueError as e:
        # ValueError: No objects to concatenate
        print(e)
        main()


def main():
    while True:
        batch_indexing()
        time.sleep(10)


if __name__ == '__main__':
    main()
