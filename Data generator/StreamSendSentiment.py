from kafka import KafkaProducer
from kafka.errors import KafkaError
import csv
import json
import time


KAFKA_TOPIC_NAME_CONS = "topicA"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"


producer = KafkaProducer(bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS_CONS],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
def kafka_producer(data):
    try:
       future = producer.send(KAFKA_TOPIC_NAME_CONS, data)
       producer.flush()
       record_metadata = future.get(timeout=10)
       print(record_metadata)
    except KafkaError as e:
       print(e)

with open("tweets.csv") as f:
    fcsv = csv.reader(f)
    for row in fcsv:
        kafka_producer(row)
        time.sleep(5)
        print(row)