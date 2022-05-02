from kafka import KafkaProducer
import json
import random
from time import sleep
from datetime import datetime
import time

# Create an instance of the Kafka producer

arquivo = open('words.txt', 'r').read()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                            value_serializer=lambda v: str(v).encode('utf-8'))

# Call the producer.send method with a producer-record
print("Ctrl+c to Stop")
while True:
    producer.send('topico', arquivo)
