from kafka import KafkaConsumer
from json import loads
import pandas as pd

KAFKA_GROUP = 'moniter_group'
KAFKA_TOPIC = 'moniter'
KAFKA_OUTPUT = 'moniter_output'
KAFKA_SERVER = 'localhost:9092'


consumer = KafkaConsumer(
	KAFKA_TOPIC,
	bootstrap_servers=KAFKA_SERVER,	
	auto_offset_reset='latest',
	enable_auto_commit=True,
	group_id=KAFKA_GROUP,
	value_serializer=lambda x: loads(x.decode('utf-8'))
	)

for message in consumer:
	print(message)