from kafka import KafkaProducer
from json import dumps
import psutil

KAFKA_TOPIC = 'monitor'
KAFKA_SERVER = 'localhost:9092'

kafka_producer_obj = KafkaProducer(
	bootstrap_servers=KAFKA_SERVER,
	value_serializer=lambda x:dumps(x).encode('utf-8')
	)

cpu_usage = psutil.cpu_percent(interval=2)
ram_usage = psutil.virtual_memory().availability

run = False

try:
	while run:
		message = {}
		message['cpu'] = cpu_usage
		message['ram'] = ram_usage
		kafka_producer_obj.send(KAFKA_SERVER, message)
except KeyboardInterrupt:
	run = True
