from pyspark.sql import SparkSession
from pyspark.sql.function import *
from pyspark.sql.type import *


KAFKA_TOPIC = 'moniter'
KAFKA_OUTPUT = 'moniter_output'
KAFKA_SERVER = 'localhost:9092'

spark = SparkSession \
		.builder \
		.appName() \
		.master('local[*]') \
		.getOrCreate()

spark.SparkContext.setLogLevel('ERROR')

df1 = spark \
		.readStream \
		.format('kafka') \
		.option('kafka.bootstrap.servers', KAFKA_SERVER) \
		.option('subscribe', KAFKA_TOPIC) \
		.option('startingoffsets', 'latest') \
		.load()

print('****************************************')
df.printSchema()

df2 = df1.selectExpr('CAST(value AS STRING)')

df_schema = StructType() \
			.add('moniter_cpu_usage', StringType()) \
			.add('moniter_ram_usage', StringType()) \

df3 = df2.select('moniter.*')

df3.printSchema()

moniter_write_stream1 = df3 \
						.writeStream \
						.trigger(processingTime='1 seconds') \
						.outputMode('update') \
						.option('truncate', 'false') \
						.format('console') \
						.start()

moniter_write_stream2 = df3 \
						.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)') \
						.writeStream \
						.format('kafka') \
						.option('kafka.bootstrap.servers', KAFKA_SERVER) \
						.option('topic', KAFKA_OUTPUT) \
						.trigger(processingTime='1 seconds') \
						.outputMode('update') \
						.option('checkpointLocation', '/pycheckpoint/')
						.start()

moniter_write_stream1.awaitTermination()

print('Complete.........')