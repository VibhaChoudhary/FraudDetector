from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer
import json

# Task configuration.
topic = "Transaction"
brokerAddresses = "172.31.3.55:9092"
batchTime = 10

producer = KafkaProducer(bootstrap_servers = [brokerAddresses])

def requestApproval(iter):
    records = iter.collect()
    for record in records:
        print(record)
        producer.send('RequestApproval', record.encode('utf-8'))
        producer.flush()

def main():
    # Creating stream.
    spark = SparkSession.builder.appName("PythonHeartbeatStreaming").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc, batchTime)
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokerAddresses})

    # The streaming can be handled as an usual RDD object.
    # for example: kvs.map(lambda l : l.lower())
    values = kvs.map(lambda x: x[1])
    values.foreachRDD(requestApproval)
    # Starting the task run.
    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()

