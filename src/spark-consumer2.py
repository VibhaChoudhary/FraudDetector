import os
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, to_json, col

table_name = "purchases"

def get_dynamodb():
    return boto3.resource('dynamodb',
                          aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
                          aws_secret_access_key=os.environ['AWS_SECRET_KEY'],
                          # aws_session_token=session_token,
                          region_name='us-east-1')

class SendToDynamoDB_ForeachWriter:
    '''
    Class to send a set of rows to DynamoDB.
    When used with `foreach`, copies of this class is going to be used to write
    multiple rows in the executor. See the python docs for `DataStreamWriter.foreach`
    for more details.
    '''

    def open(self, partition_id, epoch_id):
        # This is called first when preparing to send multiple rows.
        # Put all the initialization code inside open() so that a fresh
        # copy of this class is initialized in the executor where open()
        # will be called.
        self.dynamodb = get_dynamodb()
        return True

    def process(self, item):
        # This is called for each row after open() has been called.
        # This implementation sends one row at a time.
        # A more efficient implementation can be to send batches of rows at a time.
        print("Item", item)
        print("Item is", item['transaction'])

        if item['transaction']:
            self.dynamodb.Table(table_name).put_item(Item = item['transaction'].asDict())

    def close(self, err):
        # This is called after all the rows have been processed.
        if err:
            raise err


def main():
    topic = "ApprovedRequest"
    brokerAddresses = "172.31.3.55:9092"
    spark = SparkSession.builder.appName("ApprovedTransactionStreaming").getOrCreate()
    dsraw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", brokerAddresses) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()


    schema = StructType([
        StructField('account_id', StringType()),
        StructField('timestamp', StringType()),
        StructField('pos', MapType(StringType(), StringType())),
        StructField('amount', StringType()),
        StructField('location', MapType(StringType(), StringType())),
        StructField('type', StringType()),
        StructField('transaction_id', StringType())])

    ds1 = dsraw.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema) \
                .alias("transaction"))

    output = ds1.writeStream \
        .foreach(SendToDynamoDB_ForeachWriter()) \
        .outputMode("update") \
        .start()

    output.awaitTermination()

if __name__ == '__main__':
    main()



