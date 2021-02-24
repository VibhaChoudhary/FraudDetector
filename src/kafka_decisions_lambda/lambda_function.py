import json
import boto3
import logging
import base64
from elasticsearch import Elasticsearch, RequestsHttpConnection
from kafka import KafkaProducer
import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
bootstrap_server = '172.31.3.55:9092'
producer = KafkaProducer(bootstrap_servers = bootstrap_server)



def get_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    return table

def upload_to_es(doc):
    """Uploads transaction details to elastic search
    Args:
       json_obj (json): transaction data.
    """
    host = u'search-transactions-t6gkmzx5znjsqzk7qurizjnkxa.us-east-1.es.amazonaws.com'
    key = doc['transaction_id']
    es = Elasticsearch(
        hosts=[{u'host': host, u'port': 443}],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        request_timeout=30
    )
    logger.debug(doc)
    res = es.index(index='transactions', doc_type='transaction', id = key, body=doc)
    logger.debug(res)


def send_to_kafka(transaction):
    # Initialize producer variable and set parameter for JSON encode
    logger.debug("Received transaction %s", transaction)
    producer.send('ApprovedRequest', transaction)
    producer.flush()

def lambda_handler(event, context):
    # TODO implement
    logger.debug(json.dumps(event))
    try:
        for key in event["records"]:
            for record in event["records"][key]:
                logger.debug(record['value'])
                message = base64.b64decode(record['value']).decode('utf-8')
                decision = json.loads(message)
                logger.debug("Message received is: %s", decision)
                table = get_table('kafka-transactions')
                try:
                    response = table.get_item(Key={'transaction_id': decision['transaction_id']})
                except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
                    logger.error('Record with transaction_id %s not found' % decision['transaction_id'])
                else:
                    response['Item']['decision'] = decision['decision']
                    upload_to_es(response['Item'])
                    if decision['decision'] == "approve":
                        # send_to_kafka(response['Item'])
                        logger.debug("Approved transaction: %s", response['Item'])
                        trans = {"account_id": str(response['Item']['account_id']),
                        "timestamp": str(response['Item']['timestamp']),
                        "pos": {"item": str(response['Item']['pos']['item']), "store_name": str(response['Item']['pos']['store_name']), "address": str(response['Item']['pos']['address'])},
                        "amount": str(response['Item']['amount']), "location": {"lat": response['Item']['location']['lat'], "lon":response['Item']['location']['lon']},
                        "type":str(response['Item']['type']),
                        "transaction_id": str(response['Item']['transaction_id'])}
                        producer.send('ApprovedRequest', json.dumps(trans).encode('utf-8'))
                        producer.flush()
    except Exception as e:
        logger.debug(e)
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
