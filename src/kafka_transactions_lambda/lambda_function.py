import json
import boto3
import logging
import base64
from boto3.dynamodb.conditions import Key, Attr

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    return table
    
def lambda_handler(event, context):
    logger.debug(json.dumps(event))
    try:
        for key in event["records"]:
            for record in event["records"][key]:
                logger.debug(record['value'])
                message = base64.b64decode(record['value']).decode('utf-8')
                json_message = json.loads(message)
                logger.debug("Message received is: %s", json_message)
                table = get_table('kafka-transactions')
                table.put_item(Item = json_message)
    except Exception as e:
        logger.debug(e)
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
