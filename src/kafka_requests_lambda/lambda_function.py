import json
import logging
import base64
from kafka import KafkaProducer

bootstrap_server = '172.31.3.55:9092'
# Initialize producer variable and set parameter for JSON encode
producer = KafkaProducer(bootstrap_servers = bootstrap_server,value_serializer=lambda v: json.dumps(v).encode('utf-8'))

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def is_fraud(transaction):
    if transaction['amount'] > 400:
        return True
    else:
        return False
        
def lambda_handler(event, context):
    # TODO implement
    logger.debug(json.dumps(event))
    try:
        for key in event["records"]:
            for record in event["records"][key]:
                logger.debug(record['value'])
                message = base64.b64decode(record['value']).decode('utf-8')
                logger.debug("Message received is: %s", message)
                transaction = json.loads(message)
                logger.debug(transaction)
                logger.debug("%s", transaction['account_id'])
                response = {
                    "transaction_id": transaction['transaction_id'],
                    "account_id": transaction['account_id'],
                    "decision" : "approve"
                }
                logger.debug(response)
                if is_fraud(transaction) == True:
                    response['decision'] = "deny"
                producer.send('RequestDecision', response)
                producer.flush()
    except Exception as e:
        logger.debug(e)
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
