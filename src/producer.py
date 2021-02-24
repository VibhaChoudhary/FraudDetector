import json
import random
from itertools import permutations
import datetime
import time
from kafka import KafkaProducer
import os

brokerAddresses = "172.31.3.55:9092"
topic = "Transaction"
producer = KafkaProducer(
    bootstrap_servers=[brokerAddresses],
    # Encode all values as JSON
    value_serializer=lambda value: json.dumps(value).encode(),
)

stores = [{"address": "100 West 72 St, New York, NY", "item": "Watch", "store_name": "Next Gen watches"},
          {"address": "100 Bedford, OH", "item": "Car", "store_name": "Car store"},
          {"address": "150 West New York, NY", "item": "Apparel", "store_name": "Apparel store"}]
locations = [{"lat": "40.601008", "lon": "-74.091464"},
             {"lat": "41.392502", "lon": "-81.534447"},
             {"lat": "40.748249", "lon": "-73.992212"}]

acc_ids = []

def main():
    count = 0
    os.environ["TZ"] = "America/New_York"
    time.tzset()
    acc_ids = []
    for perm in list(permutations(['1', '2', '3', '4', '5'])):
        acc_ids.append("9876543" + "".join(perm))
    while True:
        index = random.choice([0, 1, 2])
        acc_id = random.choice(acc_ids)
        val = "-t".join([acc_id[i:i + 3] for i in range(0, len(acc_id), 3)])
        trans_id = "TRN" + val + "-" + str(time.time()).replace('.', '-')
        transaction = {
            "account_id": random.choice(acc_ids),
            "timestamp": str(datetime.datetime.now()),
            "type": random.choice(["VISA", "Mastercard", "Amex", "Discover"]),
            "amount": random.randrange(200, 500, 5),
            "location": locations[index],
            "transaction_id": trans_id,
            "pos": stores[index]
        }
        print(json.dumps(transaction))
        producer.send(topic, transaction)
        producer.flush()
        count = count + 1

if __name__ == '__main__':
    main()
