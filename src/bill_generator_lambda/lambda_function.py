from datetime import datetime, timedelta
import json
import boto3
import logging
import base64
import time
import os
from boto3.dynamodb.conditions import Key, Attr
import pandas as pd
import fitz
from botocore.exceptions import ClientError

    
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def write_to_pdf(filename, text, end):
    doc =  fitz.open()
    page = doc.newPage()
    p = fitz.Point(50, 72)  # start point of 1st line
    rc = page.insertText(p,  # bottom-left of 1st char
                     text,  # the text (honors '\n')
                     fontname = "helv",  # the default font
                     fontsize = 11,  # the default font size
                     rotate = 0,  # also available: 90, 180, 270
                     )
    print("%i lines printed on page %i." % (rc, page.number))
    doc.save(filename)
    object_name = str(end) +"/"+filename.split('/')[-1]
    upload_file(filename, "cc-all-customer-bills", object_name)

def get_table(table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    return table

def upload_file(file_name, bucket, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # Upload the file
    s3_client = boto3.client('s3')

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    else:
        s3_resource = boto3.resource('s3')
        object_acl = s3_resource.ObjectAcl(bucket, object_name)
        response = object_acl.put(ACL='public-read')
    return True
    
def lambda_handler(event, context):
    text = ""
    os.environ["TZ"] = "America/New_York"
    time.tzset()
    end = datetime.now()
    start = end - timedelta(hours = 1)
    table = get_table('purchases')
    response = table.scan(
        FilterExpression=Attr('timestamp').between(str(start), str(end))
    )
    logger.debug("Results between %s and %s",start,end)
    itemsList = []
    result = {}
    if response['Items']:
        logger.debug(response['Items'])
        itemsList += response['Items']
        df = pd.DataFrame(itemsList)
        df['amount'] = df['amount'].astype(float)
        total_df = df.groupby(['account_id'])['amount'].sum().reset_index()
        for account_id in total_df['account_id']:
            text += "BILL FOR ACCOUNT: " + account_id + "\n\n"
            text += "FROM: " + str(start) + " to " + str(end) + "\n\n"
            temp = df[df['account_id']==account_id]
            amount = total_df[total_df['account_id']==account_id].iloc[0]['amount']
            text += "TOTAL AMOUNT: " + str(amount) + "\n\n"
            text += "PURCHASES: \n"
            count = 1
            for index, row in temp.iterrows():
                text += str(count) + ". Item: " + row['pos']['item'] + ", Amount: " + str(row['amount']) + "\n"
                text += "  Store details:" + "\n"
                text += "  " + row['pos']['store_name'] + "\n" + row['pos']['address'] + "\n\n"
                count += 1
            temp = temp.to_dict('r')
            result["account_"+account_id]={"account_id":account_id, "bill_amount": amount, "purchases" : temp}
            filename = "/tmp/" + account_id + ".pdf"
            write_to_pdf(filename, text, end)
            text = ""
        logger.debug(result)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
