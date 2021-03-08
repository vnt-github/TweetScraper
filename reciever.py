import boto3
import time
import os
import json
from os.path import join, dirname
from dotenv import load_dotenv

# Create .env file path.
dotenv_path = join(dirname(__file__), '.env')
# Load file from the path.
load_dotenv(dotenv_path)
# Create SQS client
sqs = boto3.client('sqs')
queue_url = os.getenv("QUEUE_URL")
cookie_path = os.getenv("COOKIE_PATH")

def update_cookie(value):
    with open(cookie_path, "w+") as f:
        f.write(value)

def handle_message(message):
    try:
        type = message["MessageAttributes"]["Type"]["StringValue"].lower()
        if type == 'cookie':
            update_cookie(message["Body"])
        elif type == 'scrapequery':
            # TODO:
            # https://stackoverflow.com/questions/13437402/how-to-run-scrapy-from-within-a-python-script
            print('call scrapper with time split query')
    except Exception as err:
        print(f"message:  {message} parsing err: {err}")

while(True):
    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=3
    )

    if 'Messages' in response:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        handle_message(message)
        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print('Received and deleted message: %s' % message["Body"])