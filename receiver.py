import boto3
import time
import os
import json
import sys
from os.path import join, dirname
from dotenv import load_dotenv
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Create .env file path.
dotenv_path = join(dirname(__file__), '.env')
# Load file from the path.
load_dotenv(dotenv_path)
# Create SQS client
sqs = boto3.client('sqs')
query_queue_url = os.getenv("QUERY_QUEUE_URL")
cookie_queue_url = os.getenv("COOKIE_QUEUE_URL")
cookie_path = os.getenv("COOKIE_PATH")

def update_cookie(value):
    with open(cookie_path, "w+") as f:
        f.write(value)

def start_scraping(query):
    process = CrawlerProcess(get_project_settings())
    process.crawl('TweetScraper', query)
    process.start()

def delete_message(message, queue):
    receipt_handle = message['ReceiptHandle']
    # Delete received message from queue
    sqs.delete_message(
        QueueUrl=queue,
        ReceiptHandle=receipt_handle
    )

def handle_message(message):
    try:
        type = message["MessageAttributes"]["Type"]["StringValue"].lower()
        if type == 'cookie':
            delete_message(message, cookie_queue_url)
            update_cookie(message["Body"])
        elif type == 'scrapequery':
            delete_message(message, query_queue_url)
            start_scraping(message["Body"])
    except Exception as err:
        print(f"message:  {message} parsing err: {err}")



def receive_cookies():
    print('listening for cookies events')
    while(True):
        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=cookie_queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            message = response['Messages'][0]
            print(f'Received message: {message["Body"]}')
            handle_message(message)

def receive_queries():
    print('listening for query events')
    while(True):
        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=query_queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=30,
            WaitTimeSeconds=20
        )

        if 'Messages' in response:
            message = response['Messages'][0]
            print(f'Received message: {message["Body"]}')
            handle_message(message)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f'please provide one of scraping or cookies listener type')
    else:
        listener_type = sys.argv[1]
        if listener_type == 'scraping':
            receive_queries()
        elif listener_type == 'cookies':
            receive_cookies()
        else:
            print(f'invalid listener_type: {listener_type}')
            print(f'please provide one of scraping or cookies listener type')
