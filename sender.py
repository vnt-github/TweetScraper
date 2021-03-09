import boto3
import os
from os.path import join, dirname
from dotenv import load_dotenv

# Create .env file path.
dotenv_path = join(dirname(__file__), '.env')
# Load file from the path.
load_dotenv(dotenv_path)
# Create SQS client
sqs = boto3.client('sqs')
query_queue_url = os.getenv("QUERY_QUEUE_URL")
cookie_queue_url = os.getenv("COOKIE_QUEUE_URL")
days_to_scrape = int(os.getenv("DAYS_TO_SCRAPE"))
delimiter = os.getenv("DELIMITER")

def split_and_send(keyword):
    keyword = keyword.replace(delimiter, '')
    for i in range(1, days_to_scrape+1):
        # Send message to SQS queue
        query = delimiter.join([keyword, str(i), str(i+1)])
        response = sqs.send_message(
            QueueUrl=query_queue_url,
            MessageAttributes={
                'Type': {
                    'DataType': 'String',
                    'StringValue': 'ScrapeQuery'
                }
            },
            MessageBody=(query)
        )
        print(response['MessageId'])

def send_cookies(value):
    response = sqs.send_message(
        QueueUrl=cookie_queue_url,
        MessageAttributes={
            'Type': {
                'DataType': 'String',
                'StringValue': 'Cookie'
            }
        },
        MessageBody=(value)
    )
    
    print(response['MessageId'])

if __name__ == "__main__":
    split_and_send("")
    # send_cookies("")