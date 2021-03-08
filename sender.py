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
queue_url = os.getenv("QUEUE_URL")

# Send message to SQS queue
# response = sqs.send_message(
#     QueueUrl=queue_url,
#     DelaySeconds=10,
#     MessageAttributes={
#         'Type': {
#             'DataType': 'String',
#             'StringValue': 'ScrapeQuery'
#         }
#     },
#     MessageBody=('covid-19')
# )

response = sqs.send_message(
    QueueUrl=queue_url,
    DelaySeconds=10,
    MessageAttributes={
        'Type': {
            'DataType': 'String',
            'StringValue': 'Cookie'
        }
    },
    MessageBody=('1368860798689284100')
)

print(response['MessageId'])