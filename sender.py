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
query_queue_url = os.getenv("QUERY_query_queue_url")
cookie_queue_url = os.getenv("COOKIE_QUEUE_URL")

# Send message to SQS queue
# response = sqs.send_message(
#     QueueUrl=query_queue_url,
#     MessageAttributes={
#         'Type': {
#             'DataType': 'String',
#             'StringValue': 'ScrapeQuery'
#         }
#     },
#     MessageBody=('covid-19')
# )

response = sqs.send_message(
    QueueUrl=cookie_queue_url,
    MessageAttributes={
        'Type': {
            'DataType': 'String',
            'StringValue': 'Cookie'
        }
    },
    MessageBody=('1369106427134930944')
)

print(response['MessageId'])