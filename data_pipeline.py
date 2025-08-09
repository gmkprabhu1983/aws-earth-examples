import boto3
import json
import os

def initialize_aws_clients():
    """
    Initializes and returns AWS SQS and S3 clients.
    Handles credential errors gracefully.
    """
    try:
        sqs_client = boto3.client('sqs', region_name='eu-west-2')
        s3_client = boto3.client('s3', region_name='eu-west-2')
        return sqs_client, s3_client
    except Exception as e:
        print("Error initializing AWS clients: %s" % e)
        print("Please configure your AWS credentials.")
        return None, None

def create_sqs_queue(sqs_client, queue_name):
    """
    Creates an SQS queue if it doesn't already exist.
    Returns the queue URL and ARN.
    """
    try:
        response = sqs_client.create_queue(QueueName=queue_name)
        queue_url = response['QueueUrl']
        queue_arn = sqs_client.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=['QueueArn']
        )['Attributes']['QueueArn']
        return queue_url, queue_arn
    except Exception as e:
        print("Error creating SQS queue: %s" % e)
        return None, None

