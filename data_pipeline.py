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
