import boto3
import json
import os
import time

def initialize_aws_clients():
    """
    Initializes and returns AWS SQS and S3 clients.
    Handles credential errors gracefully.
    """
    try:
        sns_client = boto3.client('sns', region_name='eu-west-2')
        sqs_client = boto3.client('sqs', region_name='eu-west-2')
        s3_client = boto3.client('s3', region_name='eu-west-2')
        return sns_client, sqs_client, s3_client
    except Exception as e:
        print("Error initializing AWS clients: %s" % e)
        print("Please configure your AWS credentials.")
        return None, None, None

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

def subscribe_queue_to_topic(sns_client, sqs_client, topic_arn, queue_arn, queue_url):
    """
    Subscribes an SQS queue to an SNS topic.
    """
    try:
        # Create a policy to allow SNS to send messages to the SQS queue
        policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "SNSTopicSendMessage",
                "Effect": "Allow",
                "Principal": "*",
                "Resource": queue_arn,
                "Action": "SQS:SendMessage",
                "Condition": {
                    "ArnEquals": {
                        "aws:SourceArn": topic_arn
                    }
                }
            }]
        }
        sqs_client.set_queue_attributes(QueueUrl=queue_url, Attributes={'Policy': json.dumps(policy)})
        subscription = sns_client.subscribe(TopicArn=topic_arn, Protocol='sqs', Endpoint=queue_arn)
        print("Successfully subscribed queue %s to topic %s." % (queue_arn, topic_arn))
        return subscription
    except Exception as e:
        print("Error subscribing queue to topic: %s" % e)
        return None

def main():
    """
    Main function to run the data pipeline.
    """
    # Configuration
    queue_name = 'met-office-weather-data-queue'
    topic_arn = 'arn:aws:sns:eu-west-2:021908831235:aws-earth-mo-atmospheric-ukv-prd'
    
    # Initialize AWS clients
    sns_client, sqs_client, s3_client = initialize_aws_clients()
    if not all([sns_client, sqs_client, s3_client]):
        return

    # Create SQS queue
    queue_url, queue_arn = create_sqs_queue(sqs_client, queue_name)
    if not all([queue_url, queue_arn]):
        return

    # Subscribe queue to SNS topic
    subscribe_queue_to_topic(sns_client, sqs_client, topic_arn, queue_arn, queue_url)

    print("
    while True:
        try:
            # Poll for messages
            print("Polling for messages...")
            time.sleep(10) # Avoid excessive polling
        except KeyboardInterrupt:
            print("
            break

if __name__ == '__main__':
    main()



