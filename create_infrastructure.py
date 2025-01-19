import json
import boto3
import time

def create_infrastructure(config):
    # Initialize boto3 clients
    sqs = boto3.client('sqs')
    s3 = boto3.client('s3')

    # Create DLQ (Standard)
    dlq_response = sqs.create_queue(
        QueueName=config['dlq'],
        Attributes={
            'VisibilityTimeout': str(config['visibility_timeout']),
            'DelaySeconds': str(config['delivery_delay'])
        }
    )
    dlq_url = dlq_response['QueueUrl']
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']

    # Create main queue (Standard)
    queue_response = sqs.create_queue(
        QueueName=config['sqs_queue_name'],
        Attributes={
            'VisibilityTimeout': str(config['visibility_timeout']),
            'DelaySeconds': str(config['delivery_delay']),
            'RedrivePolicy': json.dumps({
                'deadLetterTargetArn': dlq_arn,
                'maxReceiveCount': '5'
            })
        }
    )
    queue_url = queue_response['QueueUrl']
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']

    # Set SQS policy to allow S3 to send messages
    policy = {
        "Version": "2012-10-17",
        "Id": "S3SendMessagePolicy",
        "Statement": [
            {
                "Sid": "AllowS3ToSendMessage",
                "Effect": "Allow",
                "Principal": {"Service": "s3.amazonaws.com"},
                "Action": "SQS:SendMessage",
                "Resource": queue_arn,
                "Condition": {
                    "ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{config['s3_bucket']}"}
                }
            }
        ]
    }
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            'Policy': json.dumps(policy)
        }
    )

    # Wait for a moment to ensure the policy is applied
    time.sleep(10)

    # Set up S3 event notification
    s3.put_bucket_notification_configuration(
        Bucket=config['s3_bucket'],
        NotificationConfiguration={
            'QueueConfigurations': [
                {
                    'QueueArn': queue_arn,
                    'Events': ['s3:ObjectCreated:*'],
                    'Filter': {
                        'Key': {
                            'FilterRules': [
                                {
                                    'Name': 'prefix',
                                    'Value': config['s3_prefix']
                                }
                            ]
                        }
                    }
                }
            ]
        }
    )

    print(f"Infrastructure created successfully.")
    print(f"Main Queue URL: {queue_url}")
    print(f"DLQ URL: {dlq_url}")

if __name__ == "__main__":
    config = {
        "sqs_queue_name": "data-queue",
        "dlq": "dlq_data_queue",
        "s3_bucket": "<BUCET>",
        "s3_prefix": "raw/",
        "event_type": "All object create events",
        "visibility_timeout": 30,
        "delivery_delay": 0
    }

    create_infrastructure(config)
