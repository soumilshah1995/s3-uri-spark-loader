import time
import logging
import boto3
import os, sys, json
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger('ETL_Process')


class Poller:
    def __init__(self, queue_url, wait_time_seconds, batch_size):
        self.queue_url = queue_url
        self.sqs_client = boto3.client('sqs')
        self.batch_size = batch_size
        self.wait_time_seconds = wait_time_seconds
        self.messages_to_delete = []

    def get_messages(self):
        logger.info("Polling messages from SQS queue.")
        response = self.sqs_client.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=self.batch_size,
            WaitTimeSeconds=self.wait_time_seconds
        )

        if 'Messages' in response:
            messages = response['Messages']
            for message in messages:
                self.messages_to_delete.append({
                    'ReceiptHandle': message['ReceiptHandle'],
                    'Body': message['Body']
                })
            logger.info(f"Retrieved {len(messages)} messages from the queue.")
            return messages
        else:
            logger.info("No messages retrieved from the queue.")
            return []

    def commit(self):
        for message in self.messages_to_delete:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
        logger.info(f"Deleted {len(self.messages_to_delete)} messages from the queue.")
        self.messages_to_delete = []


def read_files_spark_df(spark, files, input_config):
    file_format = input_config.get("format")
    if file_format == "csv":
        options = input_config.get("csv_options", {})
        df = spark.read.options(**options).csv(files)
    elif file_format == "parquet":
        options = input_config.get("parquet_options", {})
        df = spark.read.options(**options).parquet(files)
    elif file_format == "json":
        options = input_config.get("json_options", {})
        df = spark.read.options(**options).json(files)
    else:
        raise ValueError(f"Unsupported format: {file_format}. Supported formats are 'csv', 'parquet', and 'json'.")

    return df


def create_spark_session(spark_config, protocol="s3"):
    builder = SparkSession.builder

    if protocol == "s3a":
        default = {
            "spark.hadoop.fs.s3a.access.key": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "spark.sql.catalog.dev.s3.endpoint": "https://s3.amazonaws.com",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        }
        for key, value in default.items():
            builder = builder.config(key, value)

    for key, value in spark_config.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()


def process_message(messages, input_config, spark):
    """
    Processes the retrieved messages and logs key metrics.
    """
    start_time = time.time()  # Start timing the processing
    batch_files = []
    protocol = input_config.get("protocol", "s3")
    message_count = len(messages)

    for message in messages:
        payload = json.loads(message['Body'])
        records = payload.get('Records', [])
        batch_files.extend([
            f"{'s3a' if protocol == 's3a' else 's3'}://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}"
            for record in records
        ])

    if batch_files:
        logger.info(f"Processing batch of {message_count} messages.")
        df = read_files_spark_df(
            spark=spark, files=batch_files, input_config=input_config
        )

        print(df.printSchema())
        print(df.show())

        logger.info(f"Files in this batch: {batch_files}")
        print(f"Processing messages: {batch_files}")
    else:
        logger.warning("No files to process in the current batch.")

    end_time = time.time()  # End timing the processing
    elapsed_time = end_time - start_time
    logger.info(f"Batch processing completed. Time taken: {elapsed_time:.2f} seconds.")
    logger.info(f"Number of messages processed: {message_count}")


def main():
    event = {
        "spark": {
            "spark.app.name": "hudi",
            "spark.jars.packages": "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.773",
            "spark.serializer":"org.apache.spark.serializer.KryoSerializer",
            "spark.sql.extensions": 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension',
        },
        "input_config": {
            "protocol": "s3a",
            "queue_url": "<URL>",
            "poll_interval": 5,
            "wait_time_seconds": 5,
            "batch_size": 10,
            "format":'csv',
            "prune_messages_once_processed":True,
            "csv_options": {
                "sep": "\t",
                "header": "true",
                "inferSchema": "true"
            }
        }
    }

    # Extract configuration values
    queue_url = event['input_config']['queue_url']
    wait_time_seconds = event['input_config']['wait_time_seconds']
    batch_size = event['input_config']['batch_size']
    poll_interval = event['input_config']['poll_interval']
    prune_messages_once_processed = event['input_config'].get('prune_messages_once_processed', True)

    # Create Poller instance with extracted values
    poller = Poller(queue_url, wait_time_seconds, batch_size)

    spark = create_spark_session(
        event.get("spark"),
        protocol=event.get("input_config").get("protocol", "s3")
    )

    while True:
        try:
            # Poll messages from SQS queue
            messages = poller.get_messages()

            if not messages:
                logger.info("No messages to process.")
            else:
                # Process the retrieved messages
                process_message(messages, input_config=event.get("input_config"), spark=spark)

                # Commit (delete) the messages after processing
                if prune_messages_once_processed: poller.commit()

            logger.info(f"Waiting {poll_interval} seconds before the next poll.")
            time.sleep(poll_interval)  # Wait before the next poll

        except Exception as e:
            logger.error(f"Error in polling loop: {str(e)}")
            time.sleep(poll_interval)  # Wait before retrying in case of an error


if __name__ == "__main__":
    main()
