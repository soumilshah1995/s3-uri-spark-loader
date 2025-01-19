# S3 URI Spark Loader

A scalable solution for incrementally processing files from Amazon S3 into your Lakehouse architecture. This template leverages S3 events, SQS queues, and Spark to efficiently process new files with minimal cost and overhead. It integrates seamlessly with AWS Lambda, Glue, EMR, and Spark, handling new file uploads, updates, and deletions.

![Architecture Overview](https://github.com/user-attachments/assets/b8c03116-c0df-43a3-80c1-b4b959605e0b)

## Overview

This repository provides a simple and effective way to ingest data from S3 into your data lakehouse architecture. The process is incremental, ensuring that only new files are processed. The solution consists of the following steps:

1. Set up S3 event notifications to detect new file uploads.
2. Configure an SQS queue to buffer event messages.
3. Poll the SQS queue with a Python consumer to retrieve the S3 URIs of new files.
4. Use Spark to process these files as DataFrames.
5. Write the processed data to your desired table format, such as Iceberg, Hudi, or Delta Lake.

## Benefits

- **Cost-Efficient**: Avoid frequent `list()` calls on S3, reducing operational overhead.
- **Scalable**: The use of S3 events and SQS queues allows for seamless handling of large data volumes.
- **Error Handling**: Failed events are routed to a Dead Letter Queue (DLQ) for reprocessing and debugging.
- **Flexibility**: Works with multiple compute frameworks like AWS Lambda, Glue, EMR on EC2/EKS, or EMR Serverless.

## Steps to Set Up

### Step 1: Setup SQS with S3 Events

Begin by setting up S3 event notifications to detect new file uploads. Use the provided Python script to create the necessary infrastructure.

```bash
python3 create_infrastructure.py
```

Step 2: Run the Consumer
Once the infrastructure is set up, you can configure the consumer to poll the SQS queue and process the incoming messages. The consumer is configured via the following JSON configuration:
```
{
    "spark": {
        "spark.app.name": "hudi",
        "spark.jars.packages": "org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.773",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    },
    "input_config": {
        "protocol": "s3a",
        "queue_url": "<URL>",
        "poll_interval": 5,
        "wait_time_seconds": 5,
        "batch_size": 10,
        "format": "csv",
        "prune_messages_once_processed": true,
        "csv_options": {
            "sep": "\t",
            "header": "true",
            "inferSchema": "true"
        }
    }
}

```
You can adjust the queue_url, poll_interval, and other parameters based on your needs.

Step 3: Run the Python Consumer
After configuring the settings, run the Python consumer to start processing the incoming SQS events.
```
python3 template_consumer.py

```

NOTE : Table Format Configurations
The processed data can be written to different table formats, such as Iceberg, Hudi, or Delta Lake. Below are the configurations for each:

Iceberg (AWS Managed)
```
{
  "spark": {
    "spark.app.name": "iceberg_lab",
    "spark.jars.packages": "com.amazonaws:aws-java-sdk-bundle:1.12.661,org.apache.hadoop:hadoop-aws:3.3.4,software.amazon.awssdk:bundle:2.29.38,com.github.ben-manes.caffeine:caffeine:3.1.8,org.apache.commons:commons-configuration2:2.11.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.3,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.6.1",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.defaultCatalog": "s3tablesbucket",
    "spark.sql.catalog.s3tablesbucket": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.s3tablesbucket.catalog-impl": "software.amazon.s3tables.iceberg.S3TablesCatalog",
    "spark.sql.catalog.s3tablesbucket.warehouse": "arn:aws:s3tables:us-east-1:867098943567:bucket/iceberg-awsmanaged-tables",
    "spark.sql.catalog.s3tablesbucket.client.region": "us-east-1"
  }
}

```

Iceberg (Unmanaged)
```
{
  "spark": {
    "spark.app.name": "iceberg_lab",
    "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0,software.amazon.awssdk:bundle:2.20.160,software.amazon.awssdk:url-connection-client:2.20.160,org.apache.hadoop:hadoop-aws:3.3.4",
    "spark.sql.catalog.dev": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.dev.warehouse": "s3a://soumil-dev-bucket-1995/warehouse/",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.dev.type": "hadoop"
  }
}

```

Hudi
```
{
  "spark": {
    "spark.app.name": "hudi",
    "spark.jars.packages": "org.apache.hudi:hudi-spark{SPARK_VERSION}-bundle_2.12:0.14.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.773",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.sql.catalog.s3tablesbucket.client.region": "us-east-1",
    "spark.sql.catalog.dev.s3.endpoint": "https://s3.amazonaws.com",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
  }
}

```

Conclusion

This repository provides an easy-to-use template for processing S3 files incrementally in your Lakehouse architecture. With minimal setup and configuration, you can start processing data efficiently, leveraging the full power of AWS and Spark.

