########## Import Necessary Libraries ##########
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, when
import boto3
import logging
import sys

########## Logging Setup ##########
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s]: %(message)s',
    handlers=[
        logging.FileHandler("data_pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

########## AWS Boto3 Setup ##########
#boto3.set_stream_logger(name='botocore', level=logging.DEBUG)
# AWS S3 client initialization
s3_client = boto3.client(
    's3',
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="us-east-2"
)
#print("client init")
# AWS SNS client initialization for notifications
sns_client = boto3.client(
    'sns',
    aws_access_key_id="",
    aws_secret_access_key="",
    region_name="us-east-2"
)

sns_topic_arn = "arn:aws:sns:us-east-2:047719656728:AutomationDataPipeline"

def send_notification(status, message):
    # Sends an SNS notification with the pipeline status
    #print("sending notif")
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=f"Data Pipeline Status: {status}",
            Message=message
        )
        logger.info(f"Notification sent: {status}")
    except Exception as e:
        logger.error(f"Failed to send notification: {e}")

########## Spark Session Setup ##########
try:
    conf = SparkConf()
    conf.set("spark.hadoop.fs.s3a.access.key", "AKIAQWHCQFUMCVFVOMGH")
    conf.set("spark.hadoop.fs.s3a.secret.key", "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")
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")

    spark = SparkSession.builder \
        .appName("Automation") \
        .config(conf=conf) \
        .getOrCreate()
    logger.info("Spark session created successfully.")
    #print("session set up")
except Exception as e:
    error_message = f"Error creating Spark session: {e}"
    logger.error(error_message)
    send_notification("Failure", error_message)
    sys.exit(1)

spark.sparkContext.setLogLevel("ERROR")

########## Data Ingestion ##########
s3_bucket_name = 'arnanand-superstore-bucket'
input_file_key = 'car_prices.csv'
try:
    s3_bucket_path = f"s3a://{s3_bucket_name}/{input_file_key}"
    df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)
    logger.info("Data successfully loaded from S3.")
except Exception as e:
    error_message = f"Error reading data from S3: {e}"
    logger.error(error_message)
    send_notification("Failure", error_message)
    sys.exit(1)

########## Data Transformations ##########
try:
    df = df.withColumn("sale_year", regexp_extract(col("saledate"), r"\b(\d{4})\b", 1).cast("int"))
    df = df.withColumn("vehicle_age", col("sale_year") - col("year"))
    df = df.drop("sale_year")

    df = df.withColumn("profit", col("sellingprice") - col("mmr"))

    #logger.info(f"Count of vehicle_age below 0: {df.filter(col('vehicle_age') < 0).count()}")
    #logger.info(f"Count of vehicle_age null: {df.filter(col('vehicle_age').isNull()).count()}")

    df = df.withColumn("vehicle_age", when(col("vehicle_age") < 0, 0).otherwise(col("vehicle_age"))) \
           .na.drop(subset=["vehicle_age"])
    df = df.na.drop()

    logger.info("Data transformations applied successfully.")
    #print("transformations done")
except Exception as e:
    error_message = f"Error during data transformations: {e}"
    logger.error(error_message)
    send_notification("Failure", error_message)
    sys.exit(1)
    

########## Back to S3 Storage ##########
output_file_key = "car_data_processed.csv"
try:
    output_path = f"s3a://{s3_bucket_name}/{output_file_key}"
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)

    logger.info(f"Data successfully written to {output_path}")

    # Validate processed file
    #s3_client.head_object(Bucket=s3_bucket_name, Key=output_file_key)
    logger.info(f"Processed file {output_path}")
    send_notification("Success", f"Pipeline completed successfully. Processed file available at: {output_file_key}")
except Exception as e:
    error_message = f"Error writing processed data to S3: {e}"
    logger.error(error_message)
    send_notification("Failure", error_message)
    sys.exit(1)

