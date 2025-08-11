########## Data Ingestion #############

from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging

logging.getLogger("py4j").setLevel(logging.ERROR)

# Set up the Spark configuration for AWS S3 access
conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.access.key", "AKIAQWHCQFUMCVFVOMGh")
conf.set("spark.hadoop.fs.s3a.secret.key", "XLB7u/7Sv75OA8wHEN7gWbACZy/4jTHshX13uXU2")
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
conf.set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
spark = SparkSession.builder \
    .appName("S3DataIngestion") \
    .config(conf=conf) \
    .getOrCreate()
print("Session Created")

spark.sparkContext.setLogLevel("ERROR")

s3_bucket_path = "s3a://arnanand-superstore-bucket/car_prices.csv"
df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)
df.show()

############## Data Transformations ##############

from pyspark.sql.functions import regexp_extract, col, lit

# extracting the year from saledate using regex
df = df.withColumn("sale_year", regexp_extract(col("saledate"), r"\b(\d{4})\b", 1).cast("int"))
df = df.withColumn("vehicle_age", col("sale_year") - col("year"))
df = df.drop("sale_year")

df = df.withColumn("profit", col("sellingprice") - col("mmr"))

print(f"Count of vehicle_age below 0: {df.filter(col('vehicle_age') < 0).count()}")
print(f"Count of vehicle_age null: {df.filter(col('vehicle_age').isNull()).count()}")

from pyspark.sql.functions import when

# convert negatives to 0 and remove nulls
df = df.withColumn("vehicle_age", when(col("vehicle_age") < 0, 0).otherwise(col("vehicle_age"))) \
       .na.drop(subset=["vehicle_age"])
df.show()

df = df.na.drop()


############ Data Aggregations ############

#  top 10 makes based on the average selling price

from pyspark.sql.functions import avg, round

avg_selling_price = df.groupBy("make").agg(round(avg("sellingprice"), 2).alias("avg_selling_price"))
avg_selling_price = avg_selling_price.orderBy("avg_selling_price", ascending=False).limit(10)

print("1) Top 10 makes based on the average selling price")
avg_selling_price.show()

# top 10 total profit by vehicle

from pyspark.sql.functions import sum

total_profit = df.groupBy("make") \
    .agg(sum("profit").alias("total_profit")) \
    .orderBy("total_profit", ascending=False) \
    .limit(10)

print("2) Top 10 total profit by vehicle")
total_profit.show()

# Number of Vehicles Sold by Condition

from pyspark.sql.functions import count

vehicles_by_condition = df.groupBy("condition").agg(count("*").alias("vehicles_sold"))
print("3) Number of Vehicles Sold by Condition")
vehicles_by_condition.show()

# Average Vehicle Age by Body Type

from pyspark.sql.functions import avg, round

avg_vehicle_age = df.groupBy("body").agg(round(avg("vehicle_age"), 2).alias("avg_vehicle_age"))

print("4) Average Vehicle Age by Body Type")
avg_vehicle_age.show()


# Maximum Profit for Each Seller

from pyspark.sql.functions import max

max_profit_per_seller = df.groupBy("seller").agg(max("profit").alias("max_profit"))
print("5) Maximum Profit for Each Seller")
max_profit_per_seller.show()


################ Back to S3 Storage ################

output_path = "s3a://arnanand-superstore-bucket/car_data_processed.csv"
df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"Data successfully written to {output_path}")


