from pyspark.sql import SparkSession
from pyspark import SparkConf
import logging

logging.getLogger("py4j").setLevel(logging.ERROR)

conf = SparkConf()
conf.set("spark.hadoop.fs.s3a.access.key", "AKIAQWHCQFUMCVFVOMGh")
conf.set("spark.hadoop.fs.s3a.secret.key", "XLB7u/7Sv75OA8wHEN7gWbACZy/4jTHshX13uXU2")
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
conf.set("fs.s3a.aws.credentials.provider","com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0")
spark = SparkSession.builder \
    .appName("SparkSQLQueries") \
    .config(conf=conf) \
    .getOrCreate()
print("Session Created")

spark.sparkContext.setLogLevel("ERROR")

s3_bucket_path = "s3a://arnanand-superstore-bucket/car_prices_processed.csv"
df = spark.read.csv(s3_bucket_path, header=True, inferSchema=True)

df.createOrReplaceTempView("car_data")

# 1. Total number of cars sold per make
print("1. Total number of cars sold per make\n")
spark.sql("""
    SELECT make, COUNT(*) AS total_cars_sold
    FROM car_data
    GROUP BY make
    ORDER BY total_cars_sold DESC
    LIMIT 10
""").show()

# 2. Average selling price by car condition
print("2. Average selling price by car condition\n")
spark.sql("""
    SELECT condition, ROUND(AVG(sellingprice), 2) AS avg_selling_price
    FROM car_data
    GROUP BY condition
    ORDER BY avg_selling_price DESC
""").show()

# 3. Top 5 makes with the highest average profit
print("3. Top 5 makes with the highest average profit\n")
spark.sql("""
    SELECT make, ROUND(AVG(profit), 2) AS avg_profit
    FROM car_data
    GROUP BY make
    ORDER BY avg_profit DESC
    LIMIT 5
""").show()

# 4. Number of vehicles sold by body type
print("4. Number of vehicles sold by body type\n")
spark.sql("""
    SELECT body, COUNT(*) AS vehicles_sold
    FROM car_data
    GROUP BY body
    ORDER BY vehicles_sold DESC
""").show()

# 5. Maximum selling price for each year
print("5. Maximum selling price for each year\n")
spark.sql("""
    SELECT year, MAX(sellingprice) AS max_selling_price
    FROM car_data
    GROUP BY year
    ORDER BY year ASC
""").show()
