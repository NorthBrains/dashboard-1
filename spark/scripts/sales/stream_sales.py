from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#Spark session initialize
spark = SparkSession \
    .builder \
    .appName("SalesDataStreaming") \
    .config("spark.cassandra.connection.host", "172.30.0.9") \
    .config("spark.cassandra.connection.port","9042")\
    .config("spark.cassandra.auth.username","cassandra")\
    .config("spark.cassandra.auth.password","cassandra")\
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Data Schema
sales_schema = StructType([
    StructField("Customer ID", IntegerType(), nullable=False),
    StructField("Product ID", IntegerType(), nullable=False),
    StructField("Age", IntegerType(), nullable=False),
    StructField("Gender", StringType(), nullable=False),
    StructField("Item Purchased", StringType(), nullable=False),
    StructField("Category", StringType(), nullable=False),
    StructField("Purchase Amount (USD)", DoubleType(), nullable=False),
    StructField("Location", StringType(), nullable=False),
    StructField("Size", StringType(), nullable=False),
    StructField("Color", StringType(), nullable=False),
    StructField("Season", StringType(), nullable=False),
    StructField("Review Rating", DoubleType(), nullable=False),
    StructField("Subscription Status", StringType(), nullable=False),
    StructField("Shipping Type", StringType(), nullable=False),
    StructField("Discount Applied", StringType(), nullable=False),
    StructField("Promo Code Used", StringType(), nullable=False),
    StructField("Previous Purchases", IntegerType(), nullable=False),
    StructField("Payment Method", StringType(), nullable=False),
    StructField("Frequency of Purchases", StringType(), nullable=False)
])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.30.0.6:9092, 172.30.0.7:9092, 172.30.0.8:9092") \
    .option("subscribe", "sales") \
    .option("startingOffsets", "earliest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = value_df.withColumn("data", F.from_json(F.col("value"), sales_schema)).select("data.*")

#add column record_id as an UUID
parsed_df = parsed_df.withColumn("record_id", F.expr("uuid()"))

#print to console
console_query = parsed_df \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

#Cassandra connect credentials
keyspace = "company_one"
table = "sales_data"

#save data into Cassandra
query = parsed_df \
    .writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", keyspace) \
    .option("table", table) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()