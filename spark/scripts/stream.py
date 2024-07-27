from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#Spark session initialize
spark = SparkSession \
    .builder \
    .appName("KafkaToCassandra") \
    .config("spark.cassandra.connection.host", "172.30.0.6") \
    .config("spark.cassandra.connection.port","9042")\
    .config("spark.cassandra.auth.username","cassandra")\
    .config("spark.cassandra.auth.password","cassandra")\
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Data Schema
schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("gender", StringType(), nullable=False),
    StructField("item_purchased", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("purchase_amount_usd", FloatType(), nullable=False),
    StructField("location", StringType(), nullable=False),
    StructField("size", StringType(), nullable=False),
    StructField("color", StringType(), nullable=False),
    StructField("season", StringType(), nullable=False),
    StructField("review_rating", FloatType(), nullable=False),
    StructField("subscription_status", StringType(), nullable=False),
    StructField("shipping_type", StringType(), nullable=False),
    StructField("discount_applied", StringType(), nullable=False),
    StructField("promo_code_used", StringType(), nullable=False),
    StructField("previous_purchases", IntegerType(), nullable=False),
    StructField("payment_method", StringType(), nullable=False),
    StructField("frequency_of_purchases", StringType(), nullable=False)
])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.30.0.5:9092") \
    .option("subscribe", "products_data") \
    .option("startingOffsets", "earliest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = value_df.withColumn("data", F.from_json(F.col("value"), flat_schema)).select("data.*")

#add column record_id as an UUID
parsed_df = parsed_df.withColumn("record_id", F.expr("uuid()"))

#print to console
console_query = parsed_df \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

#Cassandra connect credentials
keyspace = "products_data"
table = "data"

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