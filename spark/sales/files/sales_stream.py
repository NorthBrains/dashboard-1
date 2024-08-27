from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Spark session initialize
spark = SparkSession \
    .builder \
    .appName("SalesDataStreaming") \
    .config("spark.cassandra.connection.host", "172.30.0.11") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define the Spark StructType schema (corresponding to the structure of the JSON data)
sales_schema = StructType([
    StructField("customer_id", IntegerType(), nullable=False),
    StructField("age", IntegerType(), nullable=False),
    StructField("gender", StringType(), nullable=False),
    StructField("item_purchased", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("purchase_amount", FloatType(), nullable=False),
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
    StructField("frequency_of_purchases", StringType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False)
])

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.30.0.8:9092,172.30.0.9:9092,172.30.0.10:9092") \
    .option("subscribe", "sales") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data using the defined schema
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", F.from_json(F.col("value"), sales_schema)) \
    .select("data.*")

# Print to console for debugging
# console_query = parsed_df \
#     .writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# Cassandra connect credentials
keyspace = "company_one"
table = "sales_data"

# Save data into Cassandra
query = parsed_df \
    .writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", keyspace) \
    .option("table", table) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
