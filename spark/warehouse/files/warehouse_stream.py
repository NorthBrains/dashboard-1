from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# Spark session initialize
spark = SparkSession \
    .builder \
    .appName("WarehouseDataStreaming") \
    .config("spark.cassandra.connection.host", "172.30.0.11") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

warehouse_schema = StructType([
    StructField("warehouse_id", IntegerType(), nullable=False),
    StructField("product_id", IntegerType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("quantity_in_stock", FloatType(), nullable=False),
    StructField("location", StringType(), nullable=False),
    StructField("supplier", StringType(), nullable=False),
    StructField("restock_frequency", StringType(), nullable=False),
    StructField("average_delivery_time", FloatType(), nullable=False),
    StructField("stock_value", FloatType(), nullable=False),
    StructField("storage_temperature", FloatType(), nullable=False),
    StructField("shelf_life", FloatType(), nullable=False),
    StructField("hazardous_material", StringType(), nullable=False),
    StructField("reorder_level", FloatType(), nullable=False),
    StructField("daily_sales", FloatType(), nullable=False),
    StructField("daily_restock", FloatType(), nullable=False),
    StructField("stock_level", IntegerType(), nullable=False),
    StructField("sales_forecast", IntegerType(), nullable=False),
    StructField("latitude", FloatType(), nullable=False),
    StructField("longitude", FloatType(), nullable=False),
    StructField("state", StringType(), nullable=False),
    StructField("timestamp", StringType(), nullable=False)
])


# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.30.0.8:9092,172.30.0.9:9092,172.30.0.10:9092") \
    .option("subscribe", "warehouse") \
    .option("startingOffsets", "earliest") \
    .load()

# Deserialize JSON data using the defined schema
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", F.from_json(F.col("value"), warehouse_schema)) \
    .select("data.*")

# Print to console for debugging
# console_query = parsed_df \
#     .writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# Cassandra connect credentials
keyspace = "company_one"
table = "warehouse_data"

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
