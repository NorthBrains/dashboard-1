from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

#Spark session initialize
spark = SparkSession \
    .builder \
    .appName("WarehouseDataStreaming") \
    .config("spark.cassandra.connection.host", "172.30.0.9") \
    .config("spark.cassandra.connection.port","9042")\
    .config("spark.cassandra.auth.username","cassandra")\
    .config("spark.cassandra.auth.password","cassandra")\
    .config("spark.driver.host", "localhost")\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Schema
warehouse_schema = StructType([
    StructField("Warehouse ID", IntegerType(), nullable=False),
    StructField("Product ID", IntegerType(), nullable=False),
    StructField("Product Name", StringType(), nullable=False),
    StructField("Category", StringType(), nullable=False),
    StructField("Quantity in Stock", DoubleType(), nullable=False),
    StructField("Location", StringType(), nullable=False),
    StructField("Supplier", StringType(), nullable=False),
    StructField("Restock Frequency", StringType(), nullable=False),
    StructField("Average Delivery Time (days)", DoubleType(), nullable=False),
    StructField("Stock Value (USD)", DoubleType(), nullable=False),
    StructField("Storage Temperature (C)", DoubleType(), nullable=False),
    StructField("Shelf Life (days)", DoubleType(), nullable=False),
    StructField("Hazardous Material", StringType(), nullable=False),
    StructField("Reorder Level", DoubleType(), nullable=False),
    StructField("Daily Sales (units)", DoubleType(), nullable=False),
    StructField("Daily Restock (units)", DoubleType(), nullable=False),
    StructField("Stock Level (units)", IntegerType(), nullable=False),
    StructField("Sales Forecast (units)", IntegerType(), nullable=False),
    StructField("Latitude", DoubleType(), nullable=False),
    StructField("Longitude", DoubleType(), nullable=False),
    StructField("State", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=True)
])

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.30.0.6:9092, 172.30.0.7:9092, 172.30.0.8:9092") \
    .option("subscribe", "warehouse") \
    .option("startingOffsets", "earliest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = value_df.withColumn("data", F.from_json(F.col("value"), warehouse_schema)).select("data.*")

#add column record_id as an UUID
parsed_df = parsed_df.withColumn("record_id", F.expr("uuid()"))

#print to console - as a check
console_query = parsed_df \
    .writeStream \
    .format("console") \
    .option("truncate", "false") \
    .start()

#Cassandra connect credentials
keyspace = "company_one"
table = "warehouse_data"

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