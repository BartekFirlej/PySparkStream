import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, current_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
from config.minio_config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, MINIO_OBJECT_KEY_PREFIX
from config.cassandra_config import CASSANDRA_PORT, CASSANDRA_KEYSPACE, CASSANDRA_HOST

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema definition for JSON data
schema = StructType([
    StructField("ProductId", IntegerType(), True),
    StructField("ProductName", StringType(), True),
    StructField("ProductTypeId", IntegerType(), True),
    StructField("ProductTypeName", StringType(), True),
    StructField("Price", FloatType(), True),
    StructField("CustomerId", IntegerType(), True),
    StructField("CustomerName", StringType(), True),
    StructField("CustomerLastName", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("AddressId", IntegerType(), True),
    StructField("Number", IntegerType(), True),
    StructField("CityId", IntegerType(), True),
    StructField("CityName", StringType(), True),
    StructField("VoivodeshipId", IntegerType(), True),
    StructField("PostalCode", StringType(), True),
    StructField("VoivodeshipName", StringType(), True),
    StructField("CountryId", IntegerType(), True),
    StructField("CountryName", StringType(), True)
])

# UDF to generate UUID
@udf(returnType=StringType())
def generate_uuid():
    return str(uuid.uuid4())

# Configure SparkSession
spark = SparkSession.builder \
    .appName("ProductViewsProcessor") \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
    .config("spark.cassandra.auth.username", os.getenv('CASSANDRA_USERNAME', '')) \
    .config("spark.cassandra.auth.password", os.getenv('CASSANDRA_PASSWORD', '')) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

# Read data from Minio
df = spark \
    .readStream \
    .format("text") \
    .load(f"s3a://{MINIO_BUCKET}/{MINIO_OBJECT_KEY_PREFIX}")

# Transform data from text format to JSON according to the schema
df_json = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Add additional columns: view_id and view_time
df_with_ids = df_json.withColumn("view_id", generate_uuid()) \
    .withColumn("view_time", current_timestamp())

# Select appropriate columns
df_selected = df_with_ids.select(
    col("view_id").cast("string").alias("view_id"),
    col("ProductId").alias("product_id"),
    col("ProductName").alias("product_name"),
    col("ProductTypeId").alias("product_type_id"),
    col("ProductTypeName").alias("product_type_name"),
    col("Price").alias("price"),
    col("CustomerId").alias("customer_id"),
    col("CustomerName").alias("customer_name"),
    col("CustomerLastName").alias("customer_last_name"),
    col("Email").alias("email"),
    col("Phone").alias("phone"),
    col("AddressId").alias("address_id"),
    col("Number").alias("number"),
    col("CityId").alias("city_id"),
    col("CityName").alias("city_name"),
    col("VoivodeshipId").alias("voivodeship_id"),
    col("PostalCode").alias("postal_code"),
    col("VoivodeshipName").alias("voivodeship_name"),
    col("CountryId").alias("country_id"),
    col("CountryName").alias("country_name"),
    col("view_time")
)

# Print the data read from Minio to console
query = df_selected.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Function to write data to Cassandra and update counters
def write_to_cassandra(batch_df, batch_id):
    session = None
    try:
        logger.info(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}")

        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)

        logger.info("Connection to Cassandra established")

        for row in batch_df.collect():
            logger.info(f"Processing row: {row}")

            # Insert view data into product_views table
            session.execute(
                """
                INSERT INTO product_views (
                    view_id, product_id, product_name, product_type_id, product_type_name, price,
                    customer_id, customer_name, customer_last_name, email, phone, address_id,
                    number, city_id, city_name, voivodeship_id, postal_code, voivodeship_name,
                    country_id, country_name, view_time
                ) VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row.product_id, row.product_name, row.product_type_id, row.product_type_name,
                    row.price, row.customer_id, row.customer_name, row.customer_last_name, row.email, row.phone,
                    row.address_id, row.number, row.city_id, row.city_name, row.voivodeship_id, row.postal_code,
                    row.voivodeship_name, row.country_id, row.country_name, row.view_time
                )
            )

            # Update view count in product_view_counts table
            session.execute(
                """
                UPDATE product_view_counts SET view_count = view_count + 1 WHERE product_id = %s
                """,
                (row.product_id,)
            )

            # Update view count in customer_view_counts table
            session.execute(
                """
                UPDATE customer_view_counts SET view_count = view_count + 1 WHERE customer_id = %s
                """,
                (row.customer_id,)
            )

        logger.info("Batch processed successfully")
    except Exception as e:
        logger.error(f"Error processing batch: {e}")
    finally:
        if session:
            session.shutdown()
            logger.info("Cassandra session closed")

# Stream data to Cassandra
df_selected.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("update") \
    .start() \
    .awaitTermination()

# Wait for the console printing to complete
query.awaitTermination()
