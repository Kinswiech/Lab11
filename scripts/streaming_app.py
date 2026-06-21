from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, count, sum, window

spark = SparkSession.builder \
    .appName("LAB11_StructuredStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
])

df = spark.readStream \
    .schema(schema) \
    .option("header", True) \
    .csv("data/input_stream")

df = df.withColumn("event_time", to_timestamp(col("event_time")))

clean_df = df.filter(col("amount").isNotNull()) \
    .filter(col("status").isNotNull()) \
    .select("event_time", "user_id", "category", "amount", "status")

print("Czy DataFrame jest strumieniowy?")
print(clean_df.isStreaming)

print("Schemat danych:")
clean_df.printSchema()

summary = clean_df.filter(col("status") == "paid") \
    .groupBy("category") \
    .agg(
        count("*").alias("events_count"),
        sum("amount").alias("total_amount")
    )

window_summary = clean_df.withWatermark("event_time", "10 minutes") \
    .filter(col("status") == "paid") \
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("category")
    ) \
    .agg(
        count("*").alias("events_count"),
        sum("amount").alias("total_amount")
    )

console_query = summary.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", False) \
    .queryName("category_summary_console") \
    .start()

window_console_query = window_summary.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .queryName("window_summary_console") \
    .start()

file_query = window_summary.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("path", "data/output_stream") \
    .option("checkpointLocation", "checkpoints/lab11") \
    .option("header", True) \
    .start()

console_query.awaitTermination()
window_console_query.awaitTermination()
file_query.awaitTermination()