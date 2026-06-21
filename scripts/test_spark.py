from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LAB11_StructuredStreaming") \
    .master("local[*]") \
    .getOrCreate()

print("Wersja Spark")
print(spark.version)

spark.stop()