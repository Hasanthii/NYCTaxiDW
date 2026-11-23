from pyspark.sql import SparkSession

# 1. Start Spark (local mode = uses your own computer)
spark = SparkSession.builder \
    .appName("NYCTaxiExplore") \
    .master("local[*]") \
    .getOrCreate()

# 2. Read the Parquet file into a DataFrame
df = spark.read.parquet(r"D:\NYCTaxiDW\data\raw\yellow_tripdata_2024-01.parquet")

# 3. Look at the structure and size
df.printSchema()
print("Row count:", df.count())

# 4. Show the first 5 rows
df.show(5)

spark.stop()