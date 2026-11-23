from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("NYCTaxiProfile") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(r"D:\NYCTaxiDW\data\raw\yellow_tripdata_2024-01.parquet")
total = df.count()
print("Total rows:", total)

# Each check is a name plus a condition that describes a "bad" row
checks = {
    "dropoff before pickup":
        F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
    "pickup outside Jan 2024":
        (F.col("tpep_pickup_datetime") < "2024-01-01") |
        (F.col("tpep_pickup_datetime") >= "2024-02-01"),
    "trip_distance <= 0":
        F.col("trip_distance") <= 0,
    "fare_amount <= 0":
        F.col("fare_amount") <= 0,
    "total_amount <= 0":
        F.col("total_amount") <= 0,
    "passenger_count null or 0":
        F.col("passenger_count").isNull() | (F.col("passenger_count") == 0),
    "pickup location not 1-265":
        ~F.col("PULocationID").between(1, 265),
    "dropoff location not 1-265":
        ~F.col("DOLocationID").between(1, 265),
}

for name, condition in checks.items():
    bad = df.filter(condition).count()
    print(f"{name}: {bad} rows ({bad / total:.2%})")

print("\nNull counts per column:")
df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
]).show(vertical=True)

print("Distinct payment_type values:")
df.groupBy("payment_type").count().orderBy("payment_type").show()

print("Distinct RatecodeID values:")
df.groupBy("RatecodeID").count().orderBy("RatecodeID").show()

print("Distinct VendorID values:")
df.groupBy("VendorID").count().orderBy("VendorID").show()

spark.stop()