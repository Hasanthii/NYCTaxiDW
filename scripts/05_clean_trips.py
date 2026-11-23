from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---- Settings (we'll make these dynamic when we do incremental loading) ----
SOURCE_FILE = "yellow_tripdata_2024-01.parquet"
RAW_PATH = rf"D:\NYCTaxiDW\data\raw\{SOURCE_FILE}"
CLEAN_PATH = r"D:\NYCTaxiDW\data\clean\yellow_tripdata_2024-01"
REJECT_PATH = r"D:\NYCTaxiDW\data\rejected\yellow_tripdata_2024-01"

# The month this file should contain, taken from the file name
year = int(SOURCE_FILE[16:20])
month = int(SOURCE_FILE[21:23])
month_start = f"{year}-{month:02d}-01"
next_month_start = f"{year + 1}-01-01" if month == 12 else f"{year}-{month + 1:02d}-01"

spark = SparkSession.builder \
    .appName("CleanTrips") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 1. Read raw data
raw = spark.read.parquet(RAW_PATH)
total = raw.count()

# 2. Rename columns, fix data types, apply the "fix" rules, add new columns
typed = raw.select(
    F.col("VendorID").cast("int").alias("vendor_id"),
    F.col("tpep_pickup_datetime").alias("pickup_datetime"),
    F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
    # Fix: passenger_count of 0 becomes NULL
    F.when(F.col("passenger_count") == 0, F.lit(None).cast("int"))
        .otherwise(F.col("passenger_count").cast("int")).alias("passenger_count"),
    F.col("trip_distance").cast("decimal(10,2)").alias("trip_distance"),
    # Fix: missing rate code becomes 99 (unknown)
    F.coalesce(F.col("RatecodeID").cast("int"), F.lit(99)).alias("rate_code_id"),
    F.col("PULocationID").cast("int").alias("pickup_location_id"),
    F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
    F.col("payment_type").cast("int").alias("payment_type_id"),
    F.col("fare_amount").cast("decimal(10,2)").alias("fare_amount"),
    F.col("extra").cast("decimal(10,2)").alias("extra"),
    F.col("mta_tax").cast("decimal(10,2)").alias("mta_tax"),
    F.col("tip_amount").cast("decimal(10,2)").alias("tip_amount"),
    F.col("tolls_amount").cast("decimal(10,2)").alias("tolls_amount"),
    F.col("improvement_surcharge").cast("decimal(10,2)").alias("improvement_surcharge"),
    F.col("congestion_surcharge").cast("decimal(10,2)").alias("congestion_surcharge"),
    F.col("Airport_fee").cast("decimal(10,2)").alias("airport_fee"),
    F.col("total_amount").cast("decimal(10,2)").alias("total_amount"),
)

# 3. Add the calculated columns
typed = typed \
    .withColumn(
        "trip_duration_minutes",
        (F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / 60)
            .cast("decimal(10,2)")) \
    .withColumn("pickup_hour", F.hour("pickup_datetime")) \
    .withColumn("source_file", F.lit(SOURCE_FILE))

# 4. Give every bad row a reason (the first rule that matches wins)
flagged = typed.withColumn(
    "reject_reason",
    F.when(F.col("dropoff_datetime") < F.col("pickup_datetime"), "dropoff before pickup")
     .when((F.col("pickup_datetime") < month_start) |
           (F.col("pickup_datetime") >= next_month_start), "pickup outside file month")
     .when(F.col("trip_distance") <= 0, "trip_distance <= 0")
     .when(F.col("fare_amount") <= 0, "fare_amount <= 0")
     .when(F.col("total_amount") <= 0, "total_amount <= 0")
)

# 5. Split into clean and rejected
clean = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
rejected = flagged.filter(F.col("reject_reason").isNotNull())

# 6. Save both
clean.write.mode("overwrite").parquet(CLEAN_PATH)
rejected.write.mode("overwrite").parquet(REJECT_PATH)

# 7. Validate: do the numbers add up?
clean_count = spark.read.parquet(CLEAN_PATH).count()
reject_count = spark.read.parquet(REJECT_PATH).count()
print("Raw rows:      ", total)
print("Clean rows:    ", clean_count)
print("Rejected rows: ", reject_count)
print("Clean + rejected = raw?", clean_count + reject_count == total)

print("\nRejected rows by reason:")
spark.read.parquet(REJECT_PATH).groupBy("reject_reason").count().orderBy(F.desc("count")).show(truncate=False)

spark.stop()