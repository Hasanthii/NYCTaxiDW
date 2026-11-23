import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from py4j.protocol import Py4JJavaError

# ---------------- Settings ----------------
RAW_DIR = r"D:\NYCTaxiDW\data\raw"
CLEAN_DIR = r"D:\NYCTaxiDW\data\clean"
REJECT_DIR = r"D:\NYCTaxiDW\data\rejected"
JDBC_JAR = "D:/NYCTaxiDW/drivers/mssql-jdbc-12.10.0.jre11.jar"
FILE_PATTERN = re.compile(r"^yellow_tripdata_(\d{4})-(\d{2})\.parquet$")

db_server = os.environ["NYCTAXI_DB_SERVER"]
db_user = os.environ["NYCTAXI_DB_USER"]
db_password = os.environ["NYCTAXI_DB_PASSWORD"]

jdbc_url = (
    f"jdbc:sqlserver://{db_server};"
    "databaseName=NYCTaxiDW;"
    "encrypt=true;trustServerCertificate=true"
)
jdbc_options = {
    "url": jdbc_url,
    "user": db_user,
    "password": db_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}


# ---------------- Helper functions ----------------
def read_table(spark, table_or_query):
    """Read a SQL Server table (or a subquery in brackets) into a DataFrame."""
    return spark.read.format("jdbc").options(**jdbc_options) \
        .option("dbtable", table_or_query).load()


def get_loaded_files(spark):
    """File names already loaded successfully (from etl_file_log)."""
    df = read_table(
        spark,
        "(SELECT file_name FROM dbo.etl_file_log WHERE status = 'SUCCESS') AS t")
    return {row["file_name"] for row in df.collect()}


def get_new_files(loaded):
    """Parquet files in the raw folder that are not loaded yet, oldest first."""
    all_files = sorted(f for f in os.listdir(RAW_DIR) if FILE_PATTERN.match(f))
    return [f for f in all_files if f not in loaded]


def clean_file(spark, file_name):
    """Apply the data quality rules. Returns counts and the clean data path."""
    match = FILE_PATTERN.match(file_name)
    year, month = int(match.group(1)), int(match.group(2))
    month_start = f"{year}-{month:02d}-01"
    next_month_start = (f"{year + 1}-01-01" if month == 12
                        else f"{year}-{month + 1:02d}-01")

    stem = file_name.replace(".parquet", "")
    clean_path = os.path.join(CLEAN_DIR, stem)
    reject_path = os.path.join(REJECT_DIR, stem)

    raw = spark.read.parquet(os.path.join(RAW_DIR, file_name))
    raw_count = raw.count()

    typed = raw.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.when(F.col("passenger_count") == 0, F.lit(None).cast("int"))
            .otherwise(F.col("passenger_count").cast("int")).alias("passenger_count"),
        F.col("trip_distance").cast("decimal(10,2)").alias("trip_distance"),
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

    typed = typed \
        .withColumn(
            "trip_duration_minutes",
            (F.expr("timestampdiff(SECOND, pickup_datetime, dropoff_datetime)") / 60)
                .cast("decimal(10,2)")) \
        .withColumn("pickup_hour", F.hour("pickup_datetime")) \
        .withColumn("source_file", F.lit(file_name))

    flagged = typed.withColumn(
        "reject_reason",
        F.when(F.col("dropoff_datetime") < F.col("pickup_datetime"), "dropoff before pickup")
         .when((F.col("pickup_datetime") < month_start) |
               (F.col("pickup_datetime") >= next_month_start), "pickup outside file month")
         .when(F.col("trip_distance") <= 0, "trip_distance <= 0")
         .when(F.col("fare_amount") <= 0, "fare_amount <= 0")
         .when(F.col("total_amount") <= 0, "total_amount <= 0")
    )

    flagged.filter(F.col("reject_reason").isNull()) \
        .drop("reject_reason").write.mode("overwrite").parquet(clean_path)
    flagged.filter(F.col("reject_reason").isNotNull()) \
        .write.mode("overwrite").parquet(reject_path)

    clean_count = spark.read.parquet(clean_path).count()
    reject_count = spark.read.parquet(reject_path).count()
    if clean_count + reject_count != raw_count:
        raise ValueError(
            f"Row counts do not add up: {clean_count} + {reject_count} != {raw_count}")

    return raw_count, clean_count, reject_count, clean_path


def load_staging(spark, clean_path, clean_count):
    """Replace the contents of stg_trip with the clean rows, then verify."""
    spark.read.parquet(clean_path).repartition(4).write.format("jdbc") \
        .options(**jdbc_options) \
        .option("dbtable", "dbo.stg_trip") \
        .option("batchsize", 50000) \
        .option("truncate", "true") \
        .mode("overwrite") \
        .save()

    staged_count = read_table(spark, "dbo.stg_trip").count()
    if staged_count != clean_count:
        raise ValueError(
            f"Staging has {staged_count} rows but expected {clean_count}")


def open_connection(spark):
    jvm = spark._sc._gateway.jvm
    return jvm.java.sql.DriverManager.getConnection(jdbc_url, db_user, db_password)


def call_load_procedure(spark, file_name, raw_count, clean_count, reject_count):
    """Run usp_load_fact_trip (checks, fact load and log row, in one transaction)."""
    conn = open_connection(spark)
    try:
        stmt = conn.prepareCall("{call dbo.usp_load_fact_trip(?, ?, ?, ?)}")
        stmt.setString(1, file_name)
        stmt.setInt(2, raw_count)
        stmt.setInt(3, clean_count)
        stmt.setInt(4, reject_count)
        stmt.execute()
    finally:
        conn.close()


def error_text(error):
    if isinstance(error, Py4JJavaError):
        return str(error.java_exception.getMessage())
    return str(error)


def log_failure(spark, file_name, message):
    """Record a FAILED row in etl_file_log (never overwrites a SUCCESS row)."""
    conn = open_connection(spark)
    try:
        stmt = conn.prepareStatement(
            "DELETE FROM dbo.etl_file_log WHERE file_name = ? AND status <> 'SUCCESS'; "
            "INSERT INTO dbo.etl_file_log (file_name, status, error_message) "
            "VALUES (?, 'FAILED', ?)")
        stmt.setString(1, file_name)
        stmt.setString(2, file_name)
        stmt.setString(3, message[:500])
        stmt.execute()
    finally:
        conn.close()


# ---------------- Main pipeline ----------------
def main():
    spark = SparkSession.builder \
        .appName("NYCTaxiPipeline") \
        .master("local[*]") \
        .config("spark.driver.extraClassPath", JDBC_JAR) \
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    loaded = get_loaded_files(spark)
    new_files = get_new_files(loaded)
    print("Already loaded:", sorted(loaded))
    print("New files to process:", new_files)

    results = []
    for file_name in new_files:
        print(f"\n=== Processing {file_name} ===")
        try:
            raw_c, clean_c, rej_c, clean_path = clean_file(spark, file_name)
            print(f"Raw {raw_c} | Clean {clean_c} | Rejected {rej_c}")
            load_staging(spark, clean_path, clean_c)
            print("Staging loaded and verified")
            call_load_procedure(spark, file_name, raw_c, clean_c, rej_c)
            print("Fact table loaded")
            results.append((file_name, "SUCCESS"))
        except Exception as e:
            message = error_text(e)
            print("FAILED:", message)
            log_failure(spark, file_name, message)
            results.append((file_name, "FAILED"))

    print("\n=== Summary ===")
    if not results:
        print("No new files. Nothing to do.")
    for file_name, status in results:
        print(f"{file_name}: {status}")

    spark.stop()


if __name__ == "__main__":
    main()