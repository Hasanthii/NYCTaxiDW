import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

db_server = os.environ["NYCTAXI_DB_SERVER"]
db_user = os.environ["NYCTAXI_DB_USER"]
db_password = os.environ["NYCTAXI_DB_PASSWORD"]

spark = SparkSession.builder \
    .appName("LoadDimLocation") \
    .master("local[*]") \
    .config("spark.jars", "file:///D:/NYCTaxiDW/drivers/mssql-jdbc-12.10.0.jre11.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

jdbc_url = (
    f"jdbc:sqlserver://{db_server};"
    "databaseName=NYCTaxiDW;"
    "encrypt=true;trustServerCertificate=true"
)

# 1. Read the CSV (first line is the header)
zones = spark.read.csv(
    r"D:\NYCTaxiDW\data\reference\taxi_zone_lookup.csv",
    header=True
)

# 2. Rename columns to match dim_location, and make the key a number
dim_location = zones.select(
    F.col("LocationID").cast("int").alias("location_key"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone_name"),
    F.col("service_zone").alias("service_zone"),
)

# 3. Look at the data before loading it
print("Rows to load:", dim_location.count())
print("Placeholder zones (264 and 265):")
dim_location.filter(F.col("location_key") >= 264).show(truncate=False)

# 4. Write into SQL Server
dim_location.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.dim_location") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("append") \
    .save()

print("Load finished.")
spark.stop()