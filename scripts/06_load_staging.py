import os
from pyspark.sql import SparkSession

db_server = os.environ["NYCTAXI_DB_SERVER"]
db_user = os.environ["NYCTAXI_DB_USER"]
db_password = os.environ["NYCTAXI_DB_PASSWORD"]

CLEAN_PATH = r"D:\NYCTaxiDW\data\clean\yellow_tripdata_2024-01"

spark = SparkSession.builder \
    .appName("LoadStaging") \
    .master("local[*]") \
    .config("spark.jars", "file:///D:/NYCTaxiDW/drivers/mssql-jdbc-12.10.0.jre11.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

jdbc_url = (
    f"jdbc:sqlserver://{db_server};"
    "databaseName=NYCTaxiDW;"
    "encrypt=true;trustServerCertificate=true"
)

# 1. Read the clean data
clean = spark.read.parquet(CLEAN_PATH)
clean_count = clean.count()
print("Clean rows to load:", clean_count)

# 2. Write to staging. "overwrite" + "truncate" empties the table first,
#    but keeps the table itself (a plain overwrite would drop and recreate it)
clean.repartition(4).write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.stg_trip") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("batchsize", 50000) \
    .option("truncate", "true") \
    .mode("overwrite") \
    .save()

# 3. Validate: count what actually arrived in SQL Server
staged = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.stg_trip") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
staged_count = staged.count()
print("Rows in stg_trip:", staged_count)
print("Counts match?", staged_count == clean_count)

spark.stop()