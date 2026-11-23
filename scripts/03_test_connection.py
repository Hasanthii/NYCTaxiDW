import os
from pyspark.sql import SparkSession

# 1. Read the login details from environment variables (no passwords in code)
db_server = os.environ["NYCTAXI_DB_SERVER"]
db_user = os.environ["NYCTAXI_DB_USER"]
db_password = os.environ["NYCTAXI_DB_PASSWORD"]

# 2. Start Spark and tell it where the JDBC driver file is
spark = SparkSession.builder \
    .appName("NYCTaxiTestConnection") \
    .master("local[*]") \
    .config("spark.jars", "file:///D:/NYCTaxiDW/drivers/mssql-jdbc-12.10.0.jre11.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 3. Build the connection address
jdbc_url = (
    f"jdbc:sqlserver://{db_server};"
    "databaseName=NYCTaxiDW;"
    "encrypt=true;trustServerCertificate=true"
)

# 4. Read one table from SQL Server into a DataFrame
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.dim_vendor") \
    .option("user", db_user) \
    .option("password", db_password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()

df.show()
print("Rows:", df.count())

spark.stop()