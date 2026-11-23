import os
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

db_server = os.environ["NYCTAXI_DB_SERVER"]
db_user = os.environ["NYCTAXI_DB_USER"]
db_password = os.environ["NYCTAXI_DB_PASSWORD"]

# This time the driver goes on Spark's classpath (not spark.jars),
# so Java's DriverManager can find it
spark = SparkSession.builder \
    .appName("TestCallProc") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath",
            "D:/NYCTaxiDW/drivers/mssql-jdbc-12.10.0.jre11.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

jdbc_url = (
    f"jdbc:sqlserver://{db_server};"
    "databaseName=NYCTaxiDW;"
    "encrypt=true;trustServerCertificate=true"
)

# Open a plain Java connection through Spark's JVM
jvm = spark._sc._gateway.jvm
conn = jvm.java.sql.DriverManager.getConnection(jdbc_url, db_user, db_password)

try:
    stmt = conn.prepareCall("{call dbo.usp_load_fact_trip(?, ?, ?, ?)}")
    stmt.setString(1, "yellow_tripdata_2024-01.parquet")
    stmt.setInt(2, 2964624)
    stmt.setInt(3, 2869641)
    stmt.setInt(4, 94983)
    stmt.execute()
    print("Procedure finished without error")
except Py4JJavaError as e:
    print("Procedure raised an error:")
    print(e.java_exception.getMessage())
finally:
    conn.close()
    spark.stop()