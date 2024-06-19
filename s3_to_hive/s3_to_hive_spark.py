from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('web_logs').enableHiveSupport().getOrCreate()

s3_file_path = "s3://web-logs-bucket-monthly/"

hive_table_name = "logs_report.web_logs_monthly"

web_logs_df = spark.read.option("header","true").option("inferSchema","true").csv(s3_file_path)

web_logs_df_renaming = web_logs_df.withColumnRenmaed('IP','IP_address')

web_logs_df_renaming.printSchema()
web_logs_df_renaming.show()

# Create Hive database
spark.sql("CREATE DATABASE IF NOT EXISTS logs_report")
print(f"Database Created Successfully !!")

# Create Hive internal table with the specified schema
spark.sql("""
CREATE TABLE IF NOT EXISTS logs_report.web_logs_monthly (
    IP_address STRING,
    user_identifier STRING,
    user_auth STRING,
    date STRING,
    method STRING,
    url STRING,
    protocol STRING,
    status STRING,
    size INT,
) STORED AS PARQUET
""")
          
print(f"Table Created Successfully !!")

# Ingest DataFrame into the Hive table
web_logs_df_renaming.write.mode("overwrite").insertInto("logs_report.web_logs_monthly")

print(f"Data ingested successfully into {hive_table_name}")

# Stop the Spark session
spark.stop()

