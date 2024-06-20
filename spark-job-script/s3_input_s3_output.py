from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('web_logs').getOrCreate()

s3_file_path = "s3://web-logs-bucket-monthly/"

web_logs_df = spark.read.option("header","true").option("inferSchema","true").csv(s3_file_path)

web_logs_df_renaming = web_logs_df.withColumnRenamed('IP','IP_address')

web_logs_df_renaming.printSchema()
web_logs_df_renaming.show()

output_path = "s3://web-logs-processed-bucket/output/"
web_logs_df_renaming.write.csv(output_path, header = True)


# Stop the Spark session
spark.stop()

