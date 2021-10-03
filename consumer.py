from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, col, count, desc, expr

# get or create a Spark session
spark = SparkSession.builder.appName(
    "streaming-lab").master("local[4]").getOrCreate()

# set the log level to WARN b/c the default INFO drowns out the console output below
spark.sparkContext.setLogLevel('WARN')

# define the common values
kafka_servers = "localhost:9092"
csv_root_path = "csv"
delta_root_path = "delta"
topic_name = "inbox.urls"

# define a streaming dataframe using the "inbox.urls" topic in kafka as it's source
inbox_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_servers).option(
    "subscribe", topic_name).option("startingOffsets", "earliest").load()

# stream values to the console
inbox_df.selectExpr("CAST(timestamp as TIMESTAMP)",
                    "CAST(value AS STRING)").writeStream.format("console").start()

# stream values to CSV
inbox_df.selectExpr("CAST(timestamp as TIMESTAMP)", "CAST(value AS STRING)").writeStream.format("csv").option(
    "checkpointLocation", f"{csv_root_path}/_checkpoints").option("path", f"{csv_root_path}/{topic_name}").outputMode("append").start()

# engineer some additional columns
df = inbox_df.selectExpr(
    "CAST(timestamp as TIMESTAMP) as timestamp", "TRIM(CAST(value as STRING)) as url")
df = df.withColumn("host", expr("parse_url(url, 'HOST')"))
df = df.withColumn("path", expr("parse_url(url, 'PATH')"))
df = df.withColumn("url_hash", sha2(col("url"), 256))
# and stream data to delta table
df.writeStream.format("delta").outputMode("append").option(
    "checkpointLocation", f"{delta_root_path}/{topic_name}/_checkpoints").start(f"{delta_root_path}/{topic_name}")

# count the occurrences of the host
count_hosts = df.groupBy("host").agg(
    (count("host").alias("count"))).sort(desc("count"))
# and write the results to the console
count_hosts.writeStream.format("console").outputMode("complete").start()

# wait for job interruption
spark.streams.awaitAnyTermination()
