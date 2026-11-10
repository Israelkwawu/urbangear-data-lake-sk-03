"""Lab 02: revenue by category over all raw days, run on the Spark cluster."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab02-agg").getOrCreate()   # settings come from spark-submit
raw = spark.read.json("s3a://urbangear-data-lake/raw/orders/")    # all dt= folders at once
(raw.groupBy("category")
    .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("revenue"),
         F.count("*").alias("lines"))
    .orderBy(F.desc("revenue"))
    .show())
input("Open http://localhost:8081 -> Running Applications -> lab02-agg -> Application Detail UI. Press Enter to finish.")
spark.stop()
