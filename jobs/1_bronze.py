from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Bronze").getOrCreate()
base = "/opt/bitnami/spark/data"

spark.read.json(f"{base}/source/otto.jsonl").write.mode("overwrite").parquet(f"{base}/lake/bronze/otto")
spark.read.json(f"{base}/source/amazon.jsonl").write.mode("overwrite").parquet(f"{base}/lake/bronze/amazon")

print(">>> BRONZE OK")
spark.stop()