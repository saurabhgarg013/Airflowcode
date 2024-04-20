from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Word Count") \
    .getOrCreate()

# Create DataFrame from text file
lines = spark.read.text("s3://emrbucket13/input/abc.txt")

# Perform word count
word_counts = lines.selectExpr("explode(split(value, ' ')) as word") \
                   .groupBy("word") \
                   .count()

# Write output to S3
word_counts.write.mode("overwrite").csv("s3://emrbucket13/output/word_count")

spark.stop()
