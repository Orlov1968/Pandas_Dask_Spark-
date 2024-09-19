from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master('local[*]') \
    .appName('data_frames_spark') \
    .getOrCreate

spark.read.csv('5000 Sales Records.csv').show()

