import sys
from pyspark.sql import SparkSession
# you may add more import if you need to
from pyspark.sql.functions import col


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

hdfs_nn = "localhost"
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
#df.printSchema()

# filtering out rows with no reviews or rating < 1.0
filtered = df.filter(~(col("Number of Reviews") == 0) | (col("Rating") < 1.0))

filtered.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn))
#filtered.write.mode("overwrite").option("header",True).csv(output_path)

spark.stop()

