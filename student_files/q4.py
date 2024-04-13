import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import col, explode, split, trim, count, regexp_replace

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# Remove square brackets and extra spaces from Cuisine Style
df_stripped = df.withColumn("Cuisine Style", regexp_replace(col("Cuisine Style"), "[\[\]']", ""))

# Split Cuisine Style column into separate rows
df_exploded = df_stripped.withColumn("Cuisine Style", explode(split(trim(col("Cuisine Style")), ", ")))

# Group by City and Cuisine Style and count the number of restaurants
restaurant_count = df_exploded.groupBy("City", "Cuisine Style").agg(count("*").alias("count"))

# Write the output to CSV files
restaurant_count.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn))

spark.stop()

