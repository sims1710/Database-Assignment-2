import sys
from pyspark.sql import SparkSession

# you may add more import if you need to
from pyspark.sql.functions import col, avg, when, lit


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# Convert Rating column to float
df = df.withColumn("Rating", df["Rating"].cast("float"))

# Calculate average rating per city
city_avg_rating = df.groupBy("City").agg(avg("Rating").alias("AverageRating"))

# Determine if city is in top or bottom rating group
city_avg_rating = city_avg_rating.withColumn("RatingGroup", when(col("AverageRating") >= 4, "Top").otherwise("Bottom"))

# Sort by AverageRating in descending order
city_avg_rating = city_avg_rating.orderBy(col("AverageRating").desc())

# Extract top 3 and bottom 3 cities
top_cities = city_avg_rating.limit(3)
bottom_cities = city_avg_rating.orderBy(col("AverageRating")).limit(3)
sorted_bottom_cities = bottom_cities.orderBy(col("AverageRating").desc())

# Combine top and bottom cities
result = top_cities.union(sorted_bottom_cities)

# Write result to CSV file
result.coalesce(1).write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn))

spark.stop()