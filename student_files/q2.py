import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, last

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()

# Read input data
df = spark.read.option("header",True).csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# Filter to remove null values and convert Rating to float for proper comparison
filtered_restaurant_data = df.filter(df["Price Range"].isNotNull() & df["Rating"].isNotNull())
filtered_restaurant_data = filtered_restaurant_data.withColumn("Rating", filtered_restaurant_data["Rating"].cast("float"))

# Sort by City, Price Range and Rating
sorted_restaurant_data = filtered_restaurant_data.sort(["City", "Price Range", "Rating"], ascending=[1, 1, 0])

# Group by City and Price Range, and get the first (best) and last (worst) restaurant in each group
grouped_restaurant_data = sorted_restaurant_data.groupBy("City", "Price Range")\
    .agg(first("_c0").alias("Best Restaurant ID"), last("_c0").alias("Worst Restaurant ID"))

# Join with original DataFrame to get all columns for best and worst restaurants
best_restaurants_data = grouped_restaurant_data.join(sorted_restaurant_data, (grouped_restaurant_data["City"] == sorted_restaurant_data["City"]) & (grouped_restaurant_data["Price Range"] == sorted_restaurant_data["Price Range"]) & (grouped_restaurant_data["Best Restaurant ID"] == sorted_restaurant_data["_c0"]), 'left_outer')
best_restaurants_data = best_restaurants_data.drop(sorted_restaurant_data["City"]).drop(sorted_restaurant_data["Price Range"])

worst_restaurants_data = grouped_restaurant_data.join(sorted_restaurant_data, (grouped_restaurant_data["City"] == sorted_restaurant_data["City"]) & (grouped_restaurant_data["Price Range"] == sorted_restaurant_data["Price Range"]) & (grouped_restaurant_data["Worst Restaurant ID"] == sorted_restaurant_data["_c0"]), 'left_outer')
worst_restaurants_data = worst_restaurants_data.drop(sorted_restaurant_data["City"]).drop(sorted_restaurant_data["Price Range"])

# Union best and worst restaurants dataframes
final_restaurant_data = best_restaurants_data.union(worst_restaurants_data)

# Reorder the columns
final_restaurant_data = final_restaurant_data.select("_c0", "Name", "City", "Cuisine Style", "Ranking", "Rating", "Price Range", "Number of Reviews", "Reviews", "URL_TA", "ID_TA")

final_restaurant_data.write.option("header",True).csv("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn))

spark.stop()