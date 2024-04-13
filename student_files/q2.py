import sys
from pyspark.sql import SparkSession

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

# Read input data
df = spark.read.option("header",True).csv("/TA_restaurants_curated_cleaned.csv")

# Filter to remove null values
filtered_df = df.filter(df["Price Range"].isNotNull())

# Group by Price Range and City
grouped_by_price_city = filtered_df.groupBy("Price Range", "City")

# Calculate maximum and minimum rating for each group
max_rating = grouped_by_price_city.agg(max("Rating").alias("Max Rating"))
min_rating = grouped_by_price_city.agg(min("Rating").alias("Min Rating"))

# Join the two dataframes
result = max_rating.join(min_rating, ["City", "Price Range"])

# Find best restaurants
best_restaurants = result.filter(result["Max Rating"] == result["Min Rating"])

# Find worst restaurants
worst_restaurants = result.filter(result["Max Rating"] != result["Min Rating"])

# Union best and worst restaurants dataframes
final_result = best_restaurants.union(worst_restaurants)

# Join final_result with original df to get all columns
final_result_with_all_columns = final_result.join(df, ["City", "Price Range"])
final_result_with_all_columns = final_result_with_all_columns.drop("Max Rating", "Min Rating")

# Write output to HDFS, overwrite if path already exists
final_result_with_all_columns.write.mode("overwrite").csv("/assignment2/output/question2/")

final_result_with_all_columns.show()