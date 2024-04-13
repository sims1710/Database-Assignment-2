import sys 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header",True)\
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
#df.printSchema()

# Read the Parquet file into a DataFrame
#df = spark.read.parquet("hdfs://<namenode>:9000/assignment2/part2/input/tmdb_5000_credits.parquet")

# Define the schema for the cast column (string representation of a JSON array)
cast_schema = ArrayType(StructType([
    StructField("cast_id", StringType()),
    StructField("character", StringType()),
    StructField("credit_id", StringType()),
    StructField("gender", StringType()),
    StructField("id", StringType()),
    StructField("name", StringType()),
    StructField("order", StringType())
]))

# Extract the actor names from the cast column
df_new_extracted = df.withColumn("cast", F.from_json("cast", cast_schema))\
    .select("movie_id", "title", F.explode("cast").alias("cast"))\
    .select("movie_id", "title", "cast.name")

# Generate pairs of actors for each movie
df_paired = df_new_extracted.alias("df1").join(df_new_extracted.alias("df2"), ["movie_id", "title"])\
    .where("df1.name < df2.name")\
    .select("movie_id", "title", F.col("df1.name").alias("actor1"), F.col("df2.name").alias("actor2"))

# Filter out pairs of actors who have co-starred in at least 2 movies
df_paired = df_paired.groupBy("movie_id", "title", "actor1", "actor2").count().where("count >= 2")
df_paired = df_paired.drop("count")

# Write the output to a Parquet file
#df_paired.write.mode("overwrite").parquet("hdfs://<namenode>:9000/assignment2/part2/output/")
df_paired.write.option("header",True).parquet("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn))

spark.stop()
