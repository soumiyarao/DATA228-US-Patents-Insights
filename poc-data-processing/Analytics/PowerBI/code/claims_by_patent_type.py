from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Patent Type and Claim Count Correlation") \
    .getOrCreate()

# Load the patent information CSV
patent_df = spark.read.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/patent_info/*.csv")

# Show schema to understand the structure of the dataset
patent_df.printSchema()

# Convert num_claims column to integer type (if it's not already)
patent_df = patent_df.withColumn("num_claims", col("num_claims").cast("int"))

# Aggregate data by patent_type and calculate the average number of claims
patent_agg_df = patent_df.groupBy("patent_type").agg(
    avg("num_claims").alias("avg_num_claims"),
    count("patent_id").alias("patent_count")
)

# Show the results
patent_agg_df.show()

# Write the aggregated results to a CSV file
output_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/patent_type_claims.csv"
patent_agg_df.coalesce(1).write.option("header", "true").csv(output_path)

