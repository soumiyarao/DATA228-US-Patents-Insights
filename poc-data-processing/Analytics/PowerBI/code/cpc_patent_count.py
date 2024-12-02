from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CPC Section Patent Count") \
    .getOrCreate()

# Load the CPC information CSV
cpc_df = spark.read.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/cpc_info/*.csv")

# Show schema to understand the structure of the dataset
cpc_df.printSchema()

# Remove duplicates to avoid counting the same patent multiple times for the same section
distinct_cpc_df = cpc_df.select("patent_id", "cpc_section").distinct()

# Group by `cpc_section` and count distinct patents
cpc_agg_df = distinct_cpc_df.groupBy("cpc_section").agg(
    count("patent_id").alias("patent_count")
)

# Show the aggregated results
cpc_agg_df.show()

# Load CPC section descriptions from a lookup file (if available)
# Assuming you have a mapping file with `cpc_section` and `section_description`
description_df = spark.read.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/cpc_section/*.csv")

# Join with the description data to get human-readable descriptions
cpc_final_df = cpc_agg_df.join(description_df, "cpc_section", "left")

# Show the final results
cpc_final_df.show()

# Write the aggregated results to a CSV file
output_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/cpc_patent_counts"
cpc_final_df.coalesce(1).write.option("header", "true").csv(output_path)

print("Files saved successfully!")
