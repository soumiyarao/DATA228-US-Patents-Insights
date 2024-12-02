from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, desc

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Top 20 Inventors by Distinct Patent Count") \
    .getOrCreate()

# Load the inventors dataset
file_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/inventor_info/*.csv"
inventor_df = spark.read.option("header", "true").csv(file_path)

# Count the distinct patents for each inventor
inventor_patent_count = inventor_df.groupBy("inventor_id", "disambig_inventor_name_first").agg(
  countDistinct("patent_id").alias("distinct_patent_count")
)

# Sort by distinct_patent_count in descending order and select the top 20
top_20_inventors = inventor_patent_count.orderBy(desc("distinct_patent_count")).limit(20)

# Show the results
top_20_inventors.show()

# Save the top 20 inventors to a CSV file
output_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/top_20_inventors.csv"
top_20_inventors.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
