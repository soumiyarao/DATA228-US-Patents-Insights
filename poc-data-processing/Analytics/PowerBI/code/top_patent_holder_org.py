from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("TopPatentHoldersByOrganization").getOrCreate()

# Load CSV data
input_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/applicant_info/*.csv"  # Update the path as needed
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Filter out rows with null or empty organization names
df_clean = df.filter((col("raw_applicant_organization").isNotNull()) & (col("raw_applicant_organization") != ""))

# Group by organization and count patents
top_holders = (
    df_clean.groupBy("raw_applicant_organization")
    .agg(count("patent_id").alias("patent_count"))
    .orderBy(col("patent_count").desc())
)

#Show organizations with patent count
top_holders.show(20,truncate=False)

# Get only the top 10 organizations
top_10_holders = top_holders.limit(10)

# Show top 10 organizations in the console
top_10_holders.show(truncate=False)

# Save top 10 organizations to a CSV file
output_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/parquet/top_10_patent_holders"
output_path_1 = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/parquet/patent_by_organization"
top_10_holders.write.parquet(output_path)
top_holders.write.parquet(output_path_1)
