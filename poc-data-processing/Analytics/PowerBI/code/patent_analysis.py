from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, col
from pyspark.sql.functions import col, to_date, datediff, avg, min, max

# Initialize Spark session
spark = SparkSession.builder.appName("patent_trend_analysis").getOrCreate()

# Load all the CSV files in the directory into a single DataFrame
df_1 = spark.read.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/patent_info/*.csv", header=True, inferSchema=True)

# Filter out rows with null patent_date
df_clean = df_1.filter(col("patent_date").isNotNull())

# Add year column from patent_date
df = df_clean.withColumn("grant_year", year("patent_date"))

# Group by year and count patents
patents_by_year = df.groupBy("grant_year").count().orderBy("grant_year")

# Display the results
patents_by_year.show()

# Save the result to a parquet file
patents_by_year.write.parquet("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/parquet/patent_trends.parquet")

# Filter out rows with null dates
df_clean_1 = df.filter(col("filing_date").isNotNull() & col("patent_date").isNotNull())

# Calculate grant time
df_clean_1 = df_clean_1.withColumn("grant_time_days", datediff(col("patent_date"), col("filing_date")))

# Average grant time by patent type
avg_grant_time_by_type = df_clean_1.groupBy("patent_type").agg(
    avg("grant_time_days")
)
avg_grant_time_by_type.show()

# Save results
avg_grant_time_by_type.write.parquet("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/parquet/avg_grant_time_by_type.parquet")


