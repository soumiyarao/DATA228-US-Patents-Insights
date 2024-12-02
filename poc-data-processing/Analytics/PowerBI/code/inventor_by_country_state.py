from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("InventorsCountryCount").getOrCreate()

# Load all the CSV files in the directory into a single DataFrame
df = spark.read.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/inventor_info/*.csv", header=True, inferSchema=True)

# Remove rows where 'disambig_country' or 'inventor_id' are null
df_clean = df.filter(col("disambig_country").isNotNull() & col("inventor_id").isNotNull())

# Group by country and count the number of distinct inventors
country_inventors_count = df_clean.groupBy("disambig_country").agg(countDistinct("inventor_id").alias("num_inventors"))

# Save the result to a Parquet file
country_inventors_count.write.parquet("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/inventor_country_count.parquet")

# Group by state and count the number of distinct inventors
state_inventors_count = df_clean.groupBy("disambig_state").agg(countDistinct("inventor_id").alias("num_inventors"))

# Save the result to a Parquet file
state_inventors_count.write.parquet("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/inventor_state_count.parquet")
