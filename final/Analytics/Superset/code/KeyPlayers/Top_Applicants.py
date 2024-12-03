from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder.appName("PatentKeyPlayersAnalytics") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()


# Constants

filtered_patents_input_path = "../../data_source/filtered_patents"
applicant_data_path = "../../data_source/preprocessed_data_input/applicant_info"
top_applicants_path = "file:/Users/bhland/hive/warehouse/dashboard_analytics_results/top_applicants"


# Processing


# Read filtered Patents
patents_filtered_df = spark.read.parquet(filtered_patents_input_path)
patents_filtered_df.show(truncate=False)


# Read Applicant data and filter
applicant_df = spark.read.parquet(applicant_data_path)

applicant_filtered_df = applicant_df.join(
    patents_filtered_df,
    applicant_df.patent_id == patents_filtered_df.patent_id, 
    "inner"
)

applicant_filtered_df = applicant_filtered_df.drop(applicant_df['patent_id'])
applicant_filtered_df.printSchema()


# Cleaning
applicant_cleaned_df = applicant_filtered_df.filter(F.col("raw_applicant_organization").isNotNull())

applicant_normalized_df = applicant_cleaned_df.withColumn(
    "normalized_organization", 
    F.initcap(F.lower(F.col("raw_applicant_organization")))
)


# Group by branch and find top 20 in each branch
applicant_grouped_df = applicant_normalized_df.groupBy(
    "branch", 
    "normalized_organization"
).agg(
    F.count("patent_id").alias("patent_count")  
)

applicant_window_spec = Window.partitionBy("branch").orderBy(F.desc("patent_count"))
applicant_ranked_df = applicant_grouped_df.withColumn(
    "rank", 
    F.row_number().over(applicant_window_spec)
)
top_applicants = applicant_ranked_df.filter(F.col("rank") <= 20)


 # Final Result Df
final_applicant_result = top_applicants.select(
    F.col("branch"),
    F.col("normalized_organization").alias("organization"),
    F.col("patent_count"),
    F.col("rank")
).orderBy("branch", "rank")

final_applicant_result.show(truncate=False)

final_applicant_with_schema = final_applicant_result.select(
    col("branch").cast(StringType()),
    col("organization").cast(StringType()),
    col("patent_count").cast(IntegerType()),
    col("rank").cast(IntegerType())
)

# Write to Parquet
final_applicant_with_schema.write.parquet(top_applicants_path, mode="overwrite")
print("The Top Applicants results have been successfully written to the parquet file in hive warehouse.")

