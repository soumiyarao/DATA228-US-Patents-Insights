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
inventor_data_path = "../../data_source/preprocessed_data_input/inventor_info"
top_inventors_path = "file:/Users/bhland/hive/warehouse/dashboard_analytics_results/top_inventors"


# Processing


# Read Filtered Patents
patents_filtered_df = spark.read.parquet(filtered_patents_input_path)
patents_filtered_df.show(truncate=False)


# Read Inventor Data and filter
inventor_df = spark.read.parquet(inventor_data_path)

inventor_filtered_df = inventor_df.join(
    patents_filtered_df,
    inventor_df.patent_id == patents_filtered_df.patent_id, 
    "inner"
)

inventor_filtered_df = inventor_filtered_df.drop(inventor_df['patent_id'])


# Aggregations to find top 20
grouped_df = inventor_filtered_df.groupBy(
    "branch", 
    "inventor_id", 
    "disambig_inventor_name_first", 
    "disambig_inventor_name_last", 
    "gender_code", 
    "latitude",
    "longitude",
    "disambig_state"
).agg(
    F.count("patent_id").alias("patent_count")  
)


window_spec = Window.partitionBy("branch").orderBy(F.desc("patent_count"))
ranked_df = grouped_df.withColumn("rank", F.row_number().over(window_spec))
top_inventors = ranked_df.filter(F.col("rank") <= 20)


# Final Result df
final_result = top_inventors.select(
    F.col("branch"),
    F.col("inventor_id"),
    F.concat(F.col("disambig_inventor_name_first"), F.lit(" "), F.col("disambig_inventor_name_last")).alias("inventor_name"),
    F.col("gender_code").alias("gender"),
    F.col("disambig_state").alias("state"),
    F.col("latitude"),
    F.col("longitude"),
    F.col("patent_count"),
    F.col("rank")
).orderBy("branch", "rank")

final_result.show(truncate=False)

final_result_with_schema = final_result.select(
    col("branch").cast(StringType()),
    col("inventor_id").cast(StringType()),
    col("inventor_name").cast(StringType()),
    col("gender").cast(StringType()),
    col("state").cast(StringType()),
    col("latitude").cast(DoubleType()),
    col("longitude").cast(DoubleType()),
    col("patent_count").cast(IntegerType()),
    col("rank").cast(IntegerType())
)

final_result_with_schema.printSchema()

# Write to Parquet file
final_result_with_schema.write.parquet(top_inventors_path, mode="overwrite")
print("The Top Inventors results have been successfully written to the parquet file in hive warehouse.")

