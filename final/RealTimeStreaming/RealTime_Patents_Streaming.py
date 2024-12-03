from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.streaming import StreamingQueryListener


# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Real Time Patents Streaming") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


input_path = "file:///Users/bhland/Big_Data_Project/Demo/input"  

# Output path for Hudi table
patent_trends_output_path = "file:///Users/bhland/hive/warehouse/dashboard_analytics_results/patent_trends" 
gender_trends_output_path = "file:///Users/bhland/hive/warehouse/dashboard_analytics_results/gender_trends" 
geo_distribution_output_path = "file:///Users/bhland/hive/warehouse/dashboard_analytics_results/geo_distribution" 

# Checkpoint Location:
patent_trends_checkpoint_dir = "file:///Users/bhland/hive/warehouse/dashboard_analytics_results/checkpoint_patent_dir"
gender_trends_checkpoint_dir = "file:///Users/bhland/hive/warehouse/dashboard_analytics_results/checkpoint_gender_dir"
geo_distribution_checkpoint_dir = "file:///Users/bhland/hive/warehouse/dashboard_analytics_results/checkpoint_geo_dir"


# Schema for the JSON data
schema = StructType([
    StructField("patent_id", StringType(), True),
    StructField("patent_date", StringType(), True),
    StructField("num_claims", StringType(), True),
    StructField("branch", StringType(), True),
    StructField("inventors", ArrayType(StructType([
        StructField("inventor_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("state", StringType(), True)
    ])), True),
    StructField("applicants", ArrayType(StructType([
        StructField("organization", StringType(), True)
    ])), True)
])


# Read JSON files as streaming input
json_stream = spark.readStream \
    .format("json") \
    .schema(schema) \
    .load(input_path)

json_stream = json_stream.na.drop(how="any")

df_with_year_month = json_stream.withColumn("year_month", date_trunc("month", "patent_date"))


processed_df = df_with_year_month \
    .withColumn("timestamp", to_timestamp("year_month", "yyyy-mm-dd")) 


processed_df_with_watermark = processed_df.withWatermark("timestamp", "0 year")


# Annual Grant Patent Trends
patent_aggregated_stream = processed_df_with_watermark \
    .groupBy("timestamp", "branch") \
    .agg(
        avg("num_claims").alias("avg_claims"),
        count("patent_id").alias("total_patents")
    )


# Gender Trends
processed_df_with_watermark_explode = processed_df_with_watermark \
    .withColumn("inventor", explode(col("inventors"))) 


gender_aggregated_stream = processed_df_with_watermark_explode \
    .groupBy("timestamp", "branch") \
    .agg(
        count(when(col("inventor.gender") == "M", 1)).alias("male_count"),
        count(when(col("inventor.gender") == "F", 1)).alias("female_count")
    )


# Geographical Distribution
geo_aggregated_stream = processed_df_with_watermark_explode \
    .groupBy("timestamp", "branch", "inventor.state") \
    .agg(
        count("inventor.inventor_name").alias("inventor_count"),
        count("patent_id").alias("patent_count")
    )


patent_stream = patent_aggregated_stream \
    .select(
        col("timestamp"),
        to_date(col("timestamp")).alias("year"),
        col("branch"),
        col("total_patents").alias("grant_count"),
        col("avg_claims")
    )


gender_stream = gender_aggregated_stream \
    .select(
        col("timestamp"),
        to_date(col("timestamp")).alias("year"),
        col("branch"),
        col("male_count"),
        col("female_count")
    )


geo_stream = geo_aggregated_stream \
    .select(
        col("timestamp"),
        to_date(col("timestamp")).alias("year"),
        col("branch"),
        concat(lit("US-"), col("state")).alias("state"),
        col("inventor_count"),
        col("patent_count")
    )


# Hudi Options
patent_trends_options = {
    'hoodie.table.name': 'patent_trends',
    'hoodie.datasource.write.recordkey.field': 'year,branch',
    "hoodie.datasource.write.precombine.field": 'grant_count',
    'hoodie.datasource.write.table.name': 'patent_trends',
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
    "hoodie.datasource.write.operation": "upsert",
    'hoodie.datasource.write.hive_style_partitioning': 'false',
    'hoodie.insert.shuffle.parallelism': 2,
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.write.handle.insert.skip.null.record': 'true'
}


gender_trends_options = {
    'hoodie.table.name': 'gender_trends',
    'hoodie.datasource.write.recordkey.field': 'year,branch',
    "hoodie.datasource.write.precombine.field": 'male_count',
    'hoodie.datasource.write.table.name': 'gender_trends',
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
    "hoodie.datasource.write.operation": "upsert",
    'hoodie.datasource.write.hive_style_partitioning': 'false',
    'hoodie.insert.shuffle.parallelism': 2,
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.write.handle.insert.skip.null.record': 'true'
}

geo_distribution_options = {
    'hoodie.table.name': 'geo_distribution',
    'hoodie.datasource.write.recordkey.field': 'timestamp,branch,state',
    "hoodie.datasource.write.precombine.field": 'inventor_count',
    'hoodie.datasource.write.table.name': 'geo_distribution',
    'hoodie.datasource.write.table.type': 'MERGE_ON_READ',
    "hoodie.datasource.write.operation": "upsert",
    'hoodie.datasource.write.hive_style_partitioning': 'false',
    'hoodie.insert.shuffle.parallelism': 2,
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.write.handle.insert.skip.null.record': 'true'
}


# Writing to Hudi Table Using Structured Streaming
patent_query = patent_stream.writeStream \
    .format("hudi") \
    .options(**patent_trends_options) \
    .outputMode("append") \
    .option("checkpointLocation", patent_trends_checkpoint_dir) \
    .start(patent_trends_output_path)


gender_query = gender_stream.writeStream \
    .format("hudi") \
    .options(**gender_trends_options) \
    .outputMode("append") \
    .option("checkpointLocation", gender_trends_checkpoint_dir) \
    .start(gender_trends_output_path)


geo_distribution_query = geo_stream.writeStream \
    .format("hudi") \
    .options(**geo_distribution_options) \
    .outputMode("append") \
    .option("checkpointLocation", geo_distribution_checkpoint_dir) \
    .start(geo_distribution_output_path)


# Await termination to keep the stream running
patent_query.awaitTermination()
gender_query.awaitTermination()
geo_distribution_query.awaitTermination()


