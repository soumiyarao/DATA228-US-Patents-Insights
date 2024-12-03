from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("PatentBranchAnalytics") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS") \
    .config("spark.sql.datetime.java8API.enabled", "true") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()



# Constants

filtered_patents_input_path = "../../data_source/filtered_patents"
inventor_data_path = "../../data_source/preprocessed_data_input/inventor_info"
geo_distribution_table_path = "file:/Users/bhland/hive/warehouse/dashboard_analytics_results/geo_distribution"  


# Processing

# Read filtered patents
patents_filtered_df = spark.read.parquet(filtered_patents_input_path)
patents_filtered_df.show(truncate=False)

# Read inventor data and filter
inventor_df = spark.read.parquet(inventor_data_path)
inventor_filtered_df = inventor_df.join(
    patents_filtered_df,
    inventor_df.patent_id == patents_filtered_df.patent_id, 
    "inner"
)

inventor_filtered_df = inventor_filtered_df.drop(inventor_df['patent_id'])
inventor_df_with_year = inventor_filtered_df.withColumn("year", F.year("patent_date")).withColumn("month", F.month("patent_date")) 


# Aggregations
geo_data = inventor_df_with_year.groupBy("year", "month", "branch", "disambig_state").agg(
    F.countDistinct("inventor_id").alias("inventor_count"),  
    F.countDistinct("patent_id").alias("patent_count")  
)
geo_data_df = geo_data.withColumn("timestamp", make_date(col("year"), col("month"), lit(1)))
geo_data_df = geo_data_df.drop("month")
geo_distribution = geo_data_df.select(
    F.col("timestamp"),
    F.col("branch"),
    F.concat(F.lit("US-"), F.col("disambig_state")).alias("state"),
    F.col("inventor_count"),
    F.col("patent_count")
)

geo_distribution.show(truncate=False)

# Final result df
geo_distribution_with_schema = geo_distribution.select(
    col("timestamp").cast(TimestampType()),
    col("timestamp").alias("year"),
    col("branch").cast(StringType()),
    col("state").cast(StringType()),
    col("inventor_count").cast(IntegerType()),
    col("patent_count").cast(IntegerType())
)

geo_distribution_with_schema.printSchema()
geo_distribution_with_schema = geo_distribution_with_schema.na.drop(how="any")
geo_distribution_with_schema.show(truncate=False)


# Write to hudi
geo_distribution_hudi_options = {
   'hoodie.table.name': 'geo_distribution',
   'hoodie.datasource.write.recordkey.field': 'timestamp,branch,state',
   'hoodie.datasource.write.precombine.field': "inventor_count",
   'hoodie.datasource.write.table.name': 'geo_distribution',
   'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
   'hoodie.datasource.write.operation': 'insert',
   'hoodie.upsert.shuffle.parallelism': 2,
   'hoodie.insert.shuffle.parallelism': 2,
}
geo_distribution_with_schema.write.format("org.apache.hudi").options(**geo_distribution_hudi_options).mode("overwrite").save(geo_distribution_table_path)
print("The Geo Distribution results have been successfully written to the hudi table in hive warehouse.")





