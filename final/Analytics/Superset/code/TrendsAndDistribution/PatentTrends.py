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
patent_trends_table_path = "file:/Users/bhland/hive/warehouse/dashboard_analytics_results/patent_trends"  


# Processing

# Read filtered patents
patents_filtered_df = spark.read.parquet(filtered_patents_input_path)
patents_filtered_df.show(truncate=False)
patent_df_with_years = patents_filtered_df.withColumn("grant_year", year(col("patent_date"))) \
                                .withColumn("grant_month", month(col("patent_date")))
patent_df_with_years = patent_df_with_years.withColumn("grant_year", make_date(col("grant_year"), col("grant_month"), lit(1)))



# Group by Year and Aggregate Counts
grant_counts = patent_df_with_years.groupBy("grant_year", "branch").agg(
    count("patent_id").alias("patent_grant_count")
)
grant_counts = grant_counts.withColumnRenamed("branch", "grant_branch")

final_result = grant_counts.select(
    F.col("grant_branch").alias("branch"),
    F.col("grant_year").alias("year"),
    F.coalesce(grant_counts.patent_grant_count, F.lit(0)).alias("grant_count")
)
final_result.show(truncate=False)

df_with_year_and_month = patents_filtered_df \
    .withColumn("year", F.year("patent_date")) \
    .withColumn("month", F.month("patent_date"))  


# Group by year and branch, and calculate the average number of claims
avg_claims = df_with_year_and_month.groupBy("year","month","branch").agg(
    F.avg("num_claims").alias("avg_claims")
)
avg_claims = avg_claims.withColumn("year", make_date(col("year"), col("month"), lit(1)))

combined_df = final_result.join(
    avg_claims,
    on=["year", "branch"],
    how="left"
)
combined_df = combined_df.drop("month")
combined_df.show(truncate=False)


# Final result df
patent_trends_with_schema = combined_df.select(
    col("year").cast(TimestampType()).alias("timestamp"),
    col("year").cast(DateType()),
    col("branch").cast(StringType()),
    col("grant_count").cast(IntegerType()),
    col("avg_claims").cast(FloatType()),
)

patent_trends_with_schema.show(truncate=False)


# Write to Hudi
patent_trends_hudi_options = {
    'hoodie.table.name': 'patent_trends',
    'hoodie.datasource.write.recordkey.field': 'year,branch',
    'hoodie.datasource.write.precombine.field': "grant_count",
    'hoodie.datasource.write.table.name': 'patent_trends',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.operation': 'insert',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
}
patent_trends_with_schema.write.format("org.apache.hudi").options(**patent_trends_hudi_options).mode("overwrite").save(patent_trends_table_path)
print("The Patents Trends results have been successfully written to the hudi table in hive warehouse.")




