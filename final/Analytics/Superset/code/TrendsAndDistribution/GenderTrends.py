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
gender_trends_table_path = "file:/Users/bhland/hive/warehouse/dashboard_analytics_results/gender_trends"  


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

inventer_with_gender = inventor_df_with_year.withColumn(
    "gender_code",
    F.coalesce(F.col("gender_code"), F.lit("U"))
)


# Male counts
male_df = inventer_with_gender.filter(F.col("gender_code") == "M") 
male_count_df = male_df.groupBy("year", "month", "branch").agg(
    count("patent_id").alias("male_count")
)

male_count_df = male_count_df.withColumn("year", make_date(col("year"), col("month"), lit(1)))
male_df = male_count_df.drop("month")
male_df.show(truncate=False)


# Female Counts
female_df = inventer_with_gender.filter(F.col("gender_code") == "F") 
female_count_df = female_df.groupBy("year", "month", "branch").agg(
    count("patent_id").alias("female_count")
)

female_count_df = female_count_df.withColumn("year", make_date(col("year"), col("month"), lit(1)))
female_combined_df = male_df.join(
    female_count_df,
    on=["year", "branch"],
    how="left"
).orderBy(desc("year"))

female_combined_df = female_combined_df.drop("month")
female_combined_df.show(truncate=False)


# Final results df
final_combined_result = female_combined_df.select(
    female_combined_df.year,
    female_combined_df.branch,
    F.coalesce(female_combined_df.female_count, F.lit(0)).alias("female_count"),
    F.coalesce(female_combined_df.male_count, F.lit(0)).alias("male_count")
)
final_combined_result.show(truncate=False)

gender_result_with_schema = final_combined_result.select(
    col("year").cast(TimestampType()).alias("timestamp"),
    col("year").cast(DateType()),
    col("branch").cast(StringType()),
    col("male_count").cast(IntegerType()),
    col("female_count").cast(IntegerType())
)

gender_result_with_schema.printSchema()


# Write to Hudi
gender_trends_hudi_options = {
    'hoodie.table.name': 'gender_trends',
    'hoodie.datasource.write.recordkey.field': 'year,branch',
    'hoodie.datasource.write.precombine.field': "male_count",
    'hoodie.datasource.write.table.name': 'gender_trends',
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    'hoodie.datasource.write.operation': 'insert',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2,
}
gender_result_with_schema.write.format("org.apache.hudi").options(**gender_trends_hudi_options).mode("overwrite").save(gender_trends_table_path)
print("The Gender Trends results have been successfully written to the hudi table in hive warehouse.")






