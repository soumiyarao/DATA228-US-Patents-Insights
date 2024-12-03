from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, col, to_date, datediff, avg, min, max, stddev, percentile_approx, sum

# Initialize Spark session
spark = SparkSession.builder.appName("Patent Data Analysis").getOrCreate()

# Load patent information CSVs
patent_df = spark.read.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/patent_info/*.csv", header=True, inferSchema=True)
patent_df.printSchema()
cpc_df = spark.read.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/cpc_info/*.csv", header=True, inferSchema=True)
cpc_df.printSchema()

df_clean = patent_df.filter(col("patent_date").isNotNull()).withColumn("grant_year", year("patent_date"))
df_clean.printSchema()

# Total patents per decade
patents_by_decade = df_clean.withColumn("decade", (col("grant_year") / 10).cast("int") * 10) \
    .groupBy("decade").count().orderBy("decade")
patents_by_decade.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/patents_by_decade")
print("patents_by_decade saved successfully!")

# Most common patent types
common_patent_types = df_clean.groupBy("patent_type").count().orderBy(col("count").desc())
common_patent_types.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/common_patent_types")
print("common_patent_types saved successfully!")

# Count patents by year
patents_by_year = df_clean.groupBy("grant_year").count().orderBy("grant_year")
patents_by_year.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/patent_trends")
print("patent_trends saved successfully!")


# Calculate average grant time by patent type
df_clean = df_clean.filter(col("filing_date").isNotNull())
df_clean = df_clean.withColumn("grant_time_days", datediff(col("patent_date"), col("filing_date")))

avg_grant_time_by_type = df_clean.groupBy("patent_type").agg(avg("grant_time_days").alias("avg_grant_time_days"))
avg_grant_time_by_type.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/avg_grant_time_by_type")
print("avg_grant_time_by_type saved successfully!")

# CPC Section Patent Count
cpc_df = spark.read.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/cpc_info/*.csv")
distinct_cpc_df = cpc_df.select("patent_id", "cpc_section").distinct()
cpc_agg_df = distinct_cpc_df.groupBy("cpc_section").agg(count("patent_id").alias("patent_count"))

# Load CPC section descriptions
description_df = spark.read.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/cpc_section/*.csv")
cpc_final_df = cpc_agg_df.join(description_df, "cpc_section", "left")

cpc_final_df.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/cpc_patent_counts")
print("cpc_patent_counts saved successfully!")

# Patent Type and Claim Count Correlation
patent_df = patent_df.withColumn("num_claims", col("num_claims").cast("int"))
patent_agg_df = patent_df.groupBy("patent_type").agg(
    avg("num_claims").alias("avg_num_claims"),
    count("patent_id").alias("patent_count")
)
patent_agg_df.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/patent_type_claims")
print("patent_type_claims saved successfully!")

# Trend of patent filings by year
filings_by_year = df_clean.filter(col("filing_date").isNotNull()).withColumn("filing_year", year("filing_date")) \
    .groupBy("filing_year").count().orderBy("filing_year")
filings_by_year.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/filings_by_year")
print("filings_by_year saved successfully!")

# Distribution of patents with grant time over 1000 days
long_grant_patents = df_clean.filter(col("grant_time_days") > 1000).groupBy("patent_type").count()
long_grant_patents.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/long_grant_patents")
print("long_grant_patents saved successfully!")

# CPC sections with the shortest average grant time
shortest_grant_cpc = df_clean.join(cpc_df, "patent_id").groupBy("cpc_section") \
    .agg(avg("grant_time_days").alias("avg_grant_time")) \
    .orderBy("avg_grant_time")
shortest_grant_cpc.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/shortest_grant_cpc")
print("shortest_grant_cpc saved successfully!")

# Count of patents with zero claims
zero_claims = patent_df.filter(col("num_claims") == 0).count()
with open("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/zero_claims.txt", "w") as f:
    f.write(f"Patents with zero claims: {zero_claims}")
print("zero_claims saved successfully!")

# Proportion of patents filed in top 3 CPC sections
top_3_cpc = cpc_df.groupBy("cpc_section").count().orderBy(col("count").desc()).limit(3)
top_3_cpc_patents = top_3_cpc.select(col("count")).rdd.map(lambda row: row[0]).sum()
total_cpc_patents = cpc_df.select("patent_id").distinct().count()
with open("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/top_3_cpc_proportion.txt", "w") as f:
    f.write(f"Proportion: {top_3_cpc_patents / total_cpc_patents * 100:.2f}%")
print("top_3_cpc_proportion saved successfully!")

# Year with the highest claims
max_claims_year = patent_df.groupBy("grant_year").agg(sum("num_claims").alias("total_claims")) \
    .orderBy(col("total_claims").desc()).limit(1)
max_claims_year.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/max_claims_year")
print("max_claims_year saved successfully!")

#  Average claims by CPC section
avg_claims_cpc = cpc_df.join(patent_df, "patent_id").groupBy("cpc_section").agg(avg("num_claims").alias("avg_claims"))
avg_claims_cpc.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/avg_claims_cpc")
print("avg_claims_cpc saved successfully!")

#  Total claims by year and patent type
claims_year_type = patent_df.groupBy("grant_year", "patent_type").agg(sum("num_claims").alias("total_claims"))
claims_year_type.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/claims_year_type")
print("claims_year_type saved successfully!")


# Median grant time for each CPC section
median_grant_cpc = df_clean.join(cpc_df, "patent_id").groupBy("cpc_section") \
    .agg(percentile_approx("grant_time_days", 0.5).alias("median_grant_time"))
median_grant_cpc.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/median_grant_cpc")
print("median_grant_cpc saved successfully!")

# Top 10 patents with the longest grant time
longest_grant_time = df_clean.orderBy(col("grant_time_days").desc()).limit(10)
longest_grant_time.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/longest_grant_time")
print("longest_grant_time saved successfully!")

#  Total patents granted per country
if "country" in df_clean.columns:
    patents_by_country = df_clean.groupBy("country").count()
    patents_by_country.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/patents_by_country")
print("patents_by_country saved successfully!")

# Patent type distribution by decade
type_by_decade = df_clean.withColumn("decade", (col("grant_year") / 10).cast("int") * 10) \
    .groupBy("decade", "patent_type").count()
type_by_decade.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/type_by_decade")
print("type_by_decade saved successfully!")

# 15. Top 5 CPC sections with increasing trends over decades
trending_cpc = cpc_df.join(df_clean, "patent_id").withColumn("decade", (col("grant_year") / 10).cast("int") * 10) \
    .groupBy("cpc_section", "decade").count()
trending_cpc.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/trending_cpc")
print("trending_cpc saved successfully!")

# Percentage of patents granted in under a year
under_year = df_clean.filter(col("grant_time_days") < 365).count()
with open("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/under_year.txt", "w") as f:
    f.write(f"Percentage: {under_year / df_clean.count() * 100:.2f}%")

#Claims distribution per year
claims_distribution = patent_df.groupBy("grant_year").agg(
    min("num_claims").alias("min_claims"),
    max("num_claims").alias("max_claims"),
    avg("num_claims").alias("avg_claims")
)
claims_distribution.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/claims_distribution")
print("claims_distribution saved successfully!")

# Longest grant times per country
if "country" in df_clean.columns:
    longest_grant_country = df_clean.groupBy("country").agg(max("grant_time_days").alias("longest_grant_time"))
    longest_grant_country.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/longest_grant_country")
print("longest_grant_country saved successfully!")

#Average number of claims by filing year
avg_claims_filing = df_clean.withColumn("filing_year", year("filing_date")).groupBy("filing_year").agg(avg("num_claims").alias("avg_claims"))
avg_claims_filing.write.option("header", "true").csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/avg_claims_filing")
print("avg_claims_filing saved successfully!")

#Correlation between grant time and number of claims
grant_claim_corr = df_clean.stat.corr("grant_time_days", "num_claims")
with open("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv_1/grant_claim_corr.txt", "w") as f:
    f.write(f"Correlation: {grant_claim_corr}")
print("grant_claim_corr saved successfully!")

print("All files saved successfully!")

# Stop the Spark session
spark.stop()