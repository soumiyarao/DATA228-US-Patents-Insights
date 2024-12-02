from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, collect_set, when, lit, length

# Initialize Spark session
spark = SparkSession.builder.appName("ApplicantInfoAnalysis").getOrCreate()

# Input path
input_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/applicant_info/*.csv"

# Load data
df = spark.read.csv(input_path, header=True, inferSchema=True)
df.printSchema()

# Filter out rows with missing applicant names
filtered_df = df.filter(
    (col("raw_applicant_name_first").isNotNull()) & (col("raw_applicant_name_last").isNotNull())
)

# Total collaborations by organization
collaboration_count = (
    filtered_df.groupBy("raw_applicant_organization")
    .agg(count("patent_id").alias("total_collaborations"))
    .orderBy(col("total_collaborations").desc())
)
collaboration_count.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/collaboration_count_by_organization", header=True, mode="overwrite")
print("collaboration_count_by_organization saved successfully!")

# Average number of applicants per patent
avg_applicants_per_patent = (
    df.groupBy("patent_id")
    .agg(count("applicant_sequence").alias("applicant_count"))
    .agg(avg("applicant_count").alias("average_applicants_per_patent"))
)
avg_applicants_per_patent.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/average_applicants_per_patent", header=True, mode="overwrite")
print("average_applicants_per_patent saved successfully!")

# Most frequent applicant types
applicant_type_count = (
    df.groupBy("applicant_type")
    .agg(count("applicant_sequence").alias("total_applicants"))
    .orderBy(col("total_applicants").desc())
)
applicant_type_count.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/applicant_type_frequency", header=True, mode="overwrite")
print("applicant_type_frequency saved successfully!")

# Top 10 organizations by patent count
top_organizations = (
    df.filter(col("raw_applicant_organization").isNotNull())
    .groupBy("raw_applicant_organization")
    .agg(count("patent_id").alias("patent_count"))
    .orderBy(col("patent_count").desc())
    .limit(10)
)
top_10_organizations.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/top_10_organizations_by_patent_count", header=True, mode="overwrite")
print("top_10_organizations_by_patent_count saved successfully!")

# Min, Max, and Avg collaborations per organization
collaboration_stats = (
    df.filter(col("raw_applicant_organization").isNotNull())
    .groupBy("raw_applicant_organization")
    .agg(
        count("patent_id").alias("total_collaborations")
    )
    .agg(
        avg("total_collaborations").alias("avg_collaborations"),
        max("total_collaborations").alias("max_collaborations"),
        min("total_collaborations").alias("min_collaborations")
    )
)
collaboration_stats.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/collaboration_statistics", header=True, mode="overwrite")
print("collaboration_statistics saved successfully!")

# Unique applicants by organization
unique_applicants = (
    df.filter(col("raw_applicant_organization").isNotNull())
    .groupBy("raw_applicant_organization")
    .agg(collect_set("raw_applicant_name_last").alias("unique_applicants"))
)
unique_applicants.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/unique_applicants_by_organization", header=True, mode="overwrite")
print("unique_applicants_by_organization saved successfully!")

# Patents with a single applicant
single_applicant_patents = (
    df.groupBy("patent_id")
    .agg(count("applicant_sequence").alias("applicant_count"))
    .filter(col("applicant_count") == 1)
    .agg(count("patent_id").alias("single_applicant_patent_count"))
)
single_applicant_patents.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/single_applicant_patents", header=True, mode="overwrite")
print("single_applicant_patents saved successfully!")

# Distribution of applicant types across organizations
applicant_type_distribution = (
    df.groupBy("raw_applicant_organization", "applicant_type")
    .agg(count("patent_id").alias("patent_count"))
)
applicant_type_distribution.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/applicant_type_distribution", header=True, mode="overwrite")
print("applicant_type_distribution saved successfully!")

# Top 10 applicants with the most collaborations
top_applicants = (
    filtered_df.groupBy("raw_applicant_name_first", "raw_applicant_name_last")
    .agg(count("patent_id").alias("total_collaborations"))
    .orderBy(col("total_collaborations").desc())
    .limit(10)
)
top_applicants.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/top_applicants_by_collaborations", header=True, mode="overwrite")
print("top_applicants_by_collaborations saved successfully!")

#Top organizations by patent count
# Group by organization and count patents
top_holders = (
    df_clean.groupBy("raw_applicant_organization")
    .agg(count("patent_id").alias("patent_count"))
    .orderBy(col("patent_count").desc())
)

top_organizations.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/top_organizations_by_patent_count", header=True, mode="overwrite")
print("top_organizations_by_patent_count saved successfully!")

# Total patents per year
patents_per_year = (
    df.groupBy("patent_date")
    .agg(count("patent_id").alias("total_patents"))
    .orderBy("patent_date")
)
patents_per_year.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/patents_per_year", header=True, mode="overwrite")
print("patents_per_year saved successfully!")

#Collaboration_by_relationship_type
collaboration_df = (
    df.groupBy("patent_id")
    .agg(count("applicant_sequence").alias("collaboration_count"))
)

# Filter for patents with single and multiple collaborations
single_collaboration_df = collaboration_df.filter(col("collaboration_count") == 1)
multiple_collaboration_df = collaboration_df.filter(col("collaboration_count") > 1)

# Join back with original data to include `applicant_type` and `raw_applicant_organization`
single_collaboration_details_df = single_collaboration_df.join(
    df, "patent_id", "inner"
).select("patent_id", "applicant_type", "raw_applicant_organization", "collaboration_count")

multiple_collaboration_details_df = multiple_collaboration_df.join(
    df, "patent_id", "inner"
).select("patent_id", "applicant_type", "raw_applicant_organization", "collaboration_count")

# Save results to CSV
single_collaboration_details_df.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/collaboration_count_1_by_type", header=True, mode="overwrite")
multiple_collaboration_details_df.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/collaboration_count_gt1_by_type", header=True, mode="overwrite")

print("collaboration_count_1_by_type saved successfully!")
print("collaboration_count_gt1_by_type saved successfully!")


#Organizations with the most unique applicants
unique_applicant_count = (
    filtered_df.groupBy("raw_applicant_organization")
    .agg(count("raw_applicant_name_last").alias("unique_applicant_count"))
    .orderBy(col("unique_applicant_count").desc())
)
unique_applicant_count.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/organizations_with_unique_applicants", header=True, mode="overwrite")
print("organizations_with_unique_applicants saved successfully!")

# Count of patents with multiple applicants
multi_applicant_patents = (
    df.groupBy("patent_id")
    .agg(count("applicant_sequence").alias("applicant_count"))
    .filter(col("applicant_count") > 1)
    .agg(count("patent_id").alias("multi_applicant_patent_count"))
)
multi_applicant_patents.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/multi_applicant_patents", header=True, mode="overwrite")
print("multi_applicant_patents saved successfully!")

#Average name length of applicants
average_name_length = (
    df.withColumn("name_length", length(col("raw_applicant_name_first")) + length(col("raw_applicant_name_last")))
    .agg(avg("name_length").alias("avg_name_length"))
)
average_name_length.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/average_applicant_name_length", header=True, mode="overwrite")
print("average_applicant_name_length saved successfully!")

# Patents per applicant type
patents_by_applicant_type = (
    df.groupBy("applicant_type")
    .agg(count("patent_id").alias("patent_count"))
    .orderBy(col("patent_count").desc())
)
patents_by_applicant_type.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/patents_per_applicant_type", header=True, mode="overwrite")
print("patents_per_applicant_type saved successfully!")

#Count of patents by year and applicant type
patents_by_year_and_type = (
    df.groupBy("patent_date", "applicant_type")
    .agg(count("patent_id").alias("patent_count"))
)
patents_by_year_and_type.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/patents_by_year_and_applicant_type", header=True, mode="overwrite")
print("patents_by_year_and_applicant_type saved successfully!")

#Applicants with most patents
applicants_with_most_patents = (
    filtered_df.groupBy("raw_applicant_name_first", "raw_applicant_name_last")
    .agg(count("patent_id").alias("patent_count"))
    .orderBy(col("patent_count").desc())
    .limit(10)
)
applicants_with_most_patents.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/applicants_with_most_patents", header=True, mode="overwrite")
print("applicants_with_most_patents saved successfully!")

#Collaboration of Applicants
collaboration_df = (
    filtered_df.groupBy("raw_applicant_name_first", "raw_applicant_name_last", "raw_applicant_organization")
    .agg(count("patent_id").alias("collaboration_count")))

# Split data into two DataFrames based on `collaboration_count`
single_collaboration_df = collaboration_df.filter(col("collaboration_count") == 1)
multiple_collaboration_df = collaboration_df.filter(col("collaboration_count") > 1)

# Save to CSV files
single_collaboration_df.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/collaboration_count_1", header=True, mode="overwrite")
multiple_collaboration_df.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/collaboration_count_gt1", header=True, mode="overwrite")
print("collaboration_count_1 saved successfully!")
print("collaboration_count_gt1 saved successfully!")

#Collaboration count by patent
#Group by `patent_id` and count the number of applicants (collaboration count)
collaboration_df = (
    df.groupBy("patent_id")
    .agg(count("applicant_sequence").alias("collaboration_count"))
)
collaboration_df.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/collaboration_count_by_patent", header=True, mode="overwrite")
print("collaboration_count_by_patent saved successfully!")



# Organizations with only individual applicants
individual_organizations = (
    df.filter(col("applicant_sequence") == 1)
    .groupBy("raw_applicant_organization")
    .agg(count("patent_id").alias("patent_count"))
)
individual_organizations.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/organizations_with_individual_applicants", header=True, mode="overwrite")
print("organizations_with_individual_applicants saved successfully!")

# Applicants with patents across multiple years
applicants_across_years = (
    df.groupBy("raw_applicant_name_first", "raw_applicant_name_last")
    .agg(count("patent_date").alias("distinct_years"))
    .filter(col("distinct_years") > 1)
)
applicants_across_years.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/applicants_with_patents_across_years", header=True, mode="overwrite")
print("applicants_with_patents_across_years saved successfully!")

# Most active organizations in the last year
most_active_organizations_last_year = (
    df.filter(col("patent_date").like("2023%"))
    .groupBy("raw_applicant_organization")
    .agg(count("patent_id").alias("patent_count"))
    .orderBy(col("patent_count").desc())
)
most_active_organizations_last_year.write.csv("A:/SJSU/Sem-2/DATA-228-Big Data/Project/Analysis/csv/most_active_organizations_last_year", header=True, mode="overwrite")
print("most_active_organizations_last_year saved successfully!")

print("All files saved successfully!")

# Stop the Spark session
spark.stop()
