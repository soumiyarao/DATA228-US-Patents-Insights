from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Initialize Spark session
spark = SparkSession.builder.appName("CollaborationGraph").getOrCreate()

# Input and output paths
input_path = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\output\applicant_info"
output_path_single = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\Analysis\csv\collaboration_count_1_by_type"
output_path_multiple = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\Analysis\csv\collaboration_count_gt1_by_type"

# Load data
df = spark.read.csv(input_path, header=True)


# Group by `patent_id` and count `applicant_sequence` for each patent
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
single_collaboration_details_df.write.csv(output_path_single, header=True, mode="overwrite")
multiple_collaboration_details_df.write.csv(output_path_multiple, header=True, mode="overwrite")

print("Files saved successfully!")
