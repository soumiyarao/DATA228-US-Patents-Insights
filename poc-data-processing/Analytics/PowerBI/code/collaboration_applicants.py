from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

# Initialize Spark session
spark = SparkSession.builder.appName("CollaborationGraph").getOrCreate()

# Input and output paths
input_path = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\output\applicant_info\*.csv"
output_path_single = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\Analysis\csv\collaboration_count_1"
output_path_multiple = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\Analysis\csv\collaboration_count_gt1"

# Load data
df = spark.read.csv(input_path, header=True)

# Filter out rows with missing names
filtered_df = df.filter(
    (col("raw_applicant_name_first").isNotNull()) & 
    (col("raw_applicant_name_last").isNotNull()))

# Group and count collaborations
collaboration_df = (
    filtered_df.groupBy("raw_applicant_name_first", "raw_applicant_name_last", "raw_applicant_organization")
    .agg(count("patent_id").alias("collaboration_count")))

# Split data into two DataFrames based on `collaboration_count`
single_collaboration_df = collaboration_df.filter(col("collaboration_count") == 1)
multiple_collaboration_df = collaboration_df.filter(col("collaboration_count") > 1)

# Save to CSV files
single_collaboration_df.write.csv(output_path_single, header=True, mode="overwrite")
multiple_collaboration_df.write.csv(output_path_multiple, header=True, mode="overwrite")

print("Files saved successfully!")
