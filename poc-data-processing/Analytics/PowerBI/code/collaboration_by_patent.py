from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("CollaborationCountByPatent").getOrCreate()

# Input and output paths
input_path = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\output\applicant_info\*.csv"
output_path_collaboration = r"A:\SJSU\Sem-2\DATA-228-Big Data\Project\Analysis\csv\collaboration_count_by_patent"

# Load data
df = spark.read.csv(input_path, header=True)

# Group by `patent_id` and count the number of applicants (collaboration count)
collaboration_df = (
    df.groupBy("patent_id")
    .agg(count("applicant_sequence").alias("collaboration_count"))
)

# Save results to CSV
collaboration_df.write.csv(output_path_collaboration, header=True, mode="overwrite")

print("Files saved successfully!")
