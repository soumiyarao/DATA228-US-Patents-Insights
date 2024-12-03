from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create a SparkSession
spark = SparkSession.builder.appName("FilterPatents") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()



branches_data = [
    
    ("Artificial Intelligence", ["G06N", "G06F19/00", "G06F17/27", "G06F17/28", "G10L15/22", "G06T", "G06K9/00"]),
    ("Data Science and Analytics", ["G06F16/00", "G06F17/00", "H04L67/00", "H04L12/28", 
                                     "G06F3/06", "G06F12/02", "G06F16/27", "H04L29/06", "G06Q50/00", "B82Y30/00"]),
    ("Networking and Distributed Systems", ["H04L29/00", "H04L12/00", "H04W", "H04L12/24", "H04L67/10", "G06F9/50", 
                                             "H04L67/22", "H04L29/08", "G06F9/46", "G06F15/173", "G06F15/16"]),
    ("Software Development and Security", ["G06F8/00", "G06F9/00", "G06F9/44", "G06F9/451", "H04L67/02", "G06F3/048", 
                                            "G06Q10/00", "G06F3/00", "H04L9/00", "G06F21/00", "G06Q20/40"]),
    ("Advanced Computing Technologies", ["B25J9/00", "G05B19/00", "G06F15/18", "H03K17/00", "G09G5/00", "G06Q30/00"])
]

# Create the branches DataFrame
branches_df = spark.createDataFrame(branches_data, ["branch", "codes"])

branches_df_exploded = branches_df.withColumn("code", explode(col("codes"))).drop("codes")

branches_df_exploded.printSchema()
branches_df_exploded.show(truncate=False)


# Constants

cpc_input_path = "../data_source/preprocessed_data_input/cpc_info"
patent_input_path = "../data_source/preprocessed_data_input/patent_info"
filtered_patents_output_path = "../data_source/filtered_patents"


# Processing

# Read cpc info and filter patents based on codes
cpc_df = spark.read.parquet(cpc_input_path)  

filtered_patents = cpc_df.join(
    branches_df_exploded,
    (cpc_df.cpc_group == branches_df_exploded.code) |  
    (cpc_df.cpc_subclass == branches_df_exploded.code),
    "inner"
).select("patent_id", "branch", "code")


filtered_patents.show(truncate=False)
filtered_patents.printSchema()
unique_patents_df = filtered_patents.dropDuplicates(["patent_id"])
unique_patents_df.show(truncate=False)


# Read Patent Data 
patent_df = spark.read.parquet(patent_input_path)

patents_filtered_year_df = patent_df.filter(patent_df["patent_date"] < "2024-08-01")
patents_filtered_df = patents_filtered_year_df.join(
    unique_patents_df,
    patents_filtered_year_df.patent_id == unique_patents_df.patent_id, 
    "inner"
)

patents_filtered_df = patents_filtered_df.drop(patent_df['patent_id'])
patents_filtered_df.show(truncate=False)
print(patents_filtered_df.count())


# Write to file
patents_filtered_df.write.parquet(filtered_patents_output_path, mode="overwrite")
print("The filtered patents have been successfully written to the output file.")







