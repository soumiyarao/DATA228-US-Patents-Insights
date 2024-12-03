from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from keybert import KeyBERT
import os



spark = SparkSession.builder \
    .appName("KeyBERT") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()


# Split the large files into 10 parts

input_path = "data/g_brf_sum_text_2024.tsv"
output_path = "data/summary_partitioned"


text_rdd = spark.sparkContext.textFile(input_path)
text_rdd_with_index = text_rdd.zipWithIndex().map(lambda x: (x[1], x[0]))
num_partitions = 10
partitioned_rdd = text_rdd_with_index.repartition(num_partitions).sortByKey().values()
partitioned_rdd.saveAsTextFile(output_path)


# Get the filtered Patent Ids related to computing tecnologies 

filtered_patents_id_path = "data/patent_ids"

patent_ids_df = spark.read.parquet(input_parquet_path)
patent_ids_df.show(10)
patent_ids_count = patent_ids_df.count()
print(f"Total number of patent IDs: {patent_ids_count}")
patent_ids = patent_ids_df.select("patent_id").rdd.flatMap(lambda row: row).collect()



# KeyBERT

def extract_keywords_from_dataframe(spark_df: DataFrame, 
                                    text_col: str, 
                                    id_col: str, 
                                    num_keywords: int = 5, 
                                    model_name: str = 'all-MiniLM-L6-v2') -> DataFrame:

    print("here")
    # Initialize KeyBERT model
    kw_model = KeyBERT(model=model_name)

    # Collect data from the Spark DataFrame
    data_collected = spark_df.select(id_col, text_col).collect()

    # Prepare results
    results = []
    for row in data_collected:
        patent_id = row[id_col]
        text = row[text_col]
    
        # Extract keywords
        keywords = kw_model.extract_keywords(
            text,
            keyphrase_ngram_range=(1, 1),
            top_n=num_keywords
        )

        # Append results
        results.append((patent_id, [(kw[0], kw[1]) for kw in keywords]))

    # Convert results back to Spark DataFrame
    result_df = spark.createDataFrame(results, [id_col, "keywords"])
    return result_df




# Apply keyword extraction for all files (splitted into 10 parts)

def list_files_in_directory(directory_path, extension):
    try:
        files = [
            f for f in os.listdir(directory_path)
            if os.path.isfile(os.path.join(directory_path, f))  
            and (extension == '' or f.endswith(extension))  
            and not f.startswith('.')  # Exclude hidden files
            and not f.endswith('.crc')  # Exclude .crc files
            and os.path.getsize(os.path.join(directory_path, f)) > 0  # Exclude empty files
        ]
        return files
    except FileNotFoundError:
        print(f"The directory {directory_path} was not found.")
        return []
    except PermissionError:
        print(f"Permission denied to access the directory {directory_path}.")
        return []

def extract_keywords_save_to_file(input_file):

    print(input_file)
    
    lines_df = spark.read.text(input_file)
    
    # Skip the header and process the lines
    lines = [row["value"] for row in lines_df.collect()][1:]  
    
    # Extract patent_id and summary_text
    data = []
    current_id = None
    current_summary = []
    
    for line in lines:
        try:
            if line.startswith('"'):
                if current_id is not None and current_summary:
                    # Save the current record
                    data.append((current_id, " ".join(current_summary).strip()))
                # Extract the new patent_id
                current_id = line.split('"')[1]  
                # Extract the start of the summary text
                current_summary = [line.split('"', 2)[2].strip()] if '"' in line else []
            else:
                # Add subsequent lines to the summary
                current_summary.append(line.strip())
        except:
            pass
    
    # Append the last record
    if current_id is not None and current_summary:
        data.append((current_id, " ".join(current_summary).strip()))

    df = spark.createDataFrame(data, ["patent_id", "summary_text"])
    
    df = df.filter(col("patent_id") != '')
    
    filtered_df = df.filter(df["patent_id"].isin(patent_ids))
    print(filtered_df.count())

    keywords_df = extract_keywords_from_dataframe(
    spark_df=filtered_df,
    text_col="summary_text",
    id_col="patent_id",
    num_keywords=5
    )

    keywords_df.show(truncate=False)
   
    output_path = "data/keywords"
    keywords_df.write.mode("append").parquet(output_path)
    print(f"DataFrame {input_file} saved to {output_path}")


directory_path = 'data/summary_partitioned'
extension = ''
files = list_files_in_directory(directory_path, extension)
for file in files:
    extract_keywords_save_to_file(os.path.join(directory_path, file))







