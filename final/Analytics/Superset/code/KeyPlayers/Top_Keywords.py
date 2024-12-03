from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from nltk.corpus import stopwords
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag, word_tokenize


# Create a SparkSession
spark = SparkSession.builder.appName("PatentKeyPlayersAnalytics") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()


# Constants

filtered_patents_input_path = "../../data_source/filtered_patents"
keywords_data_path = "../../data_source/keywords"
top_keywords_path = "file:/Users/bhland/hive/warehouse/dashboard_analytics_results/top_keywords"


# lemmatization

lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words("english"))


def lemmatize_keyword(keyword):
    if keyword:
        tagged = pos_tag(word_tokenize(keyword))
        pos = tagged[0][1] if tagged else 'NN'  

        if pos.startswith('V'):
            pos = 'v'  
        elif pos.startswith('N'):
            pos = 'n'  
        elif pos.startswith('J'):
            pos = 'a' 
        else:
            pos = 'n'  
        
        return lemmatizer.lemmatize(keyword, pos)
    return None

# Define a UDF to apply lemmatization
lemmatize_keyword_udf = F.udf(lemmatize_keyword, StringType())


# Processing


# Read Filtered Patents
patents_filtered_df = spark.read.parquet(filtered_patents_input_path)
patents_filtered_df.show(truncate=False)


# Read Keywords
keywords_df = spark.read.parquet(keywords_data_path)
joined_df = keywords_df.join(patents_filtered_df, on="patent_id")
exploded_df = joined_df.withColumn("keyword_struct", F.explode(F.col("keywords")))
keyword_split_df = exploded_df.withColumn("keyword", F.col("keyword_struct._1")) \
    .withColumn("weight", F.col("keyword_struct._2"))


# Cleaning
keyword_normalized_df = keyword_split_df.withColumn("normalized_keyword", lemmatize_keyword_udf(F.col("keyword")))
keyword_cleaned_df = keyword_normalized_df.filter(
    F.col("normalized_keyword").isNotNull() & (F.col("normalized_keyword") != "") & 
    (F.trim(F.col("normalized_keyword")) != "")
)


# Aggregations to find top 20 based on count and weight
keyword_agg_df = keyword_cleaned_df.groupBy("branch", "normalized_keyword").agg(
    F.count("*").alias("keyword_count"),
    F.sum("weight").alias("total_weight")
)

keyword_rank_df = keyword_agg_df.withColumn(
    "ranking_metric", F.col("keyword_count") * F.col("total_weight")
)

window_spec = Window.partitionBy("branch").orderBy(F.desc("ranking_metric"))

ranked_df = keyword_rank_df.withColumn(
    "rank", F.row_number().over(window_spec)
).filter(F.col("rank") <= 50)

top_keywords_df = ranked_df.select("branch", "normalized_keyword", "keyword_count", "total_weight", "ranking_metric", "rank")
top_keywords_df.show(truncate=False)


# Final Result df
final_keywords_with_schema = top_keywords_df.select(
    col("branch").cast(StringType()),
    col("normalized_keyword").alias("keyword").cast(StringType()),
    col("keyword_count").cast(IntegerType()),
    col("rank").cast(IntegerType())
)

final_keywords_with_schema.show()

# Write to Parquet files
final_keywords_with_schema.write.mode("overwrite").parquet(top_keywords_path)
print("The Top Keywords results have been successfully written to the parquet file in hive warehouse.")





