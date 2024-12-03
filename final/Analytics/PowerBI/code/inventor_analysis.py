from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, avg, max, min, desc, collect_set, when, length

# Initialize Spark session
spark = SparkSession.builder \
    .appName("InventorInfoAnalysis") \
    .getOrCreate()

# Load inventor data
file_path = "A:/SJSU/Sem-2/DATA-228-Big Data/Project/output/inventor_info/*.csv"
df = spark.read.option("header", "true").csv(file_path, inferSchema=True)
df.printSchema()

# Remove rows where necessary columns are missing
df_clean = df.filter(col("disambig_inventor_name_first").isNotNull() & col("disambig_inventor_name_last").isNotNull())

#Top 20 inventors by distinct patent count
inventor_patent_count = df_clean.groupBy("inventor_id", "disambig_inventor_name_first", "disambig_inventor_name_last").agg(
  countDistinct("patent_id").alias("distinct_patent_count")
)
top_20_inventors = inventor_patent_count.orderBy(desc("distinct_patent_count")).limit(20)
top_20_inventors.coalesce(1).write.csv("top_20_inventors.csv", header=True, mode="overwrite")
print("top_20_inventors.csv saved successfully!")

#Count distinct inventors by country
country_inventors_count = df_clean.groupBy("disambig_country").agg(countDistinct("inventor_id").alias("num_inventors"))
country_inventors_count.write.parquet("inventor_country_count.parquet")
print("inventor_country_count.parquet saved successfully!")

#Count distinct inventors by state
state_inventors_count = df_clean.groupBy("disambig_state").agg(countDistinct("inventor_id").alias("num_inventors"))
state_inventors_count.write.parquet("inventor_state_count.parquet")
print("inventor_state_count.parquet saved successfully!")

#Average number of patents per inventor
avg_patents_per_inventor = df_clean.groupBy("inventor_id").agg(countDistinct("patent_id").alias("patent_count"))
avg_patents_per_inventor = avg_patents_per_inventor.agg(avg("patent_count").alias("avg_patents_per_inventor"))
avg_patents_per_inventor.write.csv("avg_patents_per_inventor.csv", header=True, mode="overwrite")
print("avg_patents_per_inventor saved successfully!")

#Total number of patents by inventor (distinct)
total_patents_by_inventor = df_clean.groupBy("inventor_id").agg(countDistinct("patent_id").alias("distinct_patent_count"))
total_patents_by_inventor.write.csv("total_patents_by_inventor.csv", header=True, mode="overwrite")
print("total_patents_by_inventor saved successfully!")

#Inventor count by gender
gender_inventors_count = df_clean.groupBy("gender_code").agg(countDistinct("inventor_id").alias("num_inventors"))
gender_inventors_count.write.csv("inventors_by_gender.csv", header=True, mode="overwrite")
print("inventors_by_gender saved successfully!")

# Count of patents by city
city_patents_count = df_clean.groupBy("disambig_city").agg(countDistinct("patent_id").alias("patent_count"))
city_patents_count.write.csv("patents_by_city.csv", header=True, mode="overwrite")
print("patents_by_city saved successfully!")

# Count of patents by state
state_patents_count = df_clean.groupBy("disambig_state").agg(countDistinct("patent_id").alias("patent_count"))
state_patents_count.write.csv("patents_by_state.csv", header=True, mode="overwrite")
print("patents_by_state saved successfully!")

# Count of patents by country
country_patents_count = df_clean.groupBy("disambig_country").agg(countDistinct("patent_id").alias("patent_count"))
country_patents_count.write.csv("patents_by_country.csv", header=True, mode="overwrite")
print("patents_by_country saved successfully!")

#Inventors who have patents in multiple countries
multi_country_inventors = df_clean.groupBy("inventor_id").agg(collect_set("disambig_country").alias("countries"))
multi_country_inventors = multi_country_inventors.filter(size("countries") > 1)
multi_country_inventors.write.csv("multi_country_inventors.csv", header=True, mode="overwrite")
print("multi_country_inventors saved successfully!")

#Average latitude and longitude of inventor locations
avg_lat_lon = df_clean.agg(
    avg("latitude").alias("avg_latitude"),
    avg("longitude").alias("avg_longitude")
)
avg_lat_lon.write.csv("avg_lat_lon.csv", header=True, mode="overwrite")
print("avg_lat_lon.csv saved successfully!")

#Top 10 inventors with the most patents in a specific city
city_top_inventors = df_clean.groupBy("disambig_city", "inventor_id").agg(countDistinct("patent_id").alias("patent_count"))
top_10_inventors_city = city_top_inventors.orderBy(desc("patent_count")).limit(10)
top_10_inventors_city.write.csv("top_10_inventors_city.csv", header=True, mode="overwrite")
print("top_10_inventors_city saved successfully!")

# Average patent count by city
avg_patents_by_city = df_clean.groupBy("disambig_city").agg(avg("patent_id").alias("avg_patent_count"))
avg_patents_by_city.write.csv("avg_patents_by_city.csv", header=True, mode="overwrite")
print("avg_patents_by_city saved successfully!")

#Inventors with the fewest patents
fewest_patents = df_clean.groupBy("inventor_id").agg(countDistinct("patent_id").alias("patent_count"))
fewest_patents = fewest_patents.orderBy("patent_count").limit(10)
fewest_patents.write.csv("fewest_patents_inventors.csv", header=True, mode="overwrite")
print("fewest_patents_inventors saved successfully!")

# Number of inventors by location (city, state, country)
location_inventors_count = df_clean.groupBy("disambig_city", "disambig_state", "disambig_country").agg(countDistinct("inventor_id").alias("num_inventors"))
location_inventors_count.write.csv("inventors_by_location.csv", header=True, mode="overwrite")
print("inventors_by_location saved successfully!")

# Gender-based patent count
gender_patent_count = df_clean.groupBy("gender_code").agg(countDistinct("patent_id").alias("patent_count"))
gender_patent_count.write.csv("gender_patent_count.csv", header=True, mode="overwrite")
print("gender_patent_count saved successfully!")

# Inventors with patents over multiple years
multi_year_inventors = df_clean.groupBy("inventor_id").agg(countDistinct("patent_date").alias("distinct_years"))
multi_year_inventors = multi_year_inventors.filter(col("distinct_years") > 1)
multi_year_inventors.write.csv("multi_year_inventors.csv", header=True, mode="overwrite")
print("multi_year_inventors saved successfully!")

# Most common first name of inventors
common_first_name = df_clean.groupBy("disambig_inventor_name_first").agg(countDistinct("inventor_id").alias("name_count"))
common_first_name = common_first_name.orderBy(desc("name_count")).limit(10)
common_first_name.write.csv("common_first_name_inventors.csv", header=True, mode="overwrite")
print("common_first_name_inventors saved successfully!")

#  Inventors with patents in the last 5 years
recent_inventors = df_clean.filter(col("patent_date").between("2018-01-01", "2023-12-31"))
recent_inventors_count = recent_inventors.groupBy("inventor_id").agg(countDistinct("patent_id").alias("patent_count"))
recent_inventors_count.write.csv("recent_inventors.csv", header=True, mode="overwrite")
print("recent_inventors saved successfully!")

# Top 10 countries with the most inventors
top_10_countries_inventors = country_inventors_count.orderBy(desc("num_inventors")).limit(10)
top_10_countries_inventors.write.csv("top_10_countries_inventors.csv", header=True, mode="overwrite")
print("top_10_countries_inventors saved successfully!")
print("All files saved successfully!")

# Stop the Spark session
spark.stop()
