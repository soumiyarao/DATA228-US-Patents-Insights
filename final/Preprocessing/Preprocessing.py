from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, StringType
import re

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Preprocessing") \
    .config("spark.driver.memory", "8g").config("spark.executor.memory", "8g") \
    .getOrCreate()


# Processing g_application.tsv

application_file_path = "raw_data/g_application.tsv"

application_schema = StructType([
    StructField("application_id", StringType(), True), 
    StructField("patent_id", StringType(), True),      
    StructField("patent_application_type", StringType(), True),  
    StructField("filing_date", DateType(), True),       
    StructField("series_code", StringType(), True),    
    StructField("rule_47_flag", IntegerType(), True)    
])


application_df = spark.read.option("delimiter", "\t").option("header", "true").schema(application_schema).csv(application_file_path)
application_df.printSchema()
record_count = application_df.count()
print(f"Total number of records: {record_count}")
application_df.show(10)


def filter_valid_dates(df: DataFrame, date_column: str):
    valid_date_pattern = r"^(19|20)\d{2}-[0-1]\d-[0-3]\d$"    
    invalid_date_df = df.filter(~col(date_column).rlike(valid_date_pattern))
    invalid_date_count = invalid_date_df.count()
    valid_date_df = df.filter(col(date_column).rlike(valid_date_pattern))
    print(f"Number of records with incorrect dates: {invalid_date_count}")
    print("Top 10 records with valid dates:")
    valid_date_df.show(10)
    return valid_date_df



application_valid_date_df = filter_valid_dates(application_df, "filing_date")



def drop_columns(df: DataFrame, columns_to_drop: list) -> DataFrame:
    dropped_df = df.drop(*columns_to_drop)
    dropped_df.show(10)
    return dropped_df



columns_to_drop = ["rule_47_flag", "patent_application_type"]  
application_df_dropped = drop_columns(application_valid_date_df, columns_to_drop)


# Processing g_patent.tsv

patent_file_path = "raw_data/g_patent.tsv"

patent_schema = StructType([
    StructField("patent_id", StringType(), True),       
    StructField("patent_type", StringType(), True),     
    StructField("patent_date", DateType(), True),        
    StructField("patent_title", StringType(), True),    
    StructField("wipo_kind", StringType(), True),        
    StructField("num_claims", IntegerType(), True),      
    StructField("withdrawn", IntegerType(), True),      
    StructField("filename", StringType(), True)         
])


patent_df = spark.read.option("delimiter", "\t").option("header", "true").schema(patent_schema).csv(patent_file_path)
patent_df.printSchema()
record_count = patent_df.count()
print(f"Total number of records: {record_count}")
patent_df.show(10)

patent_valid_date_df = filter_valid_dates(patent_df, "patent_date")

columns_to_drop = ["wipo_kind", "withdrawn", "filename"]  
patent_df_dropped = drop_columns(patent_valid_date_df, columns_to_drop)


key_column = "patent_id"
patent_info_joined_df = patent_df_dropped.join(application_df_dropped, patent_df_dropped[key_column] == application_df_dropped[key_column], "inner")
patent_info_joined_df = patent_info_joined_df.drop(application_df_dropped[key_column])
record_count = patent_info_joined_df.count()
print(f"Total number of records: {record_count}")
patent_info_joined_df.printSchema()
patent_info_joined_df.show(10)


# Processing g_cpc_current.tsv

cpc_file_path = "raw_data/g_cpc_current.tsv"

cpc_schema = StructType([
    StructField("patent_id", StringType(), True),          
    StructField("cpc_sequence", IntegerType(), True),     
    StructField("cpc_section", StringType(), True),       
    StructField("cpc_class", StringType(), True),          
    StructField("cpc_subclass", StringType(), True),      
    StructField("cpc_group", StringType(), True),          
    StructField("cpc_type", StringType(), True)            
])


cpc_df = spark.read.option("delimiter", "\t").option("header", "true").schema(cpc_schema).csv(cpc_file_path)
cpc_df.printSchema()
record_count = cpc_df.count()
print(f"Total number of records: {record_count}")
cpc_df.show(10)


# Processing g_cpc_title.tsv

cpc_title_file_path = "raw_data/g_cpc_title.tsv"

cpc_title_schema = StructType([
    StructField("cpc_subclass", StringType(), True),         
    StructField("cpc_subclass_title", StringType(), True),    
    StructField("cpc_group", StringType(), True),             
    StructField("cpc_group_title", StringType(), True),      
    StructField("cpc_class", StringType(), True),            
    StructField("cpc_class_title", StringType(), True)        
])


cpc_title_df = spark.read.option("delimiter", "\t").option("header", "true").schema(cpc_title_schema).csv(cpc_title_file_path)
cpc_title_df.printSchema()
record_count = cpc_title_df.count()
print(f"Total number of records: {record_count}")
cpc_title_df.show(10)


schema = StructType([
    StructField("cpc_section", StringType(), True),
    StructField("section_description", StringType(), True)
])

data = [
    ("A", "Human Necessities"),
    ("B", "Performing Operations; Transporting"),
    ("C", "Chemistry; Metallurgy"),
    ("D", "Textiles; Paper"),
    ("E", "Fixed Constructions"),
    ("F", "Mechanical Engineering; Lighting; Heating; Weapons; Blasting Engines or Pumps"),
    ("G", "Physics"),
    ("H", "Electricity"),
    ("Y", "General Tagging of New Technological Developments")
]

cpc_section_df = spark.createDataFrame(data, schema)
cpc_section_df.show(truncate=False)




cpc_group_df = cpc_title_df.select("cpc_group", "cpc_group_title").distinct()
cpc_subclass_df = cpc_title_df.select("cpc_subclass", "cpc_subclass_title").distinct()
cpc_class_df = cpc_title_df.select("cpc_class", "cpc_class_title").distinct()

print("Unique CPC Groups:")
cpc_group_df.show(10)

print("Unique CPC Subclasses:")
cpc_subclass_df.show(10)

print("Unique CPC Classes:")
cpc_class_df.show(10)


# Processing g_inventor_disambiguated.tsv

inventor_file_path = "raw_data/g_inventor_disambiguated.tsv"

inventor_schema = StructType([
    StructField("patent_id", StringType(), True),                     
    StructField("inventor_sequence", IntegerType(), True),            
    StructField("inventor_id", StringType(), True),                    
    StructField("disambig_inventor_name_first", StringType(), True),  
    StructField("disambig_inventor_name_last", StringType(), True),   
    StructField("gender_code", StringType(), True),                    
    StructField("location_id", StringType(), True)                    
])

inventor_df = spark.read.option("delimiter", "\t").option("header", "true").schema(inventor_schema).csv(inventor_file_path)
inventor_df.printSchema()
record_count = inventor_df.count()
print(f"Total number of records: {record_count}")
inventor_df.show(10)


# Processing g_location_disambiguated.tsv

location_file_path = "raw_data/g_location_disambiguated.tsv"

location_schema = StructType([
    StructField("location_id", StringType(), True),                
    StructField("disambig_city", StringType(), True),             
    StructField("disambig_state", StringType(), True),             
    StructField("disambig_country", StringType(), True),          
    StructField("latitude", FloatType(), True),                    
    StructField("longitude", FloatType(), True),                   
    StructField("county", StringType(), True),                     
    StructField("state_fips", StringType(), True),               
    StructField("county_fips", StringType(), True)                
])

location_df = spark.read.option("delimiter", "\t").option("header", "true").schema(location_schema).csv(location_file_path)
location_df.printSchema()
record_count = location_df.count()
print(f"Total number of records: {record_count}")
location_df.show(10)


columns_to_drop = ["state_fips", "county_fips"]  
location_df_dropped = drop_columns(location_df, columns_to_drop)


key_column = "location_id"
joined_df_inventor = inventor_df.join(location_df_dropped, inventor_df[key_column] == location_df_dropped[key_column], "left")
joined_df_inventor = joined_df_inventor.drop(location_df_dropped[key_column])
joined_df_inventor.printSchema()
record_count = joined_df_inventor.count()
print(f"Total number of records: {record_count}")
joined_df_inventor.show(10)



# Processing g_applicant_not_disambiguated

applicant_file_path = "raw_data/g_applicant_not_disambiguated.tsv"

applicant_schema = StructType([
    StructField("patent_id", StringType(), True),                     
    StructField("applicant_sequence", IntegerType(), True),           
    StructField("raw_applicant_name_first", StringType(), True),       
    StructField("raw_applicant_name_last", StringType(), True),        
    StructField("raw_applicant_organization", StringType(), True),     
    StructField("applicant_type", StringType(), True),                
    StructField("applicant_designation", StringType(), True),          
    StructField("applicant_authority", StringType(), True),            
    StructField("rawlocation_id", StringType(), True)                 
])



applicant_df = spark.read.option("delimiter", "\t").option("header", "true").schema(applicant_schema).csv(applicant_file_path)
applicant_df.printSchema()
record_count = applicant_df.count()
print(f"Total number of records: {record_count}")
applicant_df.show(10)

columns_to_drop = ["applicant_authority", "rawlocation_id", "applicant_designation"]  
applicant_df_dropped = drop_columns(applicant_df, columns_to_drop)


def save_dfs_as_parquet_with_names(dfs, directory_path, df_names):
    
    for df, df_name in zip(dfs, df_names):
        file_path = f"{directory_path}/{df_name}"
        
        try:
            df.write.parquet(file_path, mode="overwrite")
            print(f"DataFrame '{df_name}' saved successfully to {file_path}")
        except Exception as e:
            print(f"Error saving DataFrame '{df_name}' to {file_path}: {e}")


save_dfs_as_parquet_with_names([patent_info_joined_df, cpc_df, cpc_section_df, cpc_group_df,cpc_subclass_df, cpc_class_df, joined_df_inventor, applicant_df_dropped], "preprocessed_data", 
                               ["patent_info", "cpc_info", "cpc_section", "cpc_group", "cpc_subclass", "cpc_class", "inventor_info", "applicant_info"])





