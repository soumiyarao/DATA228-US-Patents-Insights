# DATA228-US-Patents-Insights
Insight into US Patents: An Analysis using NLP and Big Data Technologies

<b>poc-data-processing</b>: directory contains proof of concept test scripts for experimenting data processing, analytics and real time strem processing on the dataset

<b>final</b>: directory contains final code used for processing data and creating analytics data for dashboards and real time data integration to dashboard

<b>final/Preprocessing</b>: contains preprocessing pyspark scripts used to clean, filter, and integrate related tables from raw dataset and create preprocessed csv and Parquet/Hudi files

<b>final/KeywordsExtraction</b>: contains python scripts to extract weighted keywords from patent summary data using BERT based NLP model keyBERT

<b>final/Analytics</b>: contains pyspark scripts to apply transformations/aggregatations and generate analytics data to be used for dashboards (PowerBI and Apache Superset)

<b>final/RealTimeStreaming</b>: contains python script to fetch data from real-time API and also pyspark scripts to read and process data fecthed by API in  real time using Apache Structured Streaming and append the new data to historical analytics data

Sample analytics ready data are uploaded at:
- final/Analytics/PowerBI/csv/
- final/Analytics/Superset/results/

Sample Dashboard Screenshots are uploaded at:
- final/Analytics/PowerBI/
- final/Analytics/Superset/

<b>Tools and Technologies Used:</b>
- Apache Spark
- Apache Spark Structured Streaming
- Parquet
- Apache Hudi
- keyBERT
- Apache Hive
- Docker
- Apache Superset
- PowerBI