from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

def index_data():
    # Create an instance of the Spark session
    spark = SparkSession.builder.appName("ElasticsearchIndexing").getOrCreate()

    # Load formatted data from Parquet file
    df_formatted = spark.read.parquet("data/usage/combined_data.parquet")

    # Configure the Elasticsearch connection
    es = Elasticsearch(hosts="https://localhost:9200", basic_auth=("elastic", "Ti7TIT-7QW1DedpxTe87"), verify_certs=False)

    # Convert the Spark DataFrame to a list of dictionaries
    data = df_formatted.toJSON().map(lambda x: eval(x)).collect()

    # Index data in Elasticsearch
    index_name = 'movies_index'

    for i, doc in enumerate(data):
        es.index(index=index_name, body=doc, id=i+1)