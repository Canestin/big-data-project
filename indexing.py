from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

# Crée une instance de la session Spark
spark = SparkSession.builder.appName("ElasticsearchIndexing").getOrCreate()

# Charge les données formatées depuis le fichier Parquet
df_formatted = spark.read.parquet('formatted_imdb_movies.parquet')

# Configure la connexion Elasticsearch
es = Elasticsearch(hosts="https://localhost:9200", basic_auth=("elastic", "Ti7TIT-7QW1DedpxTe87"), verify_certs=False)

# Convertit le DataFrame Spark en liste de dictionnaires
data = df_formatted.toJSON().map(lambda x: eval(x)).collect()

# Indexe les données dans Elasticsearch
index_name = 'movies_index'

for i, doc in enumerate(data):
    es.index(index=index_name, body=doc, id=i+1)


