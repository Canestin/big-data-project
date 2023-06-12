# Chargement des données formatées
formatted_imdb_movies = spark.read.parquet('formatted_imdb_movies.parquet')
formatted_tmdb_movies = spark.read.parquet('formatted_tmdb_movies.parquet')

# Combinaison des données
combined_data = formatted_data_1.join(formatted_data_2, formatted_data_1.title == formatted_data_2.title)

# Enregistrement des données d'utilisation
combined_data.write.parquet('usage_movies.parquet')
