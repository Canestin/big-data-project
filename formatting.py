from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Création de la session Spark
spark = SparkSession.builder.appName("MovieData").getOrCreate()

# Chargement des données IMDB
df_imdb = spark.read.csv("imdb_movies.csv", header=True, inferSchema=True)

# Chargement des données TMDB
df_tmdb = spark.read.csv("tmdb_movies.csv", header=True, inferSchema=True)

# Sélection des colonnes pertinentes et renommage
df_imdb_selected = df_imdb.select("movie_name", "year", "rating", "director")
df_tmdb_selected = df_tmdb.select("original_title", "release_date", "vote_average")

# Renommage des colonnes pour faciliter la combinaison
df_imdb_selected = df_imdb_selected.withColumnRenamed("movie_name", "title")
df_tmdb_selected = df_tmdb_selected.withColumnRenamed("original_title", "title")

# Combinaison des données en utilisant la colonne "title" comme clé de jointure
df_combined = df_imdb_selected.join(df_tmdb_selected, on="title", how="inner")

# Filtrage des enregistrements où la note IMDB est inférieure à 5
df_combined_filtered = df_combined.filter(df_combined["rating"] >= 5)



# Calcul de la moyenne des notes par année
df_avg_rating_by_year = df_combined_filtered.groupBy("year").agg(avg("rating").alias("avg_rating"))

# Tri des résultats par année
df_avg_rating_by_year = df_avg_rating_by_year.orderBy("year")

# Calcul de la moyenne des notes par réalisateur
df_avg_rating_by_director = df_combined_filtered.groupBy("director").agg(avg("rating").alias("avg_rating"))

# Tri des résultats par note moyenne
df_avg_rating_by_director = df_avg_rating_by_director.orderBy("avg_rating", ascending=False)


# Enregistrement des données analysées au format CSV
df_avg_rating_by_year.write.csv("avg_rating_by_year.csv", header=True)
df_avg_rating_by_director.write.csv("avg_rating_by_director.csv", header=True)
