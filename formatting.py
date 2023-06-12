from pyspark.sql import SparkSession

# Create an instance of the Spark session
spark = SparkSession.builder.appName("MoviesData").getOrCreate()

# Load IMDb data from CSV file
imdb_data = spark.read.csv('data/sources/imdb_movies.csv', header=True)

# Load TMDB data from CSV file
tmdb_data = spark.read.csv('data/sources/tmdb_movies.csv', header=True)

# Select only necessary columns from IMDb data
imdb_data = imdb_data.select("movie_name", "year", "rating")

# Select only necessary columns from TMDB data
tmdb_data = tmdb_data.select("title", "popularity", "vote_average")
