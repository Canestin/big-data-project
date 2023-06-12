import formatting
from pyspark.sql.functions import col, round

def combine_data():
    imdb_data = formatting.imdb_data
    tmdb_data = formatting.tmdb_data

    # Perform data join on "original_title" column and rename columns
    combined_data = imdb_data.join(tmdb_data, imdb_data.movie_name == tmdb_data.title, "inner") \
        .select(imdb_data.movie_name, imdb_data.year,
                round((imdb_data.rating + tmdb_data.vote_average) / 2.0, 1).alias("average_rating"),
                tmdb_data.popularity) \
        .withColumnRenamed("movie_name", "title")

    # Remove duplicate rows based on "title" column
    combined_data = combined_data.dropDuplicates(["title"])

    # Save combined data in Parquet format
    combined_data.write.parquet("data/usage/combined_data.parquet", mode="overwrite")

    # Displays a preview of the combined data
    combined_data.show(100)

    print("The data was successfully combined")


