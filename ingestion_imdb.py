import pandas as pd
import glob

def extract_data():
    # config
    input_directory = 'data/big_imdb'
    output_file = 'data/sources/imdb_movies.csv'

    # Read and load the data from each CSV file
    file_paths = glob.glob(input_directory + '/*.csv')
    dfs = []
    for file_path in file_paths:
        df = pd.read_csv(file_path)
        dfs.append(df)

    # Concatenate all dataframes into a single dataframe
    combined_df = pd.concat(dfs)

    # Sort the combined dataframe by the "rating" column
    sorted_df = combined_df.sort_values('rating', ascending=False)

    # Select the top 20000 movies
    movies = sorted_df.head(20000)

    # Save the resulting dataframe to a CSV file
    movies.to_csv(output_file, index=False)

    print("The movie data has been saved in the file 'imdb_movies.csv'.")
