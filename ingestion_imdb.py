import pandas as pd
import glob

# Step 1: Read and load the data from each CSV file
file_paths = glob.glob('data/imdb/*.csv')
dfs = []
for file_path in file_paths:
    df = pd.read_csv(file_path)
    dfs.append(df)

# Step 2: Concatenate all dataframes into a single dataframe
combined_df = pd.concat(dfs)

# Step 3: Sort the combined dataframe by the "rating" column
sorted_df = combined_df.sort_values('rating', ascending=False)

movies = sorted_df.head(10000)

# Save the resulting dataframe to a CSV file
movies.to_csv('imdb_movies.csv', index=False)
