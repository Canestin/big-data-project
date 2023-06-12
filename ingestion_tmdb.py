import pandas as pd
import requests

def extract_data():
    # Query Parameters
    api_key = 'a08d121276c3ae952f538cdb7d76d20c'
    # api_key = 'YOUR_TMDB_API_KEY'
    base_url = 'https://api.themoviedb.org/3/discover/movie'
    params = {
        'api_key': api_key,
        'page': 1
    }

    # List to store data of each movie
    films = []

    # Loop to retrieve data from each page
    for page in range(1, 501):
        params['page'] = page

        response = requests.get(base_url, params=params)
        data = response.json()

        if response.status_code == 200:
            results = data['results']
            films.extend(results)
            print(f"Page {page} récupérée avec succès.")
        else:
            print(f"Erreur lors de la récupération de la page {page}.")

    # Creation of the pandas dataframe with the movies data
    df = pd.DataFrame(films, columns=["original_title", "overview", "popularity", "release_date", "title", "vote_average", "vote_count"])

    # Saving the dataframe in a CSV file
    df.to_csv('data/sources/tmdb_movies.csv', index=False)

    print("The movie data has been saved in the file 'tmdb_movies.csv'.")
