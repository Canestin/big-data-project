import pandas as pd
import requests

# Paramètres de la requête
api_key = 'a08d121276c3ae952f538cdb7d76d20c'
base_url = 'https://api.themoviedb.org/3/discover/movie'
params = {
    'api_key': api_key,
    'page': 1
}

# Liste pour stocker les données de chaque film
films = []

# Boucle pour récupérer les données de chaque page
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

# Création du dataframe pandas avec les données des films
df = pd.DataFrame(films, columns=["original_title", "overview", "popularity", "release_date", "title", "vote_average", "vote_count"])

# Enregistrement du dataframe dans un fichier CSV
df.to_csv('tmdb_movies.csv', index=False)

print("Les données des films ont été enregistrées dans le fichier 'tmdb_movies.csv'.")
