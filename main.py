import ingestion_tmdb
import ingestion_imdb
import formatting
import combination
import indexing

# Étape 2.1 : Ingestion
ingestion_twitter.extract_data()
ingestion_imdb.extract_data()

# Étape 2.2 : Formatage
formatting.format_data()

# Étape 2.3 : Combinaison
combination.combine_data()

# Étape 2.4 : Indexation
indexing.index_data()
