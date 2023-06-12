import ingestion_tmdb
import ingestion_imdb
import formatting
import combination
import indexing

# Étape 2.1 : Ingestion
# ingestion_tmdb.extract_data()

# Should no longer pass (the big_imdb folder was deleted because of its huge size)
# ingestion_imdb.extract_data()

# Étape 2.3 : Combinaison
combination.combine_data()

# Étape 2.4 : Indexation
indexing.index_data()
