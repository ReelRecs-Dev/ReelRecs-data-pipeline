import requests
import zipfile
import os
import pandas as pd
from io import BytesIO

MOVIELENS_API_URL = "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

class ExtractMovieLensData(beam.PTransform):
    def expand(self, pcoll):
        def extract_data(_):
            response = requests.get(MOVIELENS_API_URL)
            with zipfile.ZipFile(BytesIO(response.content)) as z:
                z.extractall("/tmp/movielens")
            return {
                'movies': pd.read_csv("/tmp/movielens/movies.csv"),
                'ratings': pd.read_csv("/tmp/movielens/ratings.csv"),
                'tags': pd.read_csv("/tmp/movielens/tags.csv")
            }

        return pcoll | "Extract MovieLens Data" >> beam.Map(extract_data)
