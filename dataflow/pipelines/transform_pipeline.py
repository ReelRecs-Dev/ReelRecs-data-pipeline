class TransformMovieLensData(beam.PTransform):
    def expand(self, pcoll):
        def transform_data(data):
            # Transform movies
            movies = data['movies']
            movies['year'] = movies['title'].str.extract(r"\((\d{4})\)").fillna(0).astype(int)
            movies['genres'] = movies['genres'].str.split('|')

            # Transform ratings
            ratings = data['ratings']
            ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')

            # Transform tags
            tags = data['tags']
            tags['tag'] = tags['tag'].str.lower()

            return {
                'movies': movies,
                'ratings': ratings,
                'tags': tags
            }

        return pcoll | "Transform MovieLens Data" >> beam.Map(transform_data)
