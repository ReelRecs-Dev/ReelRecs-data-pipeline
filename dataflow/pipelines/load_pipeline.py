import apache_beam as beam
from sqlalchemy import create_engine
from config.settings import DATABASE_URI

class LoadMovieLensData(beam.PTransform):
    def expand(self, pcoll):
        def load_to_db(data):
            engine = create_engine(DATABASE_URI)
            data['movies'].to_sql('movies', con=engine, if_exists='replace', index=False)
            data['ratings'].to_sql('ratings', con=engine, if_exists='replace', index=False)
            data['tags'].to_sql('tags', con=engine, if_exists='replace', index=False)

        return pcoll | "Load MovieLens Data to PostgreSQL" >> beam.Map(load_to_db)
