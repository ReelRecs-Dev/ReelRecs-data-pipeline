import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from pipelines.extract_pipeline import ExtractMovieLensData
from pipelines.transform_pipeline import TransformMovieLensData
from pipelines.load_pipeline import LoadMovieLensData

def run():
    options = PipelineOptions(
        project="your-gcp-project-id",
        temp_location="gs://your-temp-bucket/temp",
        staging_location="gs://your-temp-bucket/staging"
    )

    with beam.Pipeline(options=options) as pipeline:
        (
            pipeline
            | "Extract Data" >> ExtractMovieLensData()
            | "Transform Data" >> TransformMovieLensData()
            | "Load Data to PostgreSQL" >> LoadMovieLensData()
        )

if __name__ == "__main__":
    run()
