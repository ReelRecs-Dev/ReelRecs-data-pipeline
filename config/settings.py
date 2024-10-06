import os

# Environment Variables for secure storage
MOVIELENS_API_URL = os.getenv("MOVIELENS_API_URL", "http://files.grouplens.org/datasets/movielens/ml-latest-small.zip")
DATABASE_URI = os.getenv("DATABASE_URI", "postgresql://username:password@localhost/reelrecs")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
BUCKET_NAME = os.getenv("GCP_BUCKET_NAME", "your-dataflow-bucket")
