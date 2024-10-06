import os

def get_env_var(key, default=None):
    return os.getenv(key, default)

def download_and_extract(url, extract_to='/tmp'):
    """Helper function to download and extract datasets"""
    response = requests.get(url)
    with zipfile.ZipFile(BytesIO(response.content)) as z:
        z.extractall(extract_to)
