"""
    Script to upload prepared data files to Google Cloud Storage (GCS).

    This script uploads the transformed files from data/prepared/ to a
    GCS bucket, preserving the folder structure so that BigQuery can
    use wildcard URIs to create external tables across multiple files.

    Prerequisites:
        - Run `gcloud auth application-default login` to authenticate.
        - Create a GCS bucket (manually or in this script).

    Usage:
        python scripts/03_upload_to_gcs.py
"""

import pathlib
from google.cloud import storage


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

BUCKET_NAME = 'musa5090-s26-kavan-data'


def upload_prepared_data():
    """Upload all prepared data files to GCS.

    Uploads the contents of data/prepared/ to the GCS bucket,
    preserving the folder structure under a prefix of 'air_quality/'.

    Expected GCS structure:
        gs://<bucket>/air_quality/hourly/2024-07-01.csv
        gs://<bucket>/air_quality/hourly/2024-07-01.jsonl
        gs://<bucket>/air_quality/hourly/2024-07-01.parquet
        ...
        gs://<bucket>/air_quality/sites/site_locations.csv
        gs://<bucket>/air_quality/sites/site_locations.jsonl
        gs://<bucket>/air_quality/sites/site_locations.geoparquet
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    prepared_dir = DATA_DIR / 'prepared'
    files = [f for f in prepared_dir.rglob('*') if f.is_file()]

    for file_path in files:
        relative = file_path.relative_to(prepared_dir)
        blob_name = 'air_quality/' + '/'.join(relative.parts)
        blob = bucket.blob(blob_name)
        print(f'Uploading {file_path.name} -> gs://{BUCKET_NAME}/{blob_name}')
        blob.upload_from_filename(str(file_path))

    print(f'Uploaded {len(files)} files.')


if __name__ == '__main__':
    upload_prepared_data()
    print('Done.')
