"""Cloud Function (2nd gen): auto-clean raw CSVs the moment they land in GCS.

Trigger: google.cloud.storage.object.v1.finalized on GCS_BUCKET.
Entry point: on_raw_uploaded

Replaces the manual "run transform_load_gcs.py" step: the instant a new
raw CSV is uploaded under raw/ (by extract_digitalocean.py, or a manual
drop), Eventarc fires this function, which reuses the same
download_raw_csv/transform_dataframe/upload_csv building blocks as
transform_load_gcs.py to write the transformed CSV to transformed/ -- no
script invocation required.

Deploy (2nd gen, from the scripts/ directory). --min-instances=0 and a
low --max-instances cap keep this inside the Cloud Run/Cloud Functions
Always Free tier (2M requests, 360K GB-seconds, 180K vCPU-seconds per
month) -- this bucket sees at most a handful of uploads a month:
  gcloud functions deploy transform-on-raw-upload \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_raw_uploaded \\
    --trigger-bucket=<BUCKET_NAME> \\
    --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3
"""

from __future__ import annotations

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import storage

from column_mapping import transform_dataframe
from transform_load_gcs import download_raw_csv, upload_csv

RAW_PREFIX = "raw/"
TRANSFORMED_PREFIX = "transformed/"


def _transformed_blob_name(raw_blob_name: str) -> str:
    """Mirrors the naming convention used by transform_load_gcs.py's
    default (raw "May 2025.csv" -> transformed "Transformed_May_2025.csv")."""
    filename = raw_blob_name.removeprefix(RAW_PREFIX)
    stem, _, ext = filename.rpartition(".")
    transformed_stem = f"Transformed_{stem.replace(' ', '_')}"
    return f"{TRANSFORMED_PREFIX}{transformed_stem}.{ext or 'csv'}"


@functions_framework.cloud_event
def on_raw_uploaded(event: CloudEvent) -> None:
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not blob_name.startswith(RAW_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {RAW_PREFIX} CSV)")
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    print(f"New raw file detected: gs://{bucket_name}/{blob_name}")
    raw_df = download_raw_csv(bucket, blob_name)
    print(f"  {len(raw_df)} rows, {len(raw_df.columns)} columns")

    transformed_df = transform_dataframe(raw_df)
    transformed_blob_name = _transformed_blob_name(blob_name)
    uri = upload_csv(bucket, transformed_blob_name, transformed_df)
    print(f"Auto-transformed {len(transformed_df)} rows -> {uri}")
