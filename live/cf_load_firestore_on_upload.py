"""Cloud Function (Generation 2): automatically loads transformed CSVs
into Firestore.

Trigger: google.cloud.storage.object.v1.finalized on GCS_BUCKET.
Entry point: on_transformed_uploaded

Supersedes the manual "execute load_to_firestore.py" procedure: the
instant a transformed CSV is uploaded under transformed/ (by
cf_transform_on_upload.py), Eventarc invokes this function, which
reuses the identical download_csv() and load_dataframe() components
employed by load_to_firestore.py to perform an upsert into Firestore
-- no script invocation is required. Per load_dataframe(), exclusively
card.number is written (indexed by card.id); every other column
remains within the transformed CSV and is never transmitted to
Firestore.

Explicitly disregards cleaned/: this prefix holds PaaS-worker-reconciled
exports, which constitute the *output* of manual review rather than
input to it, and are handled instead by
export_firestore_to_gcs.py's on_firestore_write.

Loads into a dated MMYYYY_cardholders collection derived from the
uploaded filename (see collection_naming.py), such that each month's
data is assigned its own collection -- override via
FIRESTORE_COLLECTION for a fixed identifier instead.

Deployment (Generation 2, executed from the scripts/ directory).
--min-instances=0 together with a low --max-instances ceiling maintain
this function within the Cloud Run/Cloud Functions Always Free tier
(2M requests, 360K GB-seconds, 180K vCPU-seconds per month) -- this
bucket receives, at most, a handful of uploads monthly:
  gcloud functions deploy load-firestore-on-transformed-upload \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_transformed_uploaded \\
    --trigger-bucket=<BUCKET_NAME> \\
    --set-env-vars=FIRESTORE_DATABASE="(default)" \\
    --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3
"""

from __future__ import annotations

import os

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import firestore, storage

from collection_naming import collection_for_blob_name
from load_to_firestore import download_csv, load_dataframe

TRANSFORMED_PREFIX = "transformed/"
CLEANED_PREFIX = "cleaned/"


@functions_framework.cloud_event
def on_transformed_uploaded(event: CloudEvent) -> None:
    """Cloud Run function entry point invoked upon the finalization of
    an object within GCS_BUCKET; ignores any object outside
    transformed/, and any object within cleaned/."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if blob_name.startswith(CLEANED_PREFIX):
        return  # handled instead by the PaaS reconciliation pathway
    if not blob_name.startswith(TRANSFORMED_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {TRANSFORMED_PREFIX} CSV)")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    print(f"New transformed file detected: gs://{bucket_name}/{blob_name}")
    df = download_csv(bucket, blob_name)
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    collection = os.environ.get("FIRESTORE_COLLECTION") or collection_for_blob_name(blob_name)
    database = os.environ.get("FIRESTORE_DATABASE", "(default)")
    db = firestore.Client(database=database)

    count = load_dataframe(df, db, collection)
    print(f'Automatically loaded {count} documents into "{collection}" (database "{database}")')
