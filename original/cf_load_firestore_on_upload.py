"""Cloud Function (2nd gen): auto-load transformed CSVs into Firestore.

Trigger: google.cloud.storage.object.v1.finalized on GCS_BUCKET.
Entry point: on_transformed_uploaded

Replaces the manual "run load_to_firestore.py" step: the instant a
transformed CSV is uploaded under transformed/ (by
cf_transform_on_upload.py), Eventarc fires this function, which reuses
the same download_csv/load_dataframe building blocks as
load_to_firestore.py to upsert into Firestore -- no script invocation
required. Per load_dataframe(), only card.number is written (keyed by
card.id); every other column stays in the transformed CSV and is never
sent to Firestore at all.

Explicitly ignores cleaned/: that prefix holds PaaS-worker-reconciled
exports, which are the *output* of manual review, not input to it, and
are handled by export_firestore_to_gcs.py's on_firestore_write instead.

Loads into a dated MMYYYY_cardholders collection derived from the
uploaded filename (see collection_naming.py), so each month's data gets
its own collection -- override with FIRESTORE_COLLECTION for a fixed
name instead.

Deploy (2nd gen, from the scripts/ directory). --min-instances=0 and a
low --max-instances cap keep this inside the Cloud Run/Cloud Functions
Always Free tier (2M requests, 360K GB-seconds, 180K vCPU-seconds per
month) -- this bucket sees at most a handful of uploads a month:
  gcloud functions deploy load-firestore-on-transformed-upload \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_transformed_uploaded \\
    --trigger-bucket=cardcorp-token-0dc1f93138 \\
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
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if blob_name.startswith(CLEANED_PREFIX):
        return  # handled by the PaaS reconciliation path instead
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
    print(f'Auto-loaded {count} documents into "{collection}" (database "{database}")')
