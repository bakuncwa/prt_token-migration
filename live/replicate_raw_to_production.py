"""Cloud Function (Generation 2): mirrors newly uploaded raw CSVs from
the live pipeline's bucket into the production pipeline's dedicated
bucket, merchant-namespaced under raw/<PRODUCTION_MERCHANT>/.

Trigger: google.cloud.storage.object.v1.finalized on the live bucket.
Entry point: on_raw_uploaded_replicate

This exists so that the reference merchant (configs/pilot.json, ported
one-to-one from this same live pipeline) continues to exercise
production/'s generalized staging_service.py against authentic data,
without requiring raw/<merchant>/ objects to ever be written into the
live bucket itself -- production/staging_service.py:on_raw_uploaded
expects a merchant subfolder that the live bucket's flat raw/ layout
does not, and cannot safely, provide (the live bucket's own
transform-on-raw-upload is triggered bucket-wide on any raw/ upload,
irrespective of subfolder).

Deployment (Generation 2, executed from the repository root). The Python
Cloud Functions buildpack requires the entry-point file to be named
`main.py` at the source root; since this module has no local
dependency on the rest of live/, a minimal staging directory re-exporting
its single entry point is sufficient, following the identical pattern
production/'s SETUP.md step employs for staging_service.py:
  STAGE=$(mktemp -d)
  cp live/replicate_raw_to_production.py live/requirements.txt "$STAGE/"
  echo 'from replicate_raw_to_production import on_raw_uploaded_replicate  # noqa: F401' \\
    > "$STAGE/main.py"

  gcloud functions deploy replicate-raw-to-production \\
    --gen2 --runtime=python312 --region=<REGION> \\
    --source="$STAGE" --entry-point=on_raw_uploaded_replicate \\
    --trigger-bucket=<LIVE_BUCKET_NAME> \\
    --set-env-vars=PRODUCTION_BUCKET=<PRODUCTION_BUCKET_NAME>,PRODUCTION_MERCHANT=pilot \\
    --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3
"""

from __future__ import annotations

import os

import functions_framework
from cloudevents.http import CloudEvent
from google.cloud import storage

RAW_PREFIX = "raw/"


def env(name: str) -> str:
    """Retrieves environment variable `name`, raising explicitly if it
    is absent."""
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


@functions_framework.cloud_event
def on_raw_uploaded_replicate(event: CloudEvent) -> None:
    """Cloud Run function entry point invoked upon the finalization of
    an object within the live bucket; ignores any object outside
    raw/, and copies every raw/*.csv object into
    gs://PRODUCTION_BUCKET/raw/PRODUCTION_MERCHANT/<same filename>."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not blob_name.startswith(RAW_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {RAW_PREFIX} CSV)")
        return

    production_bucket_name = env("PRODUCTION_BUCKET")
    production_merchant = env("PRODUCTION_MERCHANT")

    client = storage.Client()
    source_bucket = client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = client.bucket(production_bucket_name)
    filename = blob_name.removeprefix(RAW_PREFIX)
    destination_blob_name = f"{RAW_PREFIX}{production_merchant}/{filename}"

    source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    print(
        f"Replicated gs://{bucket_name}/{blob_name} -> "
        f"gs://{production_bucket_name}/{destination_blob_name}"
    )
