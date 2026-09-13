"""Extract raw MIT card data from DigitalOcean and stage it in GCS.

Template/placeholder only -- no live DigitalOcean API credentials are
configured in this environment (see the "Can automate extraction from
DigitalOcean" note on the ETL diagram), so fetch_mit_records() cannot be
exercised end-to-end yet. It documents the intended request shape so
this is a drop-in once DigitalOcean API access is granted.

Mirrors the leftmost leg of the "Token Migration ETL" diagram:
  DigitalOcean MIT Database --{GET}--> DigitalOcean Kubernetes API
    --{GET}--> GCS Bucket (raw/)

Landing a file under raw/ is the trigger for the rest of the pipeline:
cf_transform_on_upload.py fires automatically the moment this script (or
any other process) uploads a new raw CSV there.

Required environment variables:
  DIGITALOCEAN_TOKEN   DO API token (scope: read)
  DO_CLUSTER_NAME       DOKS cluster name or ID fronting the MIT Database
  GCS_BUCKET            e.g. cardcorp-token-0dc1f93138

Optional:
  RAW_BLOB_NAME   default "raw/<Month YYYY>.csv"

Usage (once DIGITALOCEAN_TOKEN access exists):
  DIGITALOCEAN_TOKEN=... DO_CLUSTER_NAME=... GCS_BUCKET=... \\
  python extract_digitalocean.py
"""

from __future__ import annotations

import datetime as dt
import os
import sys

import pandas as pd
from google.cloud import storage


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def fetch_mit_records(token: str, cluster: str) -> pd.DataFrame:
    """GET the raw MIT PAN dataset through the DigitalOcean Kubernetes API.

    Placeholder: DigitalOcean API access has not been provisioned yet, so
    this raises instead of guessing at an endpoint shape. Replace the body
    once the actual DOKS-fronted MIT Database read endpoint is known, e.g.:

        resp = requests.get(
            f"https://{cluster}.k8s.ondigitalocean.com/mit/records",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    """
    raise SystemExit(
        "extract_digitalocean.py is a placeholder -- no DigitalOcean API "
        "access is configured yet. Implement fetch_mit_records() once "
        "DIGITALOCEAN_TOKEN access is granted."
    )


def upload_raw_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"


def main() -> None:
    token = env("DIGITALOCEAN_TOKEN", required=True)
    cluster = env("DO_CLUSTER_NAME", required=True)
    bucket_name = env("GCS_BUCKET", required=True)
    raw_blob_name = env("RAW_BLOB_NAME", default=f"raw/{dt.date.today():%B %Y}.csv")

    print(f"Fetching MIT records via DigitalOcean Kubernetes API (cluster {cluster}) ...")
    df = fetch_mit_records(token, cluster)
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    uri = upload_raw_csv(bucket, raw_blob_name, df)
    print(f"Raw dataset staged to {uri}")
    print("Landing this file under raw/ triggers cf_transform_on_upload.py automatically.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
