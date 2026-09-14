"""Extracts raw MIT card data from DigitalOcean and stages it within GCS.

Template/placeholder exclusively -- no live DigitalOcean API
credentials are configured within this environment (see the "Can
automate extraction from DigitalOcean" annotation upon the ETL
diagram), such that fetch_mit_records() cannot yet be exercised end
to end. This documents the intended request specification, such that
it constitutes a drop-in replacement once DigitalOcean API access has
been granted.

Mirrors the leftmost leg of the "Token Migration ETL" diagram:
  DigitalOcean MIT Database --{GET}--> DigitalOcean Kubernetes API
    --{GET}--> GCS Bucket (raw/)

The landing of a file under raw/ constitutes the trigger for the
remainder of the pipeline: cf_transform_on_upload.py is invoked
automatically the instant this script (or any other process) uploads
a new raw CSV thereto.

Required environment variables:
  DIGITALOCEAN_TOKEN   DigitalOcean API token (scope: read)
  DO_CLUSTER_NAME       DOKS cluster name or identifier fronting the MIT Database
  GCS_BUCKET            for example, <BUCKET_NAME>

Optional environment variable:
  RAW_BLOB_NAME   default "raw/<Month YYYY>.csv"

Usage (once DIGITALOCEAN_TOKEN access has been provisioned):
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
    """Retrieves environment variable `name`, raising explicitly if
    it is designated as required and absent."""
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def fetch_mit_records(token: str, cluster: str) -> pd.DataFrame:
    """Retrieves the raw MIT PAN dataset via a GET request against the
    DigitalOcean Kubernetes API.

    Placeholder: DigitalOcean API access has not yet been provisioned;
    accordingly, this raises explicitly rather than presuming an
    endpoint specification. Replace the body once the authentic
    DOKS-fronted MIT Database read endpoint is known, for example:

        resp = requests.get(
            f"https://{cluster}.k8s.ondigitalocean.com/mit/records",
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return pd.DataFrame(resp.json())
    """
    raise SystemExit(
        "extract_digitalocean.py constitutes a placeholder -- no "
        "DigitalOcean API access is presently configured. Implement "
        "fetch_mit_records() once DIGITALOCEAN_TOKEN access has been granted."
    )


def upload_raw_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    """Serializes and uploads a DataFrame as a CSV object, returning
    the resultant gs:// URI."""
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"


def main() -> None:
    token = env("DIGITALOCEAN_TOKEN", required=True)
    cluster = env("DO_CLUSTER_NAME", required=True)
    bucket_name = env("GCS_BUCKET", required=True)
    raw_blob_name = env("RAW_BLOB_NAME", default=f"raw/{dt.date.today():%B %Y}.csv")

    print(f"Fetching MIT records via the DigitalOcean Kubernetes API (cluster {cluster}) ...")
    df = fetch_mit_records(token, cluster)
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    uri = upload_raw_csv(bucket, raw_blob_name, df)
    print(f"Raw dataset staged to {uri}")
    print("The landing of this file under raw/ automatically triggers cf_transform_on_upload.py.")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
