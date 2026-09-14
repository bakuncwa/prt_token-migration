"""Token Migration ETL: raw CSV (GCS) -> transform -> transformed CSV (GCS).

Mirrors the "Token Migration ETL" diagram, minus the Cloud SQL staging
step: raw and transformed datasets both live in the same GCS bucket, and
the rename/split rules from "Data Transformation_TokenMigration.xlsx" are
applied in-memory (see column_mapping.py).

Required environment variables:
  GCS_BUCKET   e.g. <BUCKET_NAME>

Optional:
  RAW_BLOB_NAME           default "raw/May 2025.csv"
  TRANSFORMED_BLOB_NAME   default "transformed/Transformed_May_2025.csv"

Usage:
  GCS_BUCKET=<BUCKET_NAME> python transform_load_gcs.py
"""

from __future__ import annotations

import io
import os
import sys

import pandas as pd
from google.cloud import storage

from column_mapping import transform_dataframe


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def download_raw_csv(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise SystemExit(f"Raw object not found: gs://{bucket.name}/{blob_name}")
    data = blob.download_as_bytes()
    # dtype=str + keep_default_na=False: this is payment/PII data, we do not
    # want pandas silently coercing types (e.g. Bin -> int, dropping leading
    # zeros) or turning blank fields into NaN.
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def upload_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"


def main() -> None:
    bucket_name = env("GCS_BUCKET", required=True)
    raw_blob_name = env("RAW_BLOB_NAME", default="raw/May 2025.csv")
    transformed_blob_name = env(
        "TRANSFORMED_BLOB_NAME", default="transformed/Transformed_May_2025.csv"
    )

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    print(f"Downloading raw CSV from gs://{bucket_name}/{raw_blob_name} ...")
    raw_df = download_raw_csv(bucket, raw_blob_name)
    print(f"  {len(raw_df)} rows, {len(raw_df.columns)} columns")

    print("Transforming (rename/split per Data Transformation_TokenMigration.xlsx) ...")
    transformed_df = transform_dataframe(raw_df)
    print(f"  {len(transformed_df)} rows, {len(transformed_df.columns)} columns")

    uri = upload_csv(bucket, transformed_blob_name, transformed_df)
    print(f"Transformed dataset written to {uri}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
