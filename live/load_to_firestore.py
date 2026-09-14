"""Token Migration ETL: transformed CSV (GCS) -> Firestore (viewable and
editable within the GCP Console; no virtual machine required).

Loads exclusively card.number into Firestore, indexed by card.id --
every other column (name, address, transaction metadata, and so
forth) remains within the transformed CSV and is never written here.
This constitutes a deliberate architectural decision, not an
oversight: card.number is the sole field the automated transformation
procedure can never populate (see column_mapping.py -- the raw PAN is
blank within the source data), necessitating that a PaaS worker enter
it manually. By storing exclusively that one field, Firestore contains
nothing else for a worker to view or modify in the first instance --
no read-only lock or Security Rule is required to protect fields that
were never written there.

Firestore Native mode possesses a genuine Always Free daily quota
(1 GiB storage, 50,000 reads / 20,000 writes / 20,000 deletes per day)
together with a built-in Data viewer within the Cloud Console for
browsing and editing records -- no virtual machine, Identity-Aware
Proxy (IAP) tunnel, or password file is required, exclusively the
operator's standard GCP credentials.

Each monthly dataset is assigned its own collection, MMYYYY_cardholders,
derived from the transformed filename (see collection_naming.py) --
"transformed/Transformed_May_2025.csv" loads into "052025_cardholders"
-- such that the reloading of a prior month does not produce a
collision with the current month. Override via FIRESTORE_COLLECTION
should a fixed identifier be required instead.

The complete reconciled record (every transformed column plus
whatever value card.number ultimately holds in Firestore) is
reassembled at export time within export_firestore_to_gcs.py, through
the fresh reading of the transformed CSV and the merging of Firestore's
card.number value for each card.id.

Required environment variable:
  GCS_BUCKET     for example, <BUCKET_NAME>

Optional environment variables:
  TRANSFORMED_BLOB_NAME  default "transformed/Transformed_May_2025.csv"
  FIRESTORE_COLLECTION   default derived as MMYYYY_cardholders
  FIRESTORE_DATABASE     default "(default)"

Usage:
  GCS_BUCKET=... python load_to_firestore.py
"""

from __future__ import annotations

import io
import os
import sys

import pandas as pd
from google.cloud import firestore, storage

from collection_naming import collection_for_blob_name


def env(name: str, default: str | None = None, required: bool = False) -> str:
    """Retrieves environment variable `name`, raising explicitly if
    it is designated as required and absent."""
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def download_csv(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    """Downloads and parses a CSV object from the specified bucket."""
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise SystemExit(f"Object not found: gs://{bucket.name}/{blob_name}")
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def load_dataframe(df: pd.DataFrame, db: firestore.Client, collection: str) -> int:
    """Writes a single Firestore document per row, indexed by card.id,
    each containing exclusively {"card_number": <value>}. Rows
    possessing a blank card.id are skipped, inasmuch as no key exists
    under which to write them. Employs set() (a complete overwrite of
    each document), such that a repeated execution against
    already-loaded data resets card_number to the transformed CSV's
    value -- consistent with the reload-is-authoritative behavior
    maintained elsewhere within this pipeline."""
    if "card.id" not in df.columns:
        raise SystemExit("The transformed data possesses no card.id column by which to key Firestore documents.")

    coll_ref = db.collection(collection)
    batch = db.batch()
    count = 0
    skipped = 0
    for row in df.to_dict(orient="records"):
        card_id = row["card.id"]
        if not card_id:
            skipped += 1
            continue
        doc_ref = coll_ref.document(card_id)
        batch.set(doc_ref, {"card_number": row.get("card.number", "")})
        count += 1
        if count % 400 == 0:  # remains under Firestore's 500-writes-per-batch limit
            batch.commit()
            batch = db.batch()
    batch.commit()
    if skipped:
        print(f"  skipped {skipped} row(s) possessing a blank card.id")
    return count


def main() -> None:
    bucket_name = env("GCS_BUCKET", required=True)
    transformed_blob_name = env(
        "TRANSFORMED_BLOB_NAME", default="transformed/Transformed_May_2025.csv"
    )
    collection = os.environ.get("FIRESTORE_COLLECTION") or collection_for_blob_name(
        transformed_blob_name
    )
    database = env("FIRESTORE_DATABASE", default="(default)")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    print(f"Downloading transformed CSV from gs://{bucket_name}/{transformed_blob_name} ...")
    df = download_csv(bucket, transformed_blob_name)
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    db = firestore.Client(database=database)

    print(f'Loading card.number into Firestore collection "{collection}" (database "{database}") ...')
    count = load_dataframe(df, db, collection)
    print(f'Loaded {count} documents into "{collection}". Browse and edit via the Cloud Console.')


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
