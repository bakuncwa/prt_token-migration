"""Token Migration ETL: transformed CSV (GCS) -> Firestore (viewable/editable
in the GCP Console, no VM required).

Loads ONLY card.number into Firestore, keyed by card.id -- every other
column (name, address, transaction metadata, ...) stays in the
transformed CSV and is never written here. This is a deliberate design
choice, not an oversight: card.number is the one field the automated
transform can never populate (see column_mapping.py -- the raw PAN is
blank in the source), so a PaaS worker needs somewhere to enter it by
hand. By only ever storing that one field, there's nothing else in
Firestore for a worker to see or edit in the first place -- no
read-only lock or Security Rule is needed to protect fields that were
never written there.

Firestore Native mode has a genuinely Always Free daily quota (1 GiB
storage, 50K reads / 20K writes / 20K deletes per day) and a built-in
Data viewer in the Cloud Console for browsing/editing records -- no VM,
IAP tunnel, or password file needed, just your normal GCP login.

Each month's dataset gets its own collection, MMYYYY_cardholders,
derived from the transformed filename (see collection_naming.py) --
"transformed/Transformed_May_2025.csv" loads into "052025_cardholders" --
so re-loading an old month never collides with the current one. Override
with FIRESTORE_COLLECTION if a fixed name is needed instead.

The full reconciled record (all transformed columns + whatever
card.number ends up in Firestore) is reassembled at export time in
export_firestore_to_gcs.py, by reading the transformed CSV fresh and
merging in the card.number Firestore has for each card.id.

Required environment variables:
  GCS_BUCKET     e.g. <BUCKET_NAME>

Optional:
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
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def download_csv(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise SystemExit(f"Object not found: gs://{bucket.name}/{blob_name}")
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def load_dataframe(df: pd.DataFrame, db: firestore.Client, collection: str) -> int:
    """Writes one Firestore document per row, keyed by card.id, each
    containing only {"card_number": <value>}. Rows with a blank
    card.id are skipped -- there's no key to write them under. Uses
    set() (full overwrite of each doc), so re-running this against
    already-loaded data resets card_number back to the transformed
    CSV's value, same as the rest of the reload-is-authoritative
    behavior elsewhere in this pipeline."""
    if "card.id" not in df.columns:
        raise SystemExit("Transformed data has no card.id column to key Firestore documents by.")

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
        if count % 400 == 0:  # stay under Firestore's 500-writes-per-batch limit
            batch.commit()
            batch = db.batch()
    batch.commit()
    if skipped:
        print(f"  skipped {skipped} row(s) with a blank card.id")
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
    print(f'Loaded {count} documents into "{collection}". Browse/edit via the Cloud Console.')


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
