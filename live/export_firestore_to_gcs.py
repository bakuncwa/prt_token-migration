"""Firestore (PaaS-worker-entered card.number) -> GCS cleaned/.

Mirrors the "Export Firestore mod. data back" arrow on the ETL diagram.
Three ways this runs:

1. Automatically, as the on_export_completed Cloud Function (2nd gen):
   fires when a PaaS worker clicks "Export" in the Firestore Console
   (Import/Export tab) with cleaned/ as the destination -- this is the
   real "I'm done reconciling" signal a human gives. Firestore's managed
   export writes a top-level "<name>.overall_export_metadata" object
   *last*, as its completion marker; that object landing under cleaned/
   is what this function watches for via a plain GCS finalize trigger.
   It does NOT parse the LevelDB-format export files Firestore itself
   writes (those are ignored entirely). Instead it treats the marker's
   arrival as "re-export now" and looks up the matching export operation
   via the Firestore Admin API (google.cloud.firestore_admin_v1) to read
   its ExportDocumentsMetadata.collection_ids -- the collection(s) the
   Console "Export" dialog was actually scoped to -- then re-queries
   Firestore live for just those dated MMYYYY_cardholders collections,
   via the same run_export() used everywhere else. If the Console export
   covered the whole database (collection_ids empty) or the matching
   operation can't be found/read, it falls back to re-exporting every
   dated collection currently in Firestore, same as before this filter
   existed. Either way this only changes which collections get
   re-exported to cleaned/ -- it never touches what's read from or
   written to Firestore itself (fetch_card_numbers() and the card.number
   overlay are unaffected).
2. Also automatically, as the on_firestore_write Cloud Function (2nd
   gen): fires on every write to any dated MMYYYY_cardholders collection
   -- a faster-reacting complement to #1 for anyone watching cleaned/
   update live, in case a worker never clicks Export. Firestore doesn't
   carry a per-document "done reviewing" signal, so this re-exports the
   whole collection on every write rather than trying to detect
   completion; at this collection's scale (tens of rows) that's cheap.
   The collection name is read from the event payload (google-events),
   and mapped back to its month/year via collection_naming.py to find
   the matching transformed/ schema file -- e.g. a write to
   "052025_cardholders" reads "transformed/Transformed_May_2025.csv" and
   writes "cleaned/Reconciled_Transformed_May_2025.csv".
3. Manually, via main() below, for local runs / backfills.

Landing the file under cleaned/ is what triggers deploy_digitalocean.py's
on_paas_reconciled() Cloud Function, which syncs the reconciled rows
back to DigitalOcean.

How the export is built: the transformed CSV in GCS is the single
source of truth for every column except card.number (see
load_to_firestore.py -- Firestore only ever stores card.number, keyed
by card.id, nothing else). This reads that CSV fresh, then overlays
each row's card.number with whatever value Firestore currently has for
that card.id. There's no dot/underscore field-name mapping to worry
about here and no read-only lock to enforce -- Firestore structurally
cannot contain any field other than card.number, so there's nothing
else for a PaaS worker to see or tamper with in the first place.

Required environment variables:
  GCS_BUCKET   e.g. <BUCKET_NAME>

Optional:
  TRANSFORMED_BLOB_NAME   default "transformed/Transformed_May_2025.csv" (source of truth)
  CLEANED_BLOB_NAME       default "cleaned/Reconciled_<transformed filename>"
  FIRESTORE_COLLECTION    default derived as MMYYYY_cardholders (manual/CLI run only;
                          the Cloud Function always derives it from the event)
  FIRESTORE_DATABASE      default "(default)"

Manual usage:
  GCS_BUCKET=<BUCKET_NAME> python export_firestore_to_gcs.py

Deploy both Cloud Functions (2nd gen, from the scripts/ directory).
--min-instances=0 and a low --max-instances cap keep both inside the
Cloud Run/Cloud Functions Always Free tier.

on_export_completed -- plain GCS finalize trigger, same shape as the
other GCS-triggered functions in this pipeline. Its service account also
needs "roles/datastore.viewer" (or another role granting
datastore.operations.list/get) for the collection_ids lookup described
above, in addition to whatever it already needs to read Firestore:
  gcloud functions deploy export-on-firestore-export-button \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_export_completed \\
    --trigger-bucket=<BUCKET_NAME> \\
    --set-env-vars=FIRESTORE_DATABASE="(default)" \\
    --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

on_firestore_write -- Firestore document-write trigger. --trigger-
location must match the Firestore database's location (europe-west2,
per FIRESTORE_ACCESS.md). The path pattern wildcards the collection
segment so it fires on any MMYYYY_cardholders collection, not just one
fixed month -- writes to other collections are ignored in code:
  gcloud functions deploy export-on-firestore-write \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_firestore_write \\
    --trigger-event-filters="type=google.cloud.firestore.document.v1.written" \\
    --trigger-event-filters="database=(default)" \\
    --trigger-event-filters-path-pattern="document={collection}/{docId}" \\
    --trigger-location=europe-west2 \\
    --set-env-vars=GCS_BUCKET=<BUCKET_NAME>,FIRESTORE_DATABASE="(default)" \\
    --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3
"""

from __future__ import annotations

import os
import sys

import functions_framework
import pandas as pd
from cloudevents.http import CloudEvent
from google.cloud import firestore, firestore_admin_v1, storage
from google.events.cloud.firestore_v1 import DocumentEventData

from collection_naming import (
    collection_for_blob_name,
    is_dated_cardholders_collection,
    month_year_stem_for_collection,
)
from load_to_firestore import download_csv

DEFAULT_TRANSFORMED_BLOB_NAME = "transformed/Transformed_May_2025.csv"
CLEANED_PREFIX = "cleaned/"
EXPORT_METADATA_SUFFIX = ".overall_export_metadata"


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def fetch_card_numbers(db: firestore.Client, collection: str) -> dict[str, str]:
    """card.id -> card_number, read straight from Firestore. Every other
    field lives only in the transformed CSV -- Firestore never stores
    it, so there's nothing else to fetch here."""
    return {doc.id: doc.to_dict().get("card_number", "") for doc in db.collection(collection).stream()}


def merge_card_numbers(transformed_df: pd.DataFrame, card_numbers: dict[str, str]) -> pd.DataFrame:
    """Overlay each row's card.number with Firestore's value for that
    card.id, keeping every other column exactly as transform_dataframe
    produced it. A card.id with no matching Firestore document (nothing
    loaded yet, or a blank card.id that load_to_firestore.py skipped)
    keeps its original (blank) transformed value."""
    if "card.id" not in transformed_df.columns:
        raise SystemExit("transformed CSV has no card.id column to merge Firestore card.number by.")
    merged = transformed_df.copy()
    merged["card.number"] = merged["card.id"].map(card_numbers).fillna(merged["card.number"])
    return merged


def upload_reconciled_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"


def run_export(
    bucket_name: str,
    transformed_blob_name: str,
    cleaned_blob_name: str | None,
    collection: str,
    database: str,
) -> str:
    """Shared by main() (manual/CLI), on_export_completed, and
    on_firestore_write (Cloud Functions): read the transformed CSV
    (source of truth for everything but card.number), overlay Firestore's
    current card.number per card.id, and upload the result to cleaned/.
    Returns the gs:// URI written."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    print(f"Reading transformed CSV from gs://{bucket_name}/{transformed_blob_name} ...")
    transformed_df = download_csv(bucket, transformed_blob_name)

    if cleaned_blob_name is None:
        cleaned_blob_name = "cleaned/Reconciled_" + transformed_blob_name.rsplit("/", 1)[-1]

    db = firestore.Client(database=database)
    print(f'Reading card.number values from Firestore collection "{collection}" (database "{database}") ...')
    card_numbers = fetch_card_numbers(db, collection)
    print(f"  {len(card_numbers)} card.number value(s) in Firestore")

    df = merge_card_numbers(transformed_df, card_numbers)
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    uri = upload_reconciled_csv(bucket, cleaned_blob_name, df)
    print(f"Reconciled dataset written to {uri}")
    return uri


def main() -> None:
    bucket_name = env("GCS_BUCKET", required=True)
    transformed_blob_name = env("TRANSFORMED_BLOB_NAME", default=DEFAULT_TRANSFORMED_BLOB_NAME)
    cleaned_blob_name = os.environ.get("CLEANED_BLOB_NAME")
    collection = os.environ.get("FIRESTORE_COLLECTION") or collection_for_blob_name(
        transformed_blob_name
    )
    database = env("FIRESTORE_DATABASE", default="(default)")

    run_export(bucket_name, transformed_blob_name, cleaned_blob_name, collection, database)
    print(
        "Landing this file under cleaned/ triggers on_paas_reconciled() "
        "in deploy_digitalocean.py automatically."
    )


def _is_export_completion_marker(blob_name: str) -> bool:
    """True for the "<name>.overall_export_metadata" object a Firestore
    export writes *last*, under cleaned/ -- Firestore's completion
    signal. Its exact depth under cleaned/ varies: pointing the Console
    "Export" dialog at cleaned/ as the destination folder makes Firestore
    auto-generate a timestamped subfolder (e.g.
    cleaned/2026-01-13T10:00:00_12345/2026-...overall_export_metadata),
    one level deeper than an API/CLI export given an exact destination
    (e.g. cleaned/cleaned.overall_export_metadata) -- both are handled
    here since only the suffix is checked. This also naturally excludes
    the nested per-kind metadata file Firestore writes alongside it
    (e.g. cleaned/<ts>/all_namespaces/kind_.../....export_metadata),
    which is not a completion marker: it ends in ".export_metadata",
    not ".overall_export_metadata", regardless of nesting depth."""
    return blob_name.startswith(CLEANED_PREFIX) and blob_name.endswith(EXPORT_METADATA_SUFFIX)


def _requested_collection_ids(bucket_name: str, project: str, database: str) -> set[str] | None:
    """Look up the ExportDocumentsMetadata for the export operation that
    just landed its completion marker under cleaned/, via the Firestore
    Admin API's operations list (not the LevelDB export files themselves
    -- those stay unparsed, same as before). Its collection_ids field is
    exactly what the Console "Export" dialog was scoped to.

    Returns None if the Console export was scoped to the whole database
    (collection_ids empty) or if the matching operation couldn't be
    found/read -- either way, callers should fall back to exporting
    every dated collection, same as the pre-filter behavior."""
    admin_client = firestore_admin_v1.FirestoreAdminClient()
    parent = f"projects/{project}/databases/{database}"
    output_prefix = f"gs://{bucket_name}/{CLEANED_PREFIX}"

    try:
        operations = []
        page_token = ""
        while True:
            response = admin_client.list_operations(request={"name": parent, "page_token": page_token})
            operations.extend(response.operations)
            page_token = response.next_page_token
            if not page_token:
                break
    except Exception as e:
        print(f"Could not list Firestore Admin operations ({e}); exporting all dated collections.")
        return None

    latest: firestore_admin_v1.ExportDocumentsMetadata | None = None
    for operation in operations:
        metadata = firestore_admin_v1.ExportDocumentsMetadata()
        # operation.metadata is a raw google.protobuf.any_pb2.Any; metadata
        # is proto-plus-wrapped, so Is()/Unpack() need its underlying _pb --
        # proto-plus doesn't expose a public helper for this.
        if not operation.metadata.Is(metadata._pb.DESCRIPTOR):
            continue
        operation.metadata.Unpack(metadata._pb)
        if not metadata.output_uri_prefix.startswith(output_prefix):
            continue
        if metadata.end_time is None:
            continue  # still in progress -- not a candidate for "most recently completed"
        if latest is None or metadata.end_time > latest.end_time:
            latest = metadata

    if latest is None:
        print(
            f"No matching export operation found for {output_prefix}; "
            "exporting all dated collections."
        )
        return None

    if not latest.collection_ids:
        return None  # Console export was scoped to "Export entire database"

    return set(latest.collection_ids)


def _delete_export_artifacts(bucket: storage.Bucket, marker_blob_name: str) -> None:
    """Delete the raw Firestore export folder (the marker plus its
    sibling all_namespaces/kind_.../ data) once on_export_completed has
    finished reading it. Nothing in this pipeline ever parses those
    LevelDB-format files -- only the marker's arrival matters -- so
    there's no reason to leave them cluttering cleaned/ after every
    Console export click. Skips deletion if the marker sits directly in
    cleaned/ with no dedicated subfolder of its own, since that would
    mean deleting cleaned/ itself -- home to the Reconciled_*.csv
    outputs this function just wrote."""
    folder_prefix = marker_blob_name.rsplit("/", 1)[0] + "/"
    if folder_prefix == CLEANED_PREFIX:
        print(f"Marker {marker_blob_name!r} has no dedicated export subfolder; skipping cleanup.")
        return
    blobs = list(bucket.list_blobs(prefix=folder_prefix))
    bucket.delete_blobs(blobs)
    print(f"Deleted {len(blobs)} raw export artifact(s) under gs://{bucket.name}/{folder_prefix}")


@functions_framework.cloud_event
def on_export_completed(event: CloudEvent) -> None:
    """Cloud Function (2nd gen): fires when a PaaS worker clicks
    "Export" in the Firestore Console with cleaned/ as the destination
    -- the real "I'm done reconciling" signal. Re-exports the dated
    MMYYYY_cardholders collection(s) the Console export was scoped to
    (via the Admin API operation's collection_ids -- see
    _requested_collection_ids), or every dated collection currently in
    Firestore if the export covered the whole database. The
    Firestore-managed export files themselves (LevelDB format) are still
    never parsed, only the completion marker's arrival is used as the
    trigger -- and once they've served that purpose, this deletes them
    (see _delete_export_artifacts) so cleaned/ doesn't accumulate a new
    junk subfolder on every Console export click."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not _is_export_completion_marker(blob_name):
        return

    print(f"Firestore Export completion detected: gs://{bucket_name}/{blob_name}")
    database = env("FIRESTORE_DATABASE", default="(default)")
    db = firestore.Client(database=database)
    requested = _requested_collection_ids(bucket_name, db.project, database)
    if requested is not None:
        print(f"Export was scoped to collection(s): {sorted(requested)}")

    collections = [
        c.id
        for c in db.collections()
        if is_dated_cardholders_collection(c.id) and (requested is None or c.id in requested)
    ]
    if not collections:
        print("No matching MMYYYY_cardholders collections found -- nothing to export.")
    else:
        for collection in collections:
            month_year_stem = month_year_stem_for_collection(collection)
            transformed_blob_name = f"transformed/Transformed_{month_year_stem}.csv"
            run_export(bucket_name, transformed_blob_name, None, collection, database)

    storage_client = storage.Client()
    _delete_export_artifacts(storage_client.bucket(bucket_name), blob_name)


def _collection_from_event(event: CloudEvent) -> str | None:
    """Extract the Firestore collection name (e.g. "052025_cardholders")
    from a google.cloud.firestore.document.v1.written CloudEvent. The
    payload is protobuf, not JSON -- deserialize via google-events rather
    than treating event.data as a dict."""
    payload = DocumentEventData.deserialize(event.data)
    doc = payload.value if payload.value.name else payload.old_value
    if not doc.name:
        return None
    # doc.name is "projects/P/databases/(default)/documents/{collection}/{docId}"
    return doc.name.split("/documents/", 1)[-1].split("/", 1)[0]


@functions_framework.cloud_event
def on_firestore_write(event: CloudEvent) -> None:
    """Cloud Function (2nd gen): fires on every write to any dated
    MMYYYY_cardholders collection, re-exporting that month's reconciled
    dataset to cleaned/ -- no script invocation required."""
    collection = _collection_from_event(event)
    if not collection or not is_dated_cardholders_collection(collection):
        print(f"Ignoring write to collection {collection!r} (not a MMYYYY_cardholders collection)")
        return

    bucket_name = env("GCS_BUCKET", required=True)
    month_year_stem = month_year_stem_for_collection(collection)
    transformed_blob_name = f"transformed/Transformed_{month_year_stem}.csv"
    cleaned_blob_name = os.environ.get("CLEANED_BLOB_NAME")
    database = env("FIRESTORE_DATABASE", default="(default)")

    print(f"Firestore write detected on collection {collection!r}, re-exporting ...")
    run_export(bucket_name, transformed_blob_name, cleaned_blob_name, collection, database)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
