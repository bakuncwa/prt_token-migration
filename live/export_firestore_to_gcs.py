"""Firestore (PaaS-worker-entered card.number) -> GCS cleaned/.

Mirrors the "Export Firestore mod. data back" arrow within the ETL
diagram. This module executes via three distinct mechanisms:

1. Automatically, as the on_export_completed Cloud Function
   (Generation 2): invoked when a PaaS worker selects "Export" within
   the Firestore Console (Import/Export tab) with cleaned/ designated
   as the destination -- the authentic "reconciliation complete" signal
   provided by a human operator. Firestore's managed export writes a
   top-level "<name>.overall_export_metadata" object *last*, serving
   as its completion marker; the arrival of that object under cleaned/
   constitutes what this function monitors for, via a plain GCS
   finalize trigger. It does NOT parse the LevelDB-format export files
   Firestore itself writes (these are disregarded entirely). Instead,
   it treats the marker's arrival as a signal to re-export, and queries
   the matching export operation via the Firestore Admin API
   (google.cloud.firestore_admin_v1) to read its
   ExportDocumentsMetadata.collection_ids -- the collection(s) to which
   the Console "Export" dialog was actually scoped -- then re-queries
   Firestore, live, for exclusively those dated MMYYYY_cardholders
   collections, via the identical run_export() employed throughout.
   Should the Console export have encompassed the entire database
   (collection_ids empty) or should the matching operation prove
   unretrievable, this falls back to re-exporting every dated
   collection presently within Firestore, consistent with behavior
   prior to this filter's introduction. In either instance, this
   affects exclusively which collections are re-exported to cleaned/
   -- it never alters what is read from or written to Firestore itself
   (fetch_card_numbers() and the card.number overlay remain unaffected).
2. Also automatically, as the on_firestore_write Cloud Function
   (Generation 2): invoked upon every write to any dated
   MMYYYY_cardholders collection -- a more rapidly responsive
   complement to mechanism 1, for any party monitoring cleaned/ for
   live updates, in the event a worker never selects Export. Firestore
   provides no per-document "review complete" signal; accordingly,
   this re-exports the entire collection upon every write rather than
   attempting to detect completion, which remains economical at this
   collection's scale (tens of rows). The collection identifier is
   read from the event payload (google-events), and mapped to its
   month/year via collection_naming.py to locate the corresponding
   transformed/ schema file -- for example, a write to
   "052025_cardholders" reads "transformed/Transformed_May_2025.csv"
   and writes "cleaned/Reconciled_Transformed_May_2025.csv".
3. Manually, via main() below, for local execution and backfills.

The landing of this file under cleaned/ constitutes what triggers
deploy_digitalocean.py's on_paas_reconciled() Cloud Function, which
synchronizes the reconciled rows back to DigitalOcean.

Construction of the export: the transformed CSV within GCS constitutes
the single source of truth for every column except card.number (see
load_to_firestore.py -- Firestore stores exclusively card.number,
indexed by card.id, and nothing further). This procedure reads that
CSV afresh, then overlays each row's card.number with whatever value
Firestore presently holds for that card.id. No dot/underscore
field-name mapping requires consideration here, nor is a read-only
lock necessary for enforcement -- Firestore is structurally incapable
of containing any field other than card.number, leaving nothing else
for a PaaS worker to view or alter in the first instance.

Required environment variable:
  GCS_BUCKET   for example, <BUCKET_NAME>

Optional environment variables:
  TRANSFORMED_BLOB_NAME   default "transformed/Transformed_May_2025.csv" (source of truth)
  CLEANED_BLOB_NAME       default "cleaned/Reconciled_<transformed filename>"
  FIRESTORE_COLLECTION    default derived as MMYYYY_cardholders (manual/CLI execution
                          exclusively; the Cloud Function always derives it from the event)
  FIRESTORE_DATABASE      default "(default)"

Manual usage:
  GCS_BUCKET=<BUCKET_NAME> python export_firestore_to_gcs.py

Deployment of both Cloud Functions (Generation 2, executed from the
scripts/ directory). --min-instances=0 together with a low
--max-instances ceiling maintain both within the Cloud Run/Cloud
Functions Always Free tier.

on_export_completed -- a plain GCS finalize trigger, structurally
identical to the other GCS-triggered functions within this pipeline.
Its service account additionally requires "roles/datastore.viewer"
(or an alternative role granting datastore.operations.list/get) for
the collection_ids lookup described above, in addition to whatever
permissions it already requires to read Firestore:
  gcloud functions deploy export-on-firestore-export-button \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_export_completed \\
    --trigger-bucket=<BUCKET_NAME> \\
    --set-env-vars=FIRESTORE_DATABASE="(default)" \\
    --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

on_firestore_write -- a Firestore document-write trigger.
--trigger-location must correspond to the Firestore database's
location (europe-west2, per FIRESTORE_ACCESS.md). The path pattern
employs a wildcard for the collection segment such that it fires upon
any MMYYYY_cardholders collection, rather than a single, fixed month
-- writes to other collections are disregarded within the code itself:
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
    """Retrieves environment variable `name`, raising explicitly if
    it is designated as required and absent."""
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def fetch_card_numbers(db: firestore.Client, collection: str) -> dict[str, str]:
    """Returns card.id -> card_number, read directly from Firestore.
    Every other field resides exclusively within the transformed CSV
    -- Firestore never stores it -- leaving nothing further to
    retrieve here."""
    return {doc.id: doc.to_dict().get("card_number", "") for doc in db.collection(collection).stream()}


def merge_card_numbers(transformed_df: pd.DataFrame, card_numbers: dict[str, str]) -> pd.DataFrame:
    """Overlays each row's card.number with Firestore's value for the
    corresponding card.id, preserving every other column precisely as
    produced by transform_dataframe(). A card.id absent from Firestore
    (no data yet loaded, or a blank card.id that load_to_firestore.py
    skipped) retains its original, blank, transformed value."""
    if "card.id" not in transformed_df.columns:
        raise SystemExit("The transformed CSV possesses no card.id column by which to merge Firestore's card.number.")
    merged = transformed_df.copy()
    merged["card.number"] = merged["card.id"].map(card_numbers).fillna(merged["card.number"])
    return merged


def upload_reconciled_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    """Serializes and uploads the reconciled DataFrame as a CSV
    object, returning the resultant gs:// URI."""
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
    """Shared by main() (manual/CLI execution), on_export_completed,
    and on_firestore_write (Cloud Functions): reads the transformed
    CSV (source of truth for everything except card.number), overlays
    Firestore's current card.number per card.id, and uploads the
    result to cleaned/. Returns the gs:// URI written."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    print(f"Reading transformed CSV from gs://{bucket_name}/{transformed_blob_name} ...")
    transformed_df = download_csv(bucket, transformed_blob_name)

    if cleaned_blob_name is None:
        cleaned_blob_name = "cleaned/Reconciled_" + transformed_blob_name.rsplit("/", 1)[-1]

    db = firestore.Client(database=database)
    print(f'Reading card.number values from Firestore collection "{collection}" (database "{database}") ...')
    card_numbers = fetch_card_numbers(db, collection)
    print(f"  {len(card_numbers)} card.number value(s) present in Firestore")

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
        "The landing of this file under cleaned/ automatically triggers "
        "on_paas_reconciled() within deploy_digitalocean.py."
    )


def _is_export_completion_marker(blob_name: str) -> bool:
    """Returns True for the "<name>.overall_export_metadata" object a
    Firestore export writes *last*, under cleaned/ -- Firestore's
    completion signal. Its precise depth beneath cleaned/ is variable:
    directing the Console "Export" dialog at cleaned/ as the
    destination folder causes Firestore to auto-generate a timestamped
    subfolder (for example,
    cleaned/2026-01-13T10:00:00_12345/2026-...overall_export_metadata),
    one level deeper than an API/CLI export supplied an exact
    destination (for example, cleaned/cleaned.overall_export_metadata)
    -- both instances are accommodated here, inasmuch as exclusively
    the suffix is examined. This also naturally excludes the nested,
    per-kind metadata file Firestore writes alongside it (for example,
    cleaned/<ts>/all_namespaces/kind_.../....export_metadata), which
    does not constitute a completion marker: it terminates in
    ".export_metadata", not ".overall_export_metadata", irrespective
    of nesting depth."""
    return blob_name.startswith(CLEANED_PREFIX) and blob_name.endswith(EXPORT_METADATA_SUFFIX)


def _requested_collection_ids(bucket_name: str, project: str, database: str) -> set[str] | None:
    """Retrieves the ExportDocumentsMetadata for the export operation
    that has just deposited its completion marker under cleaned/, via
    the Firestore Admin API's operations listing (not the LevelDB
    export files themselves -- these remain unparsed, as before). Its
    collection_ids field is precisely what the Console "Export"
    dialog was scoped to.

    Returns None if the Console export was scoped to the entire
    database (collection_ids empty) or if the matching operation
    proves unretrievable -- in either instance, callers ought to fall
    back to exporting every dated collection, consistent with
    pre-filter behavior."""
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
        print(f"Unable to list Firestore Admin operations ({e}); exporting all dated collections.")
        return None

    latest: firestore_admin_v1.ExportDocumentsMetadata | None = None
    for operation in operations:
        metadata = firestore_admin_v1.ExportDocumentsMetadata()
        # operation.metadata constitutes a raw google.protobuf.any_pb2.Any;
        # metadata is proto-plus-wrapped, such that Is()/Unpack() require
        # its underlying _pb -- proto-plus exposes no public accessor
        # for this purpose.
        if not operation.metadata.Is(metadata._pb.DESCRIPTOR):
            continue
        operation.metadata.Unpack(metadata._pb)
        if not metadata.output_uri_prefix.startswith(output_prefix):
            continue
        if metadata.end_time is None:
            continue  # remains in progress -- not a candidate for "most recently completed"
        if latest is None or metadata.end_time > latest.end_time:
            latest = metadata

    if latest is None:
        print(
            f"No matching export operation located for {output_prefix}; "
            "exporting all dated collections."
        )
        return None

    if not latest.collection_ids:
        return None  # the Console export was scoped to "Export entire database"

    return set(latest.collection_ids)


def _delete_export_artifacts(bucket: storage.Bucket, marker_blob_name: str) -> None:
    """Deletes the raw Firestore export folder (the marker together
    with its sibling all_namespaces/kind_.../ data) once
    on_export_completed has completed its reading thereof. Nothing
    within this pipeline ever parses these LevelDB-format files --
    exclusively the marker's arrival is of consequence -- such that
    there exists no reason to permit them to accumulate within
    cleaned/ following every Console export invocation. Skips
    deletion should the marker reside directly within cleaned/ absent
    a dedicated subfolder of its own, inasmuch as that would entail
    deleting cleaned/ itself -- the location of the Reconciled_*.csv
    outputs this function has just written."""
    folder_prefix = marker_blob_name.rsplit("/", 1)[0] + "/"
    if folder_prefix == CLEANED_PREFIX:
        print(f"Marker {marker_blob_name!r} possesses no dedicated export subfolder; skipping cleanup.")
        return
    blobs = list(bucket.list_blobs(prefix=folder_prefix))
    bucket.delete_blobs(blobs)
    print(f"Deleted {len(blobs)} raw export artifact(s) under gs://{bucket.name}/{folder_prefix}")


@functions_framework.cloud_event
def on_export_completed(event: CloudEvent) -> None:
    """Cloud Function (Generation 2): invoked when a PaaS worker
    selects "Export" within the Firestore Console with cleaned/
    designated as the destination -- the authentic "reconciliation
    complete" signal. Re-exports the dated MMYYYY_cardholders
    collection(s) to which the Console export was scoped (via the
    Admin API operation's collection_ids -- see
    _requested_collection_ids), or every dated collection presently
    within Firestore should the export have encompassed the entire
    database. The Firestore-managed export files themselves
    (LevelDB format) remain unparsed; exclusively the completion
    marker's arrival serves as the trigger -- and once these have
    served that purpose, this deletes them (see
    _delete_export_artifacts) such that cleaned/ does not accumulate
    an additional extraneous subfolder upon every Console export
    invocation."""
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
        print("No matching MMYYYY_cardholders collections located -- nothing to export.")
    else:
        for collection in collections:
            month_year_stem = month_year_stem_for_collection(collection)
            transformed_blob_name = f"transformed/Transformed_{month_year_stem}.csv"
            run_export(bucket_name, transformed_blob_name, None, collection, database)

    storage_client = storage.Client()
    _delete_export_artifacts(storage_client.bucket(bucket_name), blob_name)


def _collection_from_event(event: CloudEvent) -> str | None:
    """Extracts the Firestore collection identifier (for example,
    "052025_cardholders") from a
    google.cloud.firestore.document.v1.written CloudEvent. The payload
    is protobuf, not JSON -- deserialization is performed via
    google-events rather than treating event.data as a dictionary."""
    payload = DocumentEventData.deserialize(event.data)
    doc = payload.value if payload.value.name else payload.old_value
    if not doc.name:
        return None
    # doc.name takes the form "projects/P/databases/(default)/documents/{collection}/{docId}"
    return doc.name.split("/documents/", 1)[-1].split("/", 1)[0]


@functions_framework.cloud_event
def on_firestore_write(event: CloudEvent) -> None:
    """Cloud Function (Generation 2): invoked upon every write to any
    dated MMYYYY_cardholders collection, re-exporting that month's
    reconciled dataset to cleaned/ -- no script invocation is
    required."""
    collection = _collection_from_event(event)
    if not collection or not is_dated_cardholders_collection(collection):
        print(f"Ignoring write to collection {collection!r} (not a MMYYYY_cardholders collection)")
        return

    bucket_name = env("GCS_BUCKET", required=True)
    month_year_stem = month_year_stem_for_collection(collection)
    transformed_blob_name = f"transformed/Transformed_{month_year_stem}.csv"
    cleaned_blob_name = os.environ.get("CLEANED_BLOB_NAME")
    database = env("FIRESTORE_DATABASE", default="(default)")

    print(f"Firestore write detected on collection {collection!r}; re-exporting ...")
    run_export(bucket_name, transformed_blob_name, cleaned_blob_name, collection, database)


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
