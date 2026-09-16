"""Production Token Migration ETL: the single staging component, reused
bidirectionally, applicable to any onboarded merchant.

This module generalizes two live-pipeline scripts into a single,
configuration-driven pair of functions:
  - live/column_mapping.py's transform_dataframe() is generalized as
    transform_dataframe() below, governed by configs/<merchant>.json
    rather than hardcoded SIMPLE_RENAMES/EXPANSIONS dictionaries.
  - live/export_firestore_to_gcs.py's merge_card_numbers() is
    generalized as reconcile_dataframe() below, extended from "always
    card.number, keyed by card.id" to "whichever fields and id column
    configs/<merchant>.json declares under reconciled_by."

Deployed as a single Cloud Run service backing two Eventarc-triggered
entry points (on_raw_uploaded, on_firestore_write) in addition to a
manual main() invocation -- the identical three-ways-to-execute
structure employed by the live pipeline's export_firestore_to_gcs.py,
consolidated rather than forked into a separate script per direction.

Bucket layout is merchant-namespaced, one directory level deeper than
the live pipeline's flat raw/, transformed/, cleaned/ structure:
  raw/<merchant>/<Month Year>.csv
  transformed/<merchant>/Transformed_<Month>_<Year>.csv
  cleaned/<merchant>/Reconciled_Transformed_<Month>_<Year>.csv

Required environment variable:
  GCS_BUCKET   the staging bucket for a given deployment (test and
               production deployments are provisioned against
               distinct buckets -- see open_decisions.py, the
               "separate GCS bucket" item)

See extraction_adapters.py, store_adapters.py, and sink_adapters.py
for the pluggable components on either side of this module;
gemini_mapping_agent.py for the mechanism by which
configs/<merchant>.json is initially drafted.
"""

from __future__ import annotations

import io
import sys
from datetime import datetime

import functions_framework
import pandas as pd
from cloudevents.http import CloudEvent
from google.cloud import storage
from google.events.cloud.firestore_v1 import DocumentEventData

from merchant_config import (
    collection_for_blob_name,
    env,
    load_merchant_config,
    merchant_and_prefix_for_collection,
    month_year_stem_for_collection,
)

RAW_PREFIX = "raw/"
TRANSFORMED_PREFIX = "transformed/"
CLEANED_PREFIX = "cleaned/"


# ---------------------------------------------------------------------------
# Forward direction: raw -> transformed, governed by configs/<merchant>.json
# ---------------------------------------------------------------------------


_DATE_FORMAT_MAP = {
    "YYYY-MM": "%Y-%m",
    "MM/YYYY": "%m/%Y",
    "MM-YYYY": "%m-%Y",
}


def _split_date(value: str, source_format: str) -> tuple[str, str]:
    """Returns (year, zero-padded month), parsed according to
    configs/<merchant>.json's declared source_format. Generalizes the
    live pipeline's _split_expiry(), which addressed only the reference
    merchant's "YYYY-MM" date shape. A merchant whose raw export employs
    a differing date shape (for example, "MM/YYYY") is precisely the
    case this function was, prior to remediation, ill-equipped to
    handle: the configuration declared source_format, yet this function
    disregarded it, returning blank values for any format other than
    "YYYY-MM". This defect was identified by
    test_staging_service.py's differing-schema verification procedure."""
    strptime_format = _DATE_FORMAT_MAP.get(source_format)
    if strptime_format is None:
        raise SystemExit(
            f"Unsupported split_date source_format {source_format!r}; "
            f"add an entry to _DATE_FORMAT_MAP. Recognized formats: {sorted(_DATE_FORMAT_MAP)}"
        )
    if not isinstance(value, str):
        return "", ""
    try:
        parsed = datetime.strptime(value, strptime_format)
    except ValueError:
        return "", ""
    return f"{parsed.year:04d}", f"{parsed.month:02d}"


def _zero_pad_if_length(value: str, length: int, target_length: int) -> str:
    """Applies the identical leading-zero repair as the live pipeline's
    _restore_account_number_last4_leading_zero(), generalized to any
    field and length pair a merchant's configuration declares under
    "repairs"."""
    if isinstance(value, str) and len(value) == length and value.isdigit():
        return value.zfill(target_length)
    return value


def transform_dataframe(raw_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Applies configs/<merchant>.json's renames, expansions, and
    repairs, preserving column order -- the declarative equivalent of
    live/column_mapping.py's transform_dataframe(), parameterized by
    whichever merchant's configuration is supplied."""
    columns = config["columns"]
    renames = columns.get("renames", {})
    expansions = columns.get("expansions", {})
    repairs = columns.get("repairs", {})

    out = {}
    for raw_col in raw_df.columns:
        series = raw_df[raw_col]
        if raw_col in expansions:
            rule = expansions[raw_col]
            if rule["type"] == "copy_then_blank":
                first, *rest = rule["targets"]
                out[first] = series
                for target in rest:
                    out[target] = ""
            elif rule["type"] == "split_date":
                year_col, month_col = rule["targets"]
                source_format = rule["source_format"]
                split = series.map(lambda v: _split_date(v, source_format))
                out[year_col] = split.map(lambda t: t[0])
                out[month_col] = split.map(lambda t: t[1])
            else:
                raise SystemExit(f"Unrecognized expansion type {rule['type']!r} for column {raw_col!r}")
        elif raw_col in repairs:
            rule = repairs[raw_col]
            if rule["type"] == "zero_pad_if_length":
                out[raw_col] = series.map(
                    lambda v: _zero_pad_if_length(v, rule["length"], rule["target_length"])
                )
            else:
                raise SystemExit(f"Unrecognized repair type {rule['type']!r} for column {raw_col!r}")
        elif raw_col in renames:
            out[renames[raw_col]] = series
        else:
            out[raw_col] = series
    return pd.DataFrame(out)


# ---------------------------------------------------------------------------
# Reverse direction: transformed data + reconciled store values -> cleaned
# ---------------------------------------------------------------------------


def reconcile_dataframe(
    transformed_df: pd.DataFrame, reconciled_values: dict[str, dict[str, str]], config: dict
) -> pd.DataFrame:
    """Overlays each row's reconciled field(s) with the store's current
    value for the corresponding identifier, preserving every other
    column precisely as produced by transform_dataframe() -- the
    declarative equivalent of live/export_firestore_to_gcs.py's
    merge_card_numbers(), generalized to whichever id_column and
    fields a merchant's configuration declares under reconciled_by (the
    reference merchant declares id_column "card.id" and a single field,
    "card.number"; a subsequently onboarded merchant may reconcile
    multiple fields by the same mechanism). An identifier absent from
    the store retains its original, blank, transformed value -- the
    identical fallback behavior employed by the live pipeline."""
    reconciled_by = config["reconciled_by"]
    id_column = reconciled_by["id_column"]
    fields = reconciled_by["fields"]
    if id_column not in transformed_df.columns:
        raise SystemExit(f"The transformed CSV possesses no {id_column!r} column by which to reconcile.")

    merged = transformed_df.copy()
    for field in fields:
        field_values = {
            id_value: record.get(field, "") for id_value, record in reconciled_values.items()
        }
        merged[field] = merged[id_column].map(field_values).fillna(merged[field])
    return merged


# ---------------------------------------------------------------------------
# GCS I/O (structurally identical to the live pipeline's download_csv/upload_csv)
# ---------------------------------------------------------------------------


def download_csv(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    """Downloads and parses a CSV object from the specified bucket,
    raising explicitly if the object does not exist."""
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise SystemExit(f"Object not found: gs://{bucket.name}/{blob_name}")
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def upload_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    """Serializes and uploads a DataFrame as a CSV object, returning
    the resultant gs:// URI."""
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def run_transform(bucket_name: str, merchant: str, raw_blob_name: str) -> str:
    """Executes the forward transformation:
    raw/<merchant>/<Month Year>.csv ->
    transformed/<merchant>/Transformed_<Month>_<Year>.csv."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    config = load_merchant_config(merchant)

    raw_df = download_csv(bucket, raw_blob_name)
    transformed_df = transform_dataframe(raw_df, config)

    filename = raw_blob_name.rsplit("/", 1)[-1]
    stem, _, ext = filename.rpartition(".")
    transformed_blob_name = f"{TRANSFORMED_PREFIX}{merchant}/Transformed_{stem.replace(' ', '_')}.{ext or 'csv'}"
    return upload_csv(bucket, transformed_blob_name, transformed_df)


def run_reconcile(
    bucket_name: str, merchant: str, transformed_blob_name: str, reconciled_values: dict
) -> str:
    """Executes the reconciliation procedure:
    transformed/<merchant>/*.csv combined with reconciled store values
    yields cleaned/<merchant>/Reconciled_*.csv. reconciled_values takes
    the form identifier -> {field: value}, already retrieved by a store
    adapter (see store_adapters.py); this function contains no
    store-specific logic, preserving the identical separation of
    concerns the live pipeline maintained between run_export() and
    fetch_card_numbers()."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    config = load_merchant_config(merchant)

    transformed_df = download_csv(bucket, transformed_blob_name)
    merged_df = reconcile_dataframe(transformed_df, reconciled_values, config)

    cleaned_blob_name = (
        f"{CLEANED_PREFIX}{merchant}/Reconciled_" + transformed_blob_name.rsplit("/", 1)[-1]
    )
    return upload_csv(bucket, cleaned_blob_name, merged_df)


def main() -> None:
    """Manual/CLI invocation, mirroring the live pipeline's main()
    functions.

    Usage:
      GCS_BUCKET=... MERCHANT=<MERCHANT_ID> RAW_BLOB_NAME="raw/<MERCHANT_ID>/May 2025.csv" \\
        python staging_service.py transform

      GCS_BUCKET=... MERCHANT=<MERCHANT_ID> TRANSFORMED_BLOB_NAME=transformed/<MERCHANT_ID>/Transformed_May_2025.csv \\
        python staging_service.py reconcile
    """
    if len(sys.argv) != 2 or sys.argv[1] not in ("transform", "reconcile"):
        raise SystemExit("Usage: python staging_service.py {transform|reconcile}")

    bucket_name = env("GCS_BUCKET", required=True)
    merchant = env("MERCHANT", required=True)

    if sys.argv[1] == "transform":
        raw_blob_name = env("RAW_BLOB_NAME", required=True)
        uri = run_transform(bucket_name, merchant, raw_blob_name)
        print(f"Transformed dataset written to {uri}")
    else:
        from store_adapters import read_reconciled_values

        transformed_blob_name = env("TRANSFORMED_BLOB_NAME", required=True)
        config = load_merchant_config(merchant)
        collection = collection_for_blob_name(merchant, transformed_blob_name, config)
        reconciled_values = read_reconciled_values(merchant, collection, config)
        print(f"  {len(reconciled_values)} reconciled record(s) retrieved from the store")
        uri = run_reconcile(bucket_name, merchant, transformed_blob_name, reconciled_values)
        print(f"Reconciled dataset written to {uri}")


@functions_framework.cloud_event
def on_raw_uploaded(event: CloudEvent) -> None:
    """Cloud Run function: Google Cloud Storage (GCS) finalize trigger.
    Mirrors the live pipeline's cf_transform_on_upload.py, generalized
    to derive the merchant identifier from the blob path
    (raw/<merchant>/<file>.csv) rather than presuming a single, fixed
    merchant."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not blob_name.startswith(RAW_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {RAW_PREFIX} CSV)")
        return
    parts = blob_name[len(RAW_PREFIX) :].split("/", 1)
    if len(parts) != 2:
        print(f"Ignoring gs://{bucket_name}/{blob_name} (no merchant segment present)")
        return
    merchant, raw_filename = parts

    print(f"New raw file detected for merchant {merchant!r}: gs://{bucket_name}/{blob_name}")
    uri = run_transform(bucket_name, merchant, blob_name)
    print(f"Transformed dataset written to {uri}")


@functions_framework.cloud_event
def on_firestore_write(event: CloudEvent) -> None:
    """Cloud Run function: Firestore document-write trigger. Mirrors
    the live pipeline's export_firestore_to_gcs.py:on_firestore_write
    -- re-reconciles the affected collection's dataset to cleaned/
    immediately upon a reviewer's edit, generalized to any
    merchant/collection whose identifier conforms to
    <merchant>_MMYYYY_<prefix> (see merchant_config.py)."""
    from store_adapters import read_reconciled_values

    payload = DocumentEventData.deserialize(event.data)
    doc = payload.value if payload.value.name else payload.old_value
    if not doc.name:
        return
    collection = doc.name.split("/documents/", 1)[-1].split("/", 1)[0]

    parsed = merchant_and_prefix_for_collection(collection)
    if parsed is None:
        print(f"Ignoring write to collection {collection!r} (not a <merchant>_MMYYYY_<prefix> collection)")
        return
    merchant, _prefix = parsed

    bucket_name = env("GCS_BUCKET", required=True)
    config = load_merchant_config(merchant)
    month_year_stem = month_year_stem_for_collection(collection)
    transformed_blob_name = f"{TRANSFORMED_PREFIX}{merchant}/Transformed_{month_year_stem}.csv"

    print(f"Firestore write detected on {collection!r}; re-reconciling merchant {merchant!r} ...")
    reconciled_values = read_reconciled_values(merchant, collection, config)
    uri = run_reconcile(bucket_name, merchant, transformed_blob_name, reconciled_values)
    print(f"Reconciled dataset written to {uri}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
