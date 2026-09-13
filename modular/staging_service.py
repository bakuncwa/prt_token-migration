"""Modular Token Migration ETL: the one staging component, reused in both
directions, for any onboarded merchant.

Generalizes two original-pipeline scripts into a single, config-driven
pair of functions:
  - original/column_mapping.py's transform_dataframe() -> transform_dataframe()
    below, driven by configs/<merchant>.json instead of hardcoded
    SIMPLE_RENAMES/EXPANSIONS dicts.
  - original/export_firestore_to_gcs.py's merge_card_numbers() ->
    reconcile_dataframe() below, generalized from "always card.number
    keyed by card.id" to "whatever fields + id column
    configs/<merchant>.json declares under reconciled_by".

Deployed as one Cloud Run service backing two Eventarc-triggered entry
points (on_raw_uploaded, on_reconciliation_export) plus a manual main()
-- the same three-ways-to-run shape as the original pipeline's
export_firestore_to_gcs.py, just no longer forked into a separate script
per direction.

Bucket layout is merchant-namespaced, one level deeper than the original
pipeline's flat raw/, transformed/, cleaned/:
  raw/<merchant>/<Month Year>.csv
  transformed/<merchant>/Transformed_<Month>_<Year>.csv
  cleaned/<merchant>/Reconciled_Transformed_<Month>_<Year>.csv

Required environment variables:
  GCS_BUCKET   the staging bucket for this deployment (test and
               production deployments point at different buckets --
               see open_decisions.py, "separate GCS bucket" item)

See extraction_adapters.py, store_adapters.py, and sink_adapters.py for
the pluggable pieces either side of this component; gemini_mapping_agent.py
for how configs/<merchant>.json gets drafted in the first place.
"""

from __future__ import annotations

import io
import sys

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
# Forward: raw -> transformed, driven by configs/<merchant>.json
# ---------------------------------------------------------------------------


def _split_date(value: str) -> tuple[str, str]:
    """Same "YYYY-MM" -> (year, zero-padded month) rule as the original
    pipeline's _split_expiry(), generalized to any expansion the config
    declares type "split_date" for."""
    if isinstance(value, str) and len(value) == 7 and value[4] == "-":
        year, month = value.split("-")
        if year.isdigit() and month.isdigit():
            return year, month.zfill(2)
    return "", ""


def _zero_pad_if_length(value: str, length: int, target_length: int) -> str:
    """Same leading-zero repair as the original pipeline's
    _restore_account_number_last4_leading_zero(), generalized to any
    field/length the config declares under "repairs"."""
    if isinstance(value, str) and len(value) == length and value.isdigit():
        return value.zfill(target_length)
    return value


def transform_dataframe(raw_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Apply configs/<merchant>.json's renames/expansions/repairs, in
    column order -- the declarative equivalent of
    original/column_mapping.py's transform_dataframe(), for whichever
    merchant's config is passed in."""
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
                split = series.map(_split_date)
                out[year_col] = split.map(lambda t: t[0])
                out[month_col] = split.map(lambda t: t[1])
            else:
                raise SystemExit(f"Unknown expansion type {rule['type']!r} for column {raw_col!r}")
        elif raw_col in repairs:
            rule = repairs[raw_col]
            if rule["type"] == "zero_pad_if_length":
                out[raw_col] = series.map(
                    lambda v: _zero_pad_if_length(v, rule["length"], rule["target_length"])
                )
            else:
                raise SystemExit(f"Unknown repair type {rule['type']!r} for column {raw_col!r}")
        elif raw_col in renames:
            out[renames[raw_col]] = series
        else:
            out[raw_col] = series
    return pd.DataFrame(out)


# ---------------------------------------------------------------------------
# Reverse: transformed + reconciled store values -> cleaned
# ---------------------------------------------------------------------------


def reconcile_dataframe(
    transformed_df: pd.DataFrame, reconciled_values: dict[str, dict[str, str]], config: dict
) -> pd.DataFrame:
    """Overlay each row's reconciled field(s) with the store's current
    value for that id, keeping every other column exactly as
    transform_dataframe() produced it -- the declarative equivalent of
    original/export_firestore_to_gcs.py's merge_card_numbers(), for
    whichever id_column/fields configs/<merchant>.json declares under
    reconciled_by (CardCorp: id_column "card.id", one field
    "card.number"; a future merchant could reconcile more than one
    field the same way). An id with no matching store record keeps its
    original (blank) transformed value, same fallback as the original."""
    reconciled_by = config["reconciled_by"]
    id_column = reconciled_by["id_column"]
    fields = reconciled_by["fields"]
    if id_column not in transformed_df.columns:
        raise SystemExit(f"transformed CSV has no {id_column!r} column to reconcile by.")

    merged = transformed_df.copy()
    for field in fields:
        field_values = {
            id_value: record.get(field, "") for id_value, record in reconciled_values.items()
        }
        merged[field] = merged[id_column].map(field_values).fillna(merged[field])
    return merged


# ---------------------------------------------------------------------------
# GCS I/O (unchanged shape from the original pipeline's download_csv/upload_csv)
# ---------------------------------------------------------------------------


def download_csv(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise SystemExit(f"Object not found: gs://{bucket.name}/{blob_name}")
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def upload_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def run_transform(bucket_name: str, merchant: str, raw_blob_name: str) -> str:
    """raw/<merchant>/<Month Year>.csv -> transformed/<merchant>/Transformed_<Month>_<Year>.csv"""
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
    """transformed/<merchant>/*.csv + reconciled store values ->
    cleaned/<merchant>/Reconciled_*.csv. reconciled_values is
    id -> {field: value}, already read by a store adapter (see
    store_adapters.py) -- this function has no store-specific code, same
    separation the original pipeline kept between run_export() and
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
    """Manual/CLI run, mirroring the original pipeline's main() functions.

    Usage:
      GCS_BUCKET=... MERCHANT=cardcorp RAW_BLOB_NAME="raw/cardcorp/May 2025.csv" \\
        python staging_service.py transform

      GCS_BUCKET=... MERCHANT=cardcorp TRANSFORMED_BLOB_NAME=transformed/cardcorp/Transformed_May_2025.csv \\
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
        print(f"  {len(reconciled_values)} reconciled record(s) in the store")
        uri = run_reconcile(bucket_name, merchant, transformed_blob_name, reconciled_values)
        print(f"Reconciled dataset written to {uri}")


@functions_framework.cloud_event
def on_raw_uploaded(event: CloudEvent) -> None:
    """Cloud Run function: GCS finalize trigger. Mirrors the original
    pipeline's cf_transform_on_upload.py, generalized to read the
    merchant from the blob path (raw/<merchant>/<file>.csv) instead of
    assuming a single fixed merchant."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not blob_name.startswith(RAW_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {RAW_PREFIX} CSV)")
        return
    parts = blob_name[len(RAW_PREFIX) :].split("/", 1)
    if len(parts) != 2:
        print(f"Ignoring gs://{bucket_name}/{blob_name} (no merchant segment)")
        return
    merchant, raw_filename = parts

    print(f"New raw file for merchant {merchant!r}: gs://{bucket_name}/{blob_name}")
    uri = run_transform(bucket_name, merchant, blob_name)
    print(f"Transformed dataset written to {uri}")


@functions_framework.cloud_event
def on_firestore_write(event: CloudEvent) -> None:
    """Cloud Run function: Firestore document-write trigger. Mirrors the
    original pipeline's export_firestore_to_gcs.py:on_firestore_write --
    re-reconciles that collection's dataset to cleaned/ the instant a
    reviewer's edit lands, generalized to any merchant/collection whose
    name matches <merchant>_MMYYYY_<prefix> (see merchant_config.py)."""
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

    print(f"Firestore write on {collection!r}, re-reconciling merchant {merchant!r} ...")
    reconciled_values = read_reconciled_values(merchant, collection, config)
    uri = run_reconcile(bucket_name, merchant, transformed_blob_name, reconciled_values)
    print(f"Reconciled dataset written to {uri}")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
