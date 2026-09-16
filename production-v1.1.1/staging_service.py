"""Production Pipeline v1.1.1: de-identification/re-identification staging
component, reused bidirectionally, applicable to any merchant onboarded onto
this directory (none, at present -- see DESIGN.md's "Deployment status").

transform_dataframe() and reconcile_dataframe() below are ported unmodified
from production-v1.1.0/staging_service.py: the column-mapping and
reconciliation-overlay logic configs/<merchant>.json declares is identical in
shape between the two directories, since a merchant's raw schema does not
change based on which pipeline version processes it. What differs is
everything around those two functions:

  - raw/<merchant>/*.csv holds a deterministically encrypted PAN token
    (extraction_adapters.py), not a blanked column.
  - Every transformed and reconciled row is additionally loaded into
    BigQuery (store_adapters.py), so that the dataset is queryable, not
    just staged as CSV.
  - Reconciliation is triggered by a Pub/Sub message once
    vault_reconciliation_adapters.py's token vault has new tokens ready
    (on_vault_reconciliation below), not by a human's Firestore write.
  - A second Pub/Sub message, published once reconciliation commits to
    BigQuery, triggers the write-back sync (on_reconciled_event below),
    which is the sole point at which a PAN is ever re-identified (see
    sink_adapters.py).

Bucket layout is identical to v1.1.0's merchant-namespaced structure:
  raw/<merchant>/<Month Year>.csv
  transformed/<merchant>/Transformed_<Month>_<Year>.csv

No cleaned/ prefix exists in this directory: the reconciled dataset's
authoritative copy is the BigQuery table store_adapters.py writes to, not a
GCS object, since "queryable at enterprise level" is the requirement this
directory serves.

Required environment variables:
  GCS_BUCKET       the staging bucket for a given deployment
  GCP_PROJECT      the project against which DLP/BigQuery calls are made
  PUBSUB_TOPIC     the topic on_vault_reconciliation publishes
                   "reconciliation complete" events to (see SETUP.md Step 7)
"""

from __future__ import annotations

import io
import sys
from datetime import datetime

import functions_framework
import pandas as pd
from cloudevents.http import CloudEvent
from google.cloud import pubsub_v1, storage

from extraction_adapters import upload_raw_csv
from merchant_config import (
    collection_for_blob_name,
    env,
    load_merchant_config,
)

RAW_PREFIX = "raw/"
TRANSFORMED_PREFIX = "transformed/"


# ---------------------------------------------------------------------------
# Forward direction: raw -> transformed, governed by configs/<merchant>.json
# (ported unmodified from production-v1.1.0/staging_service.py)
# ---------------------------------------------------------------------------


_DATE_FORMAT_MAP = {
    "YYYY-MM": "%Y-%m",
    "MM/YYYY": "%m/%Y",
    "MM-YYYY": "%m-%Y",
}


def _split_date(value: str, source_format: str) -> tuple[str, str]:
    """Returns (year, zero-padded month), parsed according to
    configs/<merchant>.json's declared source_format."""
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
    if isinstance(value, str) and len(value) == length and value.isdigit():
        return value.zfill(target_length)
    return value


def transform_dataframe(raw_df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Applies configs/<merchant>.json's renames, expansions, and repairs,
    preserving column order -- identical to
    production-v1.1.0/staging_service.py's function of the same name."""
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


def reconcile_dataframe(
    transformed_df: pd.DataFrame, reconciled_values: dict[str, dict[str, str]], config: dict
) -> pd.DataFrame:
    """Overlays each row's reconciled field(s) with the vault's supplied
    token for the corresponding identifier -- identical contract to
    production-v1.1.0/staging_service.py's function of the same name, except
    reconciled_values here originates from
    vault_reconciliation_adapters.fetch_new_pan_tokens() rather than a
    human-edited Firestore collection."""
    reconciled_by = config["reconciled_by"]
    id_column = reconciled_by["id_column"]
    fields = reconciled_by["fields"]
    if id_column not in transformed_df.columns:
        raise SystemExit(f"The transformed dataset possesses no {id_column!r} column by which to reconcile.")

    merged = transformed_df.copy()
    for field in fields:
        field_values = {
            id_value: record.get(field, "") for id_value, record in reconciled_values.items()
        }
        merged[field] = merged[id_column].map(field_values).fillna(merged[field])
    return merged


# ---------------------------------------------------------------------------
# GCS I/O
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


def _publish(topic: str, project_id: str, attributes: dict[str, str]) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic)
    future = publisher.publish(topic_path, b"", **attributes)
    future.result()


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def run_transform(bucket_name: str, merchant: str, raw_blob_name: str) -> tuple[str, str]:
    """Executes the forward transformation and lands the result both to GCS
    (transformed/<merchant>/*.csv, for audit/replay) and to BigQuery (the
    queryable copy downstream consumers actually read). Returns
    (gs:// uri, bigquery collection identifier)."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    config = load_merchant_config(merchant)

    raw_df = download_csv(bucket, raw_blob_name)
    transformed_df = transform_dataframe(raw_df, config)

    filename = raw_blob_name.rsplit("/", 1)[-1]
    stem, _, ext = filename.rpartition(".")
    transformed_blob_name = f"{TRANSFORMED_PREFIX}{merchant}/Transformed_{stem.replace(' ', '_')}.{ext or 'csv'}"
    uri = upload_csv(bucket, transformed_blob_name, transformed_df)

    from store_adapters import write_dataframe

    collection = collection_for_blob_name(merchant, transformed_blob_name, config)
    write_dataframe(transformed_df, collection, config)
    return uri, collection


def run_reconcile(merchant: str, collection: str, reconciled_values: dict) -> None:
    """Executes the reconciliation procedure: the BigQuery table's current
    transformed rows, combined with the vault's newly supplied PAN tokens,
    overwrite that same table (see store_adapters.write_dataframe()'s
    truncate-and-replace semantics). No cleaned/ GCS object is produced --
    the BigQuery table is this directory's single reconciled copy."""
    from store_adapters import read_reconciled_values as _read_existing
    from store_adapters import write_dataframe

    config = load_merchant_config(merchant)
    existing = _read_existing(merchant, collection, config)
    id_column = config["reconciled_by"]["id_column"]
    transformed_df = pd.DataFrame(
        [{id_column: id_value, **fields} for id_value, fields in existing.items()]
    )
    merged_df = reconcile_dataframe(transformed_df, reconciled_values, config)
    write_dataframe(merged_df, collection, config)


def main() -> None:
    """Manual/CLI invocation, mirroring production-v1.1.0/staging_service.py's
    main().

    Usage:
      GCS_BUCKET=... GCP_PROJECT=... MERCHANT=<MERCHANT_ID> \\
        RAW_BLOB_NAME="raw/<MERCHANT_ID>/May 2025.csv" \\
        python staging_service.py transform
    """
    if len(sys.argv) != 2 or sys.argv[1] != "transform":
        raise SystemExit(
            "Usage: python staging_service.py transform "
            "(reconciliation is Pub/Sub-triggered only -- see on_vault_reconciliation())"
        )

    bucket_name = env("GCS_BUCKET", required=True)
    merchant = env("MERCHANT", required=True)
    raw_blob_name = env("RAW_BLOB_NAME", required=True)
    uri, collection = run_transform(bucket_name, merchant, raw_blob_name)
    print(f"Transformed dataset written to {uri} and BigQuery collection {collection!r}")


@functions_framework.cloud_event
def on_raw_uploaded(event: CloudEvent) -> None:
    """Cloud Run function: GCS finalize trigger on raw/<merchant>/*.csv.
    Deployed (see SETUP.md Step 7) but not wired to any live bucket -- see
    DESIGN.md's "Deployment status"."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not blob_name.startswith(RAW_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {RAW_PREFIX} CSV)")
        return
    parts = blob_name[len(RAW_PREFIX) :].split("/", 1)
    if len(parts) != 2:
        print(f"Ignoring gs://{bucket_name}/{blob_name} (no merchant segment present)")
        return
    merchant, _raw_filename = parts

    print(f"New raw file detected for merchant {merchant!r}: gs://{bucket_name}/{blob_name}")
    uri, collection = run_transform(bucket_name, merchant, blob_name)
    print(f"Transformed dataset written to {uri} and BigQuery collection {collection!r}")


@functions_framework.cloud_event
def on_vault_reconciliation(event: CloudEvent) -> None:
    """Cloud Run function: Pub/Sub trigger, invoked once
    vault_reconciliation_adapters.py's token vault reports new PAN tokens
    ready for a merchant (message attribute "merchant"). Replaces v1.1.0's
    Firestore-write trigger entirely -- see DESIGN.md's "Reconciliation
    source" verdict. Deployed but not wired to any live topic subscription
    -- see DESIGN.md's "Deployment status"."""
    from vault_reconciliation_adapters import fetch_new_pan_tokens

    attributes = event.data.get("message", {}).get("attributes", {})
    merchant = attributes.get("merchant")
    collection = attributes.get("collection")
    if not merchant or not collection:
        print(f"Ignoring malformed vault-reconciliation event: {attributes!r}")
        return

    config = load_merchant_config(merchant)
    reconciled_values = fetch_new_pan_tokens(merchant, config)
    print(f"  {len(reconciled_values)} reconciled token(s) retrieved from the vault")
    run_reconcile(merchant, collection, reconciled_values)

    project_id = env("GCP_PROJECT", required=True)
    topic = env("PUBSUB_RECONCILED_TOPIC", required=True)
    _publish(topic, project_id, {"merchant": merchant, "collection": collection})
    print(f"Reconciled BigQuery collection {collection!r}; published to {topic!r}")


@functions_framework.cloud_event
def on_reconciled_event(event: CloudEvent) -> None:
    """Cloud Run function: Pub/Sub trigger, invoked once on_vault_reconciliation
    above commits a reconciled table and publishes to PUBSUB_RECONCILED_TOPIC.
    Reads the reconciled rows back from BigQuery and dispatches to the
    merchant's sink_adapter -- the sole path along which a PAN is ever
    re-identified (see sink_adapters.py). Deployed but not wired to any live
    topic subscription -- see DESIGN.md's "Deployment status"."""
    from sink_adapters import sync
    from store_adapters import read_reconciled_values

    attributes = event.data.get("message", {}).get("attributes", {})
    merchant = attributes.get("merchant")
    collection = attributes.get("collection")
    if not merchant or not collection:
        print(f"Ignoring malformed reconciled event: {attributes!r}")
        return

    config = load_merchant_config(merchant)
    reconciled = read_reconciled_values(merchant, collection, config)
    id_column = config["reconciled_by"]["id_column"]
    df = pd.DataFrame([{id_column: id_value, **fields} for id_value, fields in reconciled.items()])

    print(f"Reconciled event for merchant {merchant!r}, collection {collection!r}: syncing {len(df)} row(s)")
    sync(merchant, config, df)
    print(f"Synchronized {len(df)} reconciled row(s) to merchant {merchant!r}'s sink ({config['sink_adapter']})")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
