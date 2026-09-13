"""Pluggable extraction from a merchant's source system into
raw/<merchant>/. Selected per merchant via configs/<merchant>.json's
"extraction_adapter" field.

cloud_run_puller is the default: a scheduled Cloud Run job doing one GET
call and one GCS upload, the same shape as the live pipeline's
extract_digitalocean.py. dataflow is an explicit placeholder for the
merchants that actually need it -- see DESIGN.md's "extraction" verdict:
default to the lightweight puller, adopt Dataflow only where a
merchant's volume or shape genuinely demands parallel/streaming
processing, not as the default for every merchant.

PCI scoping at the extraction boundary: in production, DigitalOcean's
MIT database is expected to return the PAN column already blank -- the
same assumption live/column_mapping.py documents for CardCorp's raw
export ("FullAccountNumber is blank in the source"). This module does
not trust that assumption silently; upload_raw_csv() enforces it by
blanking any PAN-shaped column before a single byte reaches GCS,
regardless of which adapter fetched the data or whether the source
system's behavior changes upstream. See _blank_pan_columns().
"""

from __future__ import annotations

import pandas as pd
from google.cloud import storage

from merchant_config import env

# Column names a source system's raw export could plausibly use for a
# full PAN. Extend this if a new merchant's source uses a different name.
PAN_COLUMN_CANDIDATES = ("FullAccountNumber", "CardNumber", "PAN", "card.number")


def _blank_pan_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Defense-in-depth PCI scoping: blanks any column in
    PAN_COLUMN_CANDIDATES present in the fetched DataFrame, regardless
    of whether the source system already sent it blank. Applied
    unconditionally in upload_raw_csv() -- not opt-in per adapter --
    so a full PAN can never reach raw/<merchant>/ even if a future
    source system's export behavior changes without this pipeline's
    knowledge."""
    df = df.copy()
    for col in PAN_COLUMN_CANDIDATES:
        if col in df.columns:
            df[col] = ""
    return df


def extract(merchant: str, config: dict, raw_blob_name: str) -> str:
    """Dispatches on config["extraction_adapter"]; add a case here (and
    a matching _extract_via_*()) to onboard a new extraction method."""
    adapter = config["extraction_adapter"]
    if adapter == "cloud_run_puller":
        return _extract_via_cloud_run_puller(merchant, config, raw_blob_name)
    if adapter == "dataflow":
        return _extract_via_dataflow(merchant, config, raw_blob_name)
    raise SystemExit(f"Unknown extraction_adapter {adapter!r} for merchant {merchant!r}")


def _extract_via_cloud_run_puller(merchant: str, config: dict, raw_blob_name: str) -> str:
    """GET the source dataset and stage it to raw/<merchant>/ via
    upload_raw_csv(), which blanks any PAN-shaped column before upload
    -- see _blank_pan_columns(). Placeholder otherwise, same status as
    the live pipeline's extract_digitalocean.py: no live source-system
    API credentials are configured in this environment, so this
    documents the intended shape rather than guessing at an endpoint."""
    token = env(f"{merchant.upper()}_SOURCE_TOKEN")
    if not token:
        raise SystemExit(
            f"cloud_run_puller extraction for merchant {merchant!r} is a placeholder -- "
            f"no source-system API access is configured yet. Set "
            f"{merchant.upper()}_SOURCE_TOKEN and implement the GET once access is granted."
        )
    raise SystemExit("Not implemented: replace with a real GET against the merchant's source API.")


def _extract_via_dataflow(merchant: str, config: dict, raw_blob_name: str) -> str:
    """Placeholder: no merchant has needed Dataflow-scale extraction yet
    (today's volumes are single-digit-thousands of rows a month across
    every onboarded merchant combined). Launching a templated Beam
    pipeline here is the intended shape once one does -- see
    DESIGN.md's 'extraction' verdict for the reasoning."""
    raise SystemExit(
        f"Dataflow extraction for merchant {merchant!r} is not implemented -- "
        "no onboarded merchant's source volume has justified it yet."
    )


def upload_raw_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    """Blanks PAN-shaped columns (see _blank_pan_columns()) before
    upload, unconditionally -- every extraction adapter's fetched data
    passes through here, so this is the one place PCI scoping has to
    hold for it to hold everywhere."""
    df = _blank_pan_columns(df)
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"
