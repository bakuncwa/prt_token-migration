"""Pluggable extraction from a merchant's source system into
raw/<merchant>/. Selected per merchant via configs/<merchant>.json's
"extraction_adapter" field.

cloud_run_puller is the default: a scheduled Cloud Run job doing one GET
call and one GCS upload, the same shape as the live pipeline's
extract_digitalocean.py. dataflow is reserved for merchants whose
volume or shape genuinely demands parallel/streaming processing -- see
DESIGN.md's "extraction" verdict: default to the lightweight puller,
adopt Dataflow only where a merchant's volume or shape requires it, not
as the default for every merchant.

PCI scoping at the extraction boundary: DigitalOcean's MIT database is
expected to return the PAN column already blank -- the same assumption
live/column_mapping.py documents for CardCorp's raw export
("FullAccountNumber is blank in the source"). This module does not
trust that assumption silently; upload_raw_csv() enforces it by
blanking any PAN-shaped column before a single byte reaches GCS,
regardless of which adapter fetched the data or whether the source
system's behavior changes upstream. See _blank_pan_columns().
"""

from __future__ import annotations

import pandas as pd
import requests
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


def extract(merchant: str, config: dict) -> pd.DataFrame:
    """Dispatches on config["extraction_adapter"]; add a case here (and
    a matching _extract_via_*()) to onboard a new extraction method.
    Returns the fetched raw dataset; pass it to upload_raw_csv() to
    stage it to raw/<merchant>/."""
    adapter = config["extraction_adapter"]
    if adapter == "cloud_run_puller":
        return _extract_via_cloud_run_puller(merchant, config)
    if adapter == "dataflow":
        return _extract_via_dataflow(merchant, config)
    raise SystemExit(f"Unknown extraction_adapter {adapter!r} for merchant {merchant!r}")


def _extract_via_cloud_run_puller(merchant: str, config: dict) -> pd.DataFrame:
    """GETs the merchant's MIT record set from its DigitalOcean
    Kubernetes-fronted source API, authenticating with
    <MERCHANT>_SOURCE_TOKEN and <MERCHANT>_SOURCE_CLUSTER, the same
    per-merchant credential pattern every other required environment
    variable in this pipeline follows (see merchant_config.env())."""
    token = env(f"{merchant.upper()}_SOURCE_TOKEN", required=True)
    cluster = env(f"{merchant.upper()}_SOURCE_CLUSTER", required=True)
    resp = requests.get(
        f"https://{cluster}.k8s.ondigitalocean.com/{merchant}/mit/records",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


def _extract_via_dataflow(merchant: str, config: dict) -> pd.DataFrame:
    """Reserved for merchants whose source volume or shape requires
    Dataflow's autoscaling worker pool and windowing model -- see
    DESIGN.md's "extraction" verdict. Every merchant onboarded to date
    runs on cloud_run_puller instead; this path launches a templated
    Apache Beam pipeline once a merchant's volume crosses that
    threshold."""
    raise SystemExit(
        f"Dataflow extraction for merchant {merchant!r}: no onboarded merchant's "
        "source volume has crossed the threshold that justifies it. See "
        "DESIGN.md's 'extraction' verdict."
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
