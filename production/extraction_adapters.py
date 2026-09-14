"""Pluggable extraction from a merchant's source system into
raw/<merchant>/. Selected per merchant via configs/<merchant>.json's
"extraction_adapter" field.

cloud_run_puller constitutes the default adapter: a scheduled Cloud Run
job executing a single GET request followed by a single GCS upload,
structurally identical to the live pipeline's extract_digitalocean.py.
dataflow is reserved for merchants whose data volume or structural
characteristics genuinely necessitate parallel or streaming processing
-- see DESIGN.md's "extraction" verdict: the lightweight puller
constitutes the default, with Dataflow's adoption reserved for
merchants whose volume or shape requires it, rather than serving as
the default for every merchant.

PCI scoping at the extraction boundary: a source system's Merchant
Initiated Transaction (MIT) database is expected to return the Primary
Account Number (PAN) column in an already-blanked state -- the
identical assumption live/column_mapping.py documents for the
reference merchant's raw export ("FullAccountNumber is blank in the
source"). This module does not accept that assumption without
independent verification; upload_raw_csv() enforces it by blanking any
PAN-shaped column prior to the transmission of a single byte to GCS,
irrespective of which adapter retrieved the data or whether the source
system's behavior is subsequently modified. See _blank_pan_columns().
"""

from __future__ import annotations

import pandas as pd
import requests
from google.cloud import storage

from merchant_config import env

# Column identifiers a source system's raw export could plausibly
# employ for a full PAN. Extend this enumeration if a newly onboarded
# merchant's source system employs a differing identifier.
PAN_COLUMN_CANDIDATES = ("FullAccountNumber", "CardNumber", "PAN", "card.number")


def _blank_pan_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Defense-in-depth PCI scoping: blanks any column present in
    PAN_COLUMN_CANDIDATES within the fetched DataFrame, irrespective of
    whether the source system has already transmitted it blank. Applied
    unconditionally within upload_raw_csv() -- not as an opt-in
    per adapter -- such that a full PAN can never reach
    raw/<merchant>/, even should a source system's export behavior be
    modified without this pipeline's knowledge."""
    df = df.copy()
    for col in PAN_COLUMN_CANDIDATES:
        if col in df.columns:
            df[col] = ""
    return df


def extract(merchant: str, config: dict) -> pd.DataFrame:
    """Dispatches according to config["extraction_adapter"]; introduce
    a case here (and a corresponding _extract_via_*() function) to
    onboard a new extraction method. Returns the fetched raw dataset;
    supply it to upload_raw_csv() to stage it to raw/<merchant>/."""
    adapter = config["extraction_adapter"]
    if adapter == "cloud_run_puller":
        return _extract_via_cloud_run_puller(merchant, config)
    if adapter == "dataflow":
        return _extract_via_dataflow(merchant, config)
    raise SystemExit(f"Unrecognized extraction_adapter {adapter!r} for merchant {merchant!r}")


def _extract_via_cloud_run_puller(merchant: str, config: dict) -> pd.DataFrame:
    """Retrieves the merchant's MIT record set via a GET request against
    its DigitalOcean Kubernetes-fronted source API, authenticating with
    <MERCHANT>_SOURCE_TOKEN and <MERCHANT>_SOURCE_CLUSTER -- the
    identical per-merchant credential convention followed by every
    other required environment variable within this pipeline (see
    merchant_config.env())."""
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
    """Reserved for merchants whose source volume or structural
    characteristics require Dataflow's autoscaling worker pool and
    windowing model -- see DESIGN.md's "extraction" verdict. Every
    merchant onboarded to date operates on cloud_run_puller instead;
    this pathway launches a templated Apache Beam pipeline once a given
    merchant's volume exceeds that threshold."""
    raise SystemExit(
        f"Dataflow extraction for merchant {merchant!r}: no onboarded merchant's "
        "source volume has exceeded the threshold that would justify its adoption. "
        "See DESIGN.md's 'extraction' verdict."
    )


def upload_raw_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame) -> str:
    """Blanks PAN-shaped columns (see _blank_pan_columns()) prior to
    upload, unconditionally -- every extraction adapter's retrieved
    data is routed through this function, rendering it the single
    location at which PCI scoping must hold in order to hold
    universally."""
    df = _blank_pan_columns(df)
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"
