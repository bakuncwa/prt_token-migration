"""Pluggable extraction from a merchant's source system into raw/<merchant>/.
Selected per merchant via configs/<merchant>.json's "extraction_adapter"
field -- the identical dispatch structure as
production-v1.1.0/extraction_adapters.py.

PCI scoping at the extraction boundary differs deliberately from v1.1.0: that
directory blanks any PAN-shaped column before a single byte reaches GCS,
because its only PAN consumer is a one-time, human-entered reconciliation
field, and avoiding storage entirely is the lower-scope choice. This
directory's reconciliation source (vault_reconciliation_adapters.py) requires
the original PAN to remain joinable against the token vault's response, so
blanking is not available; instead, upload_raw_csv() below routes every
PAN-shaped column through deidentify_adapters.deidentify_dataframe() before
upload, such that raw/<merchant>/ -- like every other stage of this
pipeline -- never holds a PAN in cleartext, only a deterministically
encrypted token. See DESIGN.md's two-condition test for when this directory,
rather than v1.1.0's blank-and-manual design, is the correct choice for a
given merchant.
"""

from __future__ import annotations

import pandas as pd
import requests
from google.cloud import storage

from deidentify_adapters import deidentify_dataframe
from merchant_config import env


def extract(merchant: str, config: dict) -> pd.DataFrame:
    """Dispatches according to config["extraction_adapter"]; introduce a
    case here (and a corresponding _extract_via_*() function) to onboard a
    new extraction method. Returns the fetched raw dataset, PAN intact --
    de-identification happens at upload_raw_csv() below, not here, so that
    every extraction adapter is subject to it uniformly."""
    adapter = config["extraction_adapter"]
    if adapter == "cloud_run_puller":
        return _extract_via_cloud_run_puller(merchant, config)
    if adapter == "dataflow":
        return _extract_via_dataflow(merchant, config)
    raise SystemExit(f"Unrecognized extraction_adapter {adapter!r} for merchant {merchant!r}")


def _extract_via_cloud_run_puller(merchant: str, config: dict) -> pd.DataFrame:
    """Retrieves the merchant's MIT record set via a GET request against its
    DigitalOcean Kubernetes-fronted source API -- structurally identical to
    production-v1.1.0/extraction_adapters.py's equivalent function. Present
    here for parity only: per DESIGN.md's condition (1), any merchant
    genuinely requiring this directory has, by definition, already exceeded
    this adapter's volume threshold and should be onboarded onto
    _extract_via_dataflow() instead."""
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
    """The expected extraction path for any merchant onboarded onto this
    directory -- see DESIGN.md's condition (1): volume or continuity
    sufficient to justify Dataflow's autoscaling worker pool is a
    prerequisite for this directory's adoption in the first place, unlike
    production-v1.1.0, where Dataflow remains an occasional opt-in. No
    merchant has been onboarded onto this pipeline (see DESIGN.md's
    "Deployment status" -- this directory exists for scalability
    demonstration, not live use), so no Apache Beam pipeline definition has
    been authored or deployed yet; this function raises explicitly rather
    than presuming one exists."""
    raise SystemExit(
        f"Dataflow extraction for merchant {merchant!r}: no merchant has been onboarded onto "
        "production-v1.1.1/. Author and deploy an Apache Beam pipeline definition before "
        "resolving this placeholder -- see DESIGN.md's 'Deployment status'."
    )


def upload_raw_csv(bucket: storage.Bucket, blob_name: str, df: pd.DataFrame, config: dict) -> str:
    """De-identifies PAN-shaped columns (see deidentify_adapters.py) prior
    to upload, unconditionally -- every extraction adapter's retrieved data
    is routed through this function, rendering it the single location at
    which PCI scoping must hold in order to hold universally, the identical
    invariant production-v1.1.0/extraction_adapters.py maintains via
    blanking rather than tokenization."""
    df = deidentify_dataframe(df, config)
    blob = bucket.blob(blob_name)
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    blob.upload_from_string(csv_bytes, content_type="text/csv")
    return f"gs://{bucket.name}/{blob_name}"
