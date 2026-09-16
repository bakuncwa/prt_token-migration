"""Pluggable synchronization of reconciled rows back to a merchant's target
system. Selected per merchant via configs/<merchant>.json's "sink_adapter"
field, dispatched identically to production-v1.1.0/sink_adapters.py.

The write-back procedure differs from v1.1.0 in one deliberate respect: this
is the single point in the entire pipeline at which a PAN is ever
re-identified (see DESIGN.md's "Re-identification: gated to the write-back
path exclusively" verdict). _sync_to_digitalocean_kubernetes() below calls
deidentify_adapters.reidentify_value() immediately before the POST, holds the
plaintext value only for the duration of that request, and never persists
it -- not to GCS, not to BigQuery, not to logs. Triggered by a Pub/Sub
message published once staging_service.py's reconcile step commits to
BigQuery, in place of v1.1.0's GCS-finalize-on-cleaned/ trigger, since
Pub/Sub is the event backbone this directory's automated (non-Firestore)
reconciliation path uses -- see staging_service.py's on_reconciled_event().
"""

from __future__ import annotations

import pandas as pd
import requests

from deidentify_adapters import reidentify_value
from merchant_config import env


def sync(merchant: str, config: dict, df: pd.DataFrame) -> None:
    """Dispatches according to config["sink_adapter"]; introduce a case
    here to onboard a new merchant's target system."""
    adapter = config["sink_adapter"]
    if adapter == "digitalocean_kubernetes":
        _sync_to_digitalocean_kubernetes(merchant, config, df)
        return
    raise SystemExit(f"Unrecognized sink_adapter {adapter!r} for merchant {merchant!r}")


def _sync_to_digitalocean_kubernetes(merchant: str, config: dict, df: pd.DataFrame) -> None:
    """Re-identifies each reconciled row's PAN column(s) just-in-time,
    transmits the plaintext result via a single POST request against the
    DigitalOcean Kubernetes API, then allows the re-identified DataFrame to
    fall out of scope -- it is never written anywhere. Authenticates with
    DIGITALOCEAN_TOKEN and <MERCHANT>_DO_CLUSTER_NAME, the identical
    convention production-v1.1.0/sink_adapters.py follows."""
    token = env("DIGITALOCEAN_TOKEN", required=True)
    cluster = env(f"{merchant.upper()}_DO_CLUSTER_NAME", required=True)

    fields = config["reconciled_by"]["fields"]
    plaintext_df = df.copy()
    for field in fields:
        if field in plaintext_df.columns:
            plaintext_df[field] = plaintext_df[field].map(lambda v: reidentify_value(v, field, config))

    resp = requests.post(
        f"https://{cluster}.k8s.ondigitalocean.com/{merchant}/records",
        headers={"Authorization": f"Bearer {token}"},
        json=plaintext_df.to_dict(orient="records"),
        timeout=30,
    )
    resp.raise_for_status()
    del plaintext_df
