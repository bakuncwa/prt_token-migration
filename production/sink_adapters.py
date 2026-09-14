"""Pluggable synchronization of cleaned/<merchant>/*.csv back to a
merchant's target system. Selected per merchant via
configs/<merchant>.json's "sink_adapter" field.

digitalocean_kubernetes constitutes the reference instance: a
DigitalOcean-hosted Merchant Initiated Transaction (MIT) database
operated by Payreto Services Inc., a payment service provider and
financial outsourcing operator (see
https://www.payreto.com/about-us/), ported from the live pipeline's
deploy_digitalocean.py:push_records_to_digitalocean().
"""

from __future__ import annotations

import pandas as pd
import requests

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
    """Transmits reconciled rows via a POST request against the
    DigitalOcean Kubernetes API, authenticating with DIGITALOCEAN_TOKEN
    and <MERCHANT>_DO_CLUSTER_NAME -- structurally identical to the
    live pipeline's push_records_to_digitalocean()."""
    token = env("DIGITALOCEAN_TOKEN", required=True)
    cluster = env(f"{merchant.upper()}_DO_CLUSTER_NAME", required=True)
    resp = requests.post(
        f"https://{cluster}.k8s.ondigitalocean.com/{merchant}/records",
        headers={"Authorization": f"Bearer {token}"},
        json=df.to_dict(orient="records"),
        timeout=30,
    )
    resp.raise_for_status()
