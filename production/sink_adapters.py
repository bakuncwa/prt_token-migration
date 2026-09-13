"""Pluggable sync of cleaned/<merchant>/*.csv back to a merchant's target
system. Selected per merchant via configs/<merchant>.json's
"sink_adapter" field.

digitalocean_kubernetes is CardCorp's real instance (Payreto's
DigitalOcean-hosted MIT database), ported from the live pipeline's
deploy_digitalocean.py:push_records_to_digitalocean() -- and it carries
the same placeholder status: no live DigitalOcean API write credentials
are configured in this environment yet, for either the live or the
production pipeline. See open_decisions.py.
"""

from __future__ import annotations

import pandas as pd

from merchant_config import env


def sync(merchant: str, config: dict, df: pd.DataFrame) -> None:
    """Dispatches on config["sink_adapter"]; add a case here to onboard a
    new merchant's target system."""
    adapter = config["sink_adapter"]
    if adapter == "digitalocean_kubernetes":
        _sync_to_digitalocean_kubernetes(merchant, config, df)
        return
    raise SystemExit(f"Unknown sink_adapter {adapter!r} for merchant {merchant!r}")


def _sync_to_digitalocean_kubernetes(merchant: str, config: dict, df: pd.DataFrame) -> None:
    """POST reconciled rows back through the DigitalOcean Kubernetes API.

    Placeholder: DigitalOcean write access has not been provisioned for
    Payreto in this environment. Replace the body once the actual
    DOKS-fronted target endpoint is known -- same shape as the live
    pipeline's push_records_to_digitalocean(), e.g.:

        resp = requests.post(
            f"https://{cluster}.k8s.ondigitalocean.com/{merchant}/records",
            headers={"Authorization": f"Bearer {token}"},
            json=df.to_dict(orient="records"),
            timeout=30,
        )
        resp.raise_for_status()
    """
    token = env("DIGITALOCEAN_TOKEN")
    cluster = env(f"{merchant.upper()}_DO_CLUSTER_NAME")
    if not token or not cluster:
        raise SystemExit(
            f"digitalocean_kubernetes sink for merchant {merchant!r} is a placeholder -- "
            "no DigitalOcean API write access is configured yet. Implement it once "
            "DIGITALOCEAN_TOKEN write access is granted. See open_decisions.py."
        )
    raise SystemExit("Not implemented: replace with a real POST to the DOKS-fronted endpoint.")
