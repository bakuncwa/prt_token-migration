"""Automated reconciliation source: retrieves the newly issued Primary
Account Number (PAN), as an already-tokenized value, from an authoritative
token vault API -- replacing production-v1.1.0/store_adapters.py's
human-in-Firestore reconciliation step entirely.

This is the change that makes "fully automated, enterprise-scale
reconciliation" possible in the first place (see DESIGN.md's
"Reconciliation source" verdict): a human editing a Firestore document does
not scale past a few hundred records a month, and reintroduces exactly the
PAN-exposure question production-v1.1.0 was designed to avoid by scoping that
exposure to one person, one field, one month. Selected per merchant via
configs/<merchant>.json's "reconciliation_adapter" field, dispatched
identically to extraction_adapters.py's "extraction_adapter" field.

No token vault has been provisioned in this environment. fetch_new_pan_tokens()
below documents the intended request/response shape and raises an explicit
exception rather than presuming a vault endpoint exists -- the identical
convention live/extract_digitalocean.py and live/deploy_digitalocean.py
follow for their own unprovisioned DigitalOcean credentials, and that
production-v1.1.0/open_decisions.py tracks as an open decision for that
directory's DigitalOcean legs.
"""

from __future__ import annotations

import requests

from merchant_config import env


def fetch_new_pan_tokens(merchant: str, config: dict) -> dict[str, str]:
    """Dispatches according to config["reconciliation_adapter"]; introduce a
    case here to onboard a new merchant's token vault. Returns
    card.id -> new PAN token (already deterministically encrypted by the
    vault under the identical KMS key configs/<merchant>.json's
    deidentify.kms_key names, so that the token returned here matches, byte
    for byte, the token deidentify_adapters.deidentify_dataframe() would
    produce for the same underlying PAN -- this is the join key
    staging_service.py's reconcile step relies on)."""
    adapter = config["reconciliation_adapter"]
    if adapter == "token_vault_api":
        return _fetch_via_token_vault_api(merchant, config)
    raise SystemExit(f"Unrecognized reconciliation_adapter {adapter!r} for merchant {merchant!r}")


def _fetch_via_token_vault_api(merchant: str, config: dict) -> dict[str, str]:
    """Intended request shape: a GET against the vault's issuance-lookup
    endpoint, authenticated with <MERCHANT>_VAULT_TOKEN, returning every
    card.id the vault has issued a new PAN token for since the last
    invocation. No vault credentials or endpoint have been provisioned;
    this function raises explicitly rather than fabricating a response."""
    vault_url = config["reconciliation"].get("vault_url") if "reconciliation" in config else None
    if not vault_url:
        raise SystemExit(
            f"No token vault endpoint configured for merchant {merchant!r} "
            "(configs/<merchant>.json's reconciliation.vault_url). No vault credentials have "
            "been provisioned in this environment -- see DESIGN.md's 'Reconciliation source' "
            "verdict and vault_reconciliation_adapters.py's module docstring."
        )
    token = env(f"{merchant.upper()}_VAULT_TOKEN", required=True)
    resp = requests.get(
        f"{vault_url}/{merchant}/issued-tokens",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    resp.raise_for_status()
    return {record["card_id"]: record["pan_token"] for record in resp.json()}
