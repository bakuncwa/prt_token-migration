"""Per-merchant configuration for production-v1.1.1's de-identification/
re-identification pipeline.

Structurally identical to production-v1.1.0/merchant_config.py; reproduced
here (rather than imported cross-directory) so that each versioned pipeline
remains independently deployable as its own Cloud Run source root, per
SETUP.md's "Architectural constraint" on the Python Cloud Functions
buildpack. configs/<merchant>.json in this directory carries three
additional fields v1.1.0 has no use for: "deidentify" (kms_key, dlp_project),
"reconciliation_adapter" (the automated vault source, in place of a human
Firestore entry), and a "bigquery" store type -- see configs/reference.json
and DESIGN.md.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime
from pathlib import Path

CONFIGS_DIR = Path(__file__).parent / "configs"

_COLLECTION_RE = re.compile(r"^([a-z0-9]+)_(\d{2})(\d{4})_([a-z0-9]+)$")


def load_merchant_config(merchant: str) -> dict:
    """configs/<merchant>.json -> parsed configuration dictionary.
    Raises explicitly if a merchant has not yet been onboarded, rather
    than silently defaulting to another merchant's rules for
    unrelated data."""
    path = CONFIGS_DIR / f"{merchant}.json"
    if not path.exists():
        raise SystemExit(
            f"No configuration exists for merchant {merchant!r} at {path}. "
            "Onboard this merchant by introducing configs/<merchant>.json "
            "(see configs/reference.json for the requisite schema)."
        )
    return json.loads(path.read_text())


def collection_for_blob_name(merchant: str, blob_name: str, config: dict) -> str:
    """"transformed/<merchant>/Transformed_May_2025.csv" ->
    "<merchant>_052025_cardholders". Retained from v1.1.0 for the BigQuery
    table-partition naming convention (see store_adapters.py); no Firestore
    collection exists in this pipeline."""
    stem = blob_name.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    stem = re.sub(r"^(Transformed_|Cleaned_|Reconciled_)+", "", stem)
    try:
        parsed = datetime.strptime(stem.replace("_", " "), "%B %Y")
    except ValueError as e:
        raise ValueError(
            f"Cannot derive a collection identifier from blob name {blob_name!r}: "
            f"a '<Month> <Year>' stem is expected, for example 'Transformed_May_2025.csv'."
        ) from e
    return f"{merchant}_{parsed:%m%Y}_{config['collection_prefix']}"


def month_year_stem_for_collection(collection: str) -> str:
    """Inverse of collection_for_blob_name()."""
    match = _COLLECTION_RE.match(collection)
    if not match:
        raise ValueError(f"{collection!r} is not a valid <merchant>_MMYYYY_<prefix> collection identifier")
    _merchant, month, year, _prefix = match.groups()
    parsed = datetime.strptime(f"{month} {year}", "%m %Y")
    return f"{parsed:%B_%Y}"


def merchant_and_prefix_for_collection(collection: str) -> tuple[str, str] | None:
    """Extracts the (merchant, collection_prefix) pair from a
    <merchant>_MMYYYY_<prefix> identifier, or returns None if the
    identifier does not conform to this structure."""
    match = _COLLECTION_RE.match(collection)
    if not match:
        return None
    merchant, _month, _year, prefix = match.groups()
    return merchant, prefix


def env(name: str, default: str | None = None, required: bool = False) -> str:
    """Retrieves environment variable `name`, raising explicitly if it
    is designated as required and absent."""
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value
