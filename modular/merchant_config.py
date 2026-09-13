"""Per-merchant configuration for the modular Token Migration ETL.

Generalizes original/column_mapping.py (one hardcoded merchant, CardCorp)
and original/collection_naming.py (one fixed collection shape) into a
declarative config any merchant's onboarding can drop in as a new
configs/<merchant>.json, with zero changes to staging_service.py.

configs/cardcorp.json is CardCorp's mapping ported 1:1 from the original
pipeline -- it exists to prove this loader reproduces the live pipeline's
behavior, not as a second, divergent copy of the mapping rules.
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
    """configs/<merchant>.json -> parsed config dict. Raises clearly if a
    merchant hasn't been onboarded yet, rather than silently falling back
    to CardCorp's rules for an unrelated merchant's data."""
    path = CONFIGS_DIR / f"{merchant}.json"
    if not path.exists():
        raise SystemExit(
            f"No config for merchant {merchant!r} at {path}. "
            "Onboard it by adding configs/<merchant>.json (see configs/cardcorp.json)."
        )
    return json.loads(path.read_text())


def collection_for_blob_name(merchant: str, blob_name: str, config: dict) -> str:
    """"transformed/cardcorp/Transformed_May_2025.csv" -> "cardcorp_052025_cardholders".
    Same MMYYYY dating as the original pipeline, namespaced by merchant so
    two merchants' May 2025 batches never collide in the same Firestore
    database."""
    stem = blob_name.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    stem = re.sub(r"^(Transformed_|Cleaned_|Reconciled_)+", "", stem)
    try:
        parsed = datetime.strptime(stem.replace("_", " "), "%B %Y")
    except ValueError as e:
        raise ValueError(
            f"Can't derive a collection from blob name {blob_name!r}: "
            f"expected a '<Month> <Year>' stem, e.g. 'Transformed_May_2025.csv'."
        ) from e
    return f"{merchant}_{parsed:%m%Y}_{config['collection_prefix']}"


def month_year_stem_for_collection(collection: str) -> str:
    """"cardcorp_052025_cardholders" -> "May_2025", the inverse half of
    collection_for_blob_name() (merchant and prefix already known by the
    caller from the collection name's own structure)."""
    match = _COLLECTION_RE.match(collection)
    if not match:
        raise ValueError(f"{collection!r} is not a <merchant>_MMYYYY_<prefix> collection name")
    _merchant, month, year, _prefix = match.groups()
    parsed = datetime.strptime(f"{month} {year}", "%m %Y")
    return f"{parsed:%B_%Y}"


def merchant_and_prefix_for_collection(collection: str) -> tuple[str, str] | None:
    match = _COLLECTION_RE.match(collection)
    if not match:
        return None
    merchant, _month, _year, prefix = match.groups()
    return merchant, prefix


def env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value
