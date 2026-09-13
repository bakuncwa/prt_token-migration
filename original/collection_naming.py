"""Derives the dated Firestore collection name (MMYYYY_cardholders) from a
transformed-CSV blob name, and back again.

Each month's dataset gets its own collection (e.g. "052025_cardholders"
for May 2025) instead of every upload piling into one flat "cardholders"
collection, so re-processing an old month never collides with the
current one.
"""

from __future__ import annotations

import re
from datetime import datetime

COLLECTION_SUFFIX = "_cardholders"
_COLLECTION_RE = re.compile(r"^(\d{2})(\d{4})" + COLLECTION_SUFFIX + r"$")
_STEM_PREFIX_RE = re.compile(r"^(Transformed_|Cleaned_|Reconciled_)+")


def collection_for_blob_name(blob_name: str) -> str:
    """"transformed/Transformed_May_2025.csv" -> "052025_cardholders"."""
    stem = blob_name.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    stem = _STEM_PREFIX_RE.sub("", stem)
    try:
        parsed = datetime.strptime(stem.replace("_", " "), "%B %Y")
    except ValueError as e:
        raise ValueError(
            f"Can't derive a MMYYYY_cardholders collection from blob name "
            f"{blob_name!r}: expected a '<Month> <Year>' stem, e.g. "
            f"'Transformed_May_2025.csv'."
        ) from e
    return f"{parsed:%m%Y}{COLLECTION_SUFFIX}"


def month_year_stem_for_collection(collection: str) -> str:
    """"052025_cardholders" -> "May_2025", the inverse of the month/year
    portion of collection_for_blob_name()."""
    match = _COLLECTION_RE.match(collection)
    if not match:
        raise ValueError(
            f"{collection!r} is not a valid {{MM}}{{YYYY}}{COLLECTION_SUFFIX} "
            "collection name"
        )
    month, year = match.groups()
    parsed = datetime.strptime(f"{month} {year}", "%m %Y")
    return f"{parsed:%B_%Y}"


def is_dated_cardholders_collection(collection: str) -> bool:
    return bool(_COLLECTION_RE.match(collection))
