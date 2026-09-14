"""Derives the dated Firestore collection identifier
(MMYYYY_cardholders) from a transformed-CSV blob name, and performs
the inverse derivation.

Each month's dataset is assigned its own collection (for example,
"052025_cardholders" for May 2025), rather than every upload
accumulating within a single, flat "cardholders" collection, such that
the reprocessing of a prior month does not produce a collision with
the current month.
"""

from __future__ import annotations

import re
from datetime import datetime

COLLECTION_SUFFIX = "_cardholders"
_COLLECTION_RE = re.compile(r"^(\d{2})(\d{4})" + COLLECTION_SUFFIX + r"$")
_STEM_PREFIX_RE = re.compile(r"^(Transformed_|Cleaned_|Reconciled_)+")


def collection_for_blob_name(blob_name: str) -> str:
    """Derives a collection identifier from a blob name, for example
    "transformed/Transformed_May_2025.csv" -> "052025_cardholders"."""
    stem = blob_name.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    stem = _STEM_PREFIX_RE.sub("", stem)
    try:
        parsed = datetime.strptime(stem.replace("_", " "), "%B %Y")
    except ValueError as e:
        raise ValueError(
            f"Cannot derive a MMYYYY_cardholders collection identifier from blob name "
            f"{blob_name!r}: a '<Month> <Year>' stem is expected, for example "
            f"'Transformed_May_2025.csv'."
        ) from e
    return f"{parsed:%m%Y}{COLLECTION_SUFFIX}"


def month_year_stem_for_collection(collection: str) -> str:
    """Performs the inverse of collection_for_blob_name()'s month/year
    derivation, for example "052025_cardholders" -> "May_2025"."""
    match = _COLLECTION_RE.match(collection)
    if not match:
        raise ValueError(
            f"{collection!r} is not a valid {{MM}}{{YYYY}}{COLLECTION_SUFFIX} "
            "collection identifier"
        )
    month, year = match.groups()
    parsed = datetime.strptime(f"{month} {year}", "%m %Y")
    return f"{parsed:%B_%Y}"


def is_dated_cardholders_collection(collection: str) -> bool:
    """Returns whether the supplied collection identifier conforms to
    the MMYYYY_cardholders structure."""
    return bool(_COLLECTION_RE.match(collection))
