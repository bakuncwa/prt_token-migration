"""Reconciliation store. Selected per merchant via
configs/<merchant>.json's "store" block -- staging_service.py never
imports a specific store client directly.

Firestore is the store: the same one-field-per-document shape as
live/load_to_firestore.py and export_firestore_to_gcs.py, generalized
to whatever fields configs/<merchant>.json's reconciled_by declares.
"""

from __future__ import annotations

from google.cloud import firestore


def read_reconciled_values(merchant: str, collection: str, config: dict) -> dict[str, dict[str, str]]:
    """id -> {field: value} for every field configs/<merchant>.json's
    reconciled_by declares, read straight from the configured store.
    Dispatches on config["store"]["type"]; add a case here (and a
    matching read_*() below) to onboard a new store type."""
    store_type = config["store"]["type"]
    if store_type == "firestore":
        return _read_from_firestore(collection, config)
    raise SystemExit(f"Unknown store type {store_type!r} for merchant {merchant!r}")


def _read_from_firestore(collection: str, config: dict) -> dict[str, dict[str, str]]:
    database = config["store"].get("database", "(default)")
    fields = config["reconciled_by"]["fields"]
    db = firestore.Client(database=database)
    return {
        doc.id: {field: doc.to_dict().get(field, "") for field in fields}
        for doc in db.collection(collection).stream()
    }
