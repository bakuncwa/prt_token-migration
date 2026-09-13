"""Real test of the modular staging service against live GCP data --
not mocks. Proves two things:

1. transform_dataframe() (modular, config-driven) reproduces
   original/column_mapping.py's transform_dataframe() (hardcoded) for
   the same real CardCorp raw data.
2. reconcile_dataframe() (modular) reproduces
   original/export_firestore_to_gcs.py's merge_card_numbers() for the
   same real Firestore reconciliation data -- i.e. the actual thing
   this round of work was asked to guarantee: when a card number is
   reconciled in Firestore, the staging layer's transformation logic
   still works, now running through the generalized Cloud Run script
   instead of the original's CardCorp-only one.

Then runs the modular pipeline's real GCS I/O (run_transform,
run_reconcile) end to end against a separate test bucket seeded from
the same source data, per open_decisions.py's resolved
"modular_test_strategy" item.

Requires gcloud application-default credentials for project
cardcorp-token-migration.

Usage:
  MODULAR_TEST_BUCKET=cardcorp-token-migration-modular-test \\
    python test_staging_service.py
"""

from __future__ import annotations

import sys

sys.path.insert(0, "../original")

from google.cloud import firestore, storage

from merchant_config import env, load_merchant_config
from staging_service import download_csv, reconcile_dataframe, run_reconcile, run_transform, transform_dataframe

ORIGINAL_BUCKET = "cardcorp-token-0dc1f93138"
ORIGINAL_PROJECT = "cardcorp-token-migration"


def test_transform_matches_original() -> None:
    print("[1/3] transform_dataframe() vs original/column_mapping.py ...")
    from column_mapping import transform_dataframe as original_transform  # original/

    client = storage.Client(project=ORIGINAL_PROJECT)
    bucket = client.bucket(ORIGINAL_BUCKET)
    raw_df = download_csv(bucket, "raw/January 2026.csv")

    config = load_merchant_config("cardcorp")
    modular_out = transform_dataframe(raw_df, config)
    original_out = original_transform(raw_df)

    assert list(modular_out.columns) == list(original_out.columns), (
        f"column mismatch: {list(modular_out.columns)} vs {list(original_out.columns)}"
    )
    assert modular_out.equals(original_out), "transformed values differ from the original pipeline's output"
    print(f"      OK -- {len(modular_out)} rows, {len(modular_out.columns)} columns, byte-identical")


def test_reconcile_matches_original() -> None:
    print("[2/3] reconcile_dataframe() vs original/export_firestore_to_gcs.py, live Firestore data ...")
    from export_firestore_to_gcs import merge_card_numbers  # original/

    client = storage.Client(project=ORIGINAL_PROJECT)
    bucket = client.bucket(ORIGINAL_BUCKET)
    transformed_df = download_csv(bucket, "transformed/Transformed_January_2026.csv")

    db = firestore.Client(project=ORIGINAL_PROJECT, database="(default)")
    docs = list(db.collection("012026_cardholders").stream())
    card_numbers = {doc.id: doc.to_dict().get("card_number", "") for doc in docs}
    reconciled_values = {card_id: {"card.number": number} for card_id, number in card_numbers.items()}

    config = load_merchant_config("cardcorp")
    modular_out = reconcile_dataframe(transformed_df, reconciled_values, config)
    original_out = merge_card_numbers(transformed_df, card_numbers)

    assert modular_out.equals(original_out), "reconciled values differ from the original pipeline's output"
    print(f"      OK -- {len(docs)} Firestore record(s), reconciled output byte-identical")


def test_end_to_end_on_test_bucket() -> None:
    print("[3/3] run_transform() + run_reconcile() end to end on a separate test bucket ...")
    test_bucket = env("MODULAR_TEST_BUCKET", required=True)

    client = storage.Client(project=ORIGINAL_PROJECT)
    src_bucket = client.bucket(ORIGINAL_BUCKET)
    dst_bucket = client.bucket(test_bucket)

    raw_blob = src_bucket.blob("raw/January 2026.csv")
    dst_bucket.blob("raw/cardcorp/January 2026.csv").upload_from_string(
        raw_blob.download_as_bytes(), content_type="text/csv"
    )
    print(f"      seeded gs://{test_bucket}/raw/cardcorp/January 2026.csv from the original bucket")

    transformed_uri = run_transform(test_bucket, "cardcorp", "raw/cardcorp/January 2026.csv")
    print(f"      transformed: {transformed_uri}")

    db = firestore.Client(project=ORIGINAL_PROJECT, database="(default)")
    docs = list(db.collection("012026_cardholders").stream())
    reconciled_values = {
        doc.id: {"card.number": doc.to_dict().get("card_number", "")} for doc in docs
    }
    cleaned_uri = run_reconcile(
        test_bucket, "cardcorp", "transformed/cardcorp/Transformed_January_2026.csv", reconciled_values
    )
    print(f"      reconciled: {cleaned_uri}")


if __name__ == "__main__":
    test_transform_matches_original()
    test_reconcile_matches_original()
    test_end_to_end_on_test_bucket()
    print("\nAll modular staging service tests passed.")
