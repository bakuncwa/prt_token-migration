"""Tests the production staging service two ways: against real CardCorp
data (proving it reproduces the live pipeline exactly) and against a
structurally different synthetic merchant (proving it generalizes
beyond CardCorp, not just relabels it). Four checks:

1. transform_dataframe() (production, config-driven) reproduces
   live/column_mapping.py's transform_dataframe() (hardcoded) for
   the same real CardCorp raw data. Requires live GCP credentials.
2. transform_dataframe() against configs/samplepay.json -- a synthetic
   merchant with different column names, a different Expiry date
   format (MM/YYYY vs. CardCorp's YYYY-MM), and a different repair
   field length. Local only, no cloud credentials needed. This check
   is what caught _split_date() silently ignoring the config's
   source_format field and hardcoding CardCorp's date shape -- see
   staging_service.py's _split_date() docstring.
3. reconcile_dataframe() (production) reproduces
   live/export_firestore_to_gcs.py's merge_card_numbers() for the
   same real Firestore reconciliation data -- i.e. the actual thing
   this round of work was asked to guarantee: when a card number is
   reconciled in Firestore, the staging layer's transformation logic
   still works, now running through the generalized Cloud Run script
   instead of the live pipeline's CardCorp-only one.
4. run_transform() and run_reconcile() (production's real GCS I/O) end
   to end against a separate test bucket seeded from the same source
   data, per open_decisions.py's resolved "production_test_strategy"
   item.

Checks 1, 3, and 4 require gcloud application-default credentials for
project cardcorp-token-migration. Check 2 does not and can run on its
own, offline, at any time.

Usage:
  PRODUCTION_TEST_BUCKET=cardcorp-token-migration-production-test \\
    python test_staging_service.py
"""

from __future__ import annotations

import sys

sys.path.insert(0, "../live")

import pandas as pd
from google.cloud import firestore, storage

from merchant_config import env, load_merchant_config
from staging_service import download_csv, reconcile_dataframe, run_reconcile, run_transform, transform_dataframe

LIVE_BUCKET = "cardcorp-token-0dc1f93138"
LIVE_PROJECT = "cardcorp-token-migration"


def test_transform_matches_live() -> None:
    print("[1/4] transform_dataframe() vs live/column_mapping.py, real CardCorp data ...")
    from column_mapping import transform_dataframe as live_transform  # live/

    client = storage.Client(project=LIVE_PROJECT)
    bucket = client.bucket(LIVE_BUCKET)
    raw_df = download_csv(bucket, "raw/January 2026.csv")

    config = load_merchant_config("cardcorp")
    production_out = transform_dataframe(raw_df, config)
    live_out = live_transform(raw_df)

    assert list(production_out.columns) == list(live_out.columns), (
        f"column mismatch: {list(production_out.columns)} vs {list(live_out.columns)}"
    )
    assert production_out.equals(live_out), "transformed values differ from the live pipeline's output"
    print(f"      OK -- {len(production_out)} rows, {len(production_out.columns)} columns, byte-identical")


def test_transform_for_different_schema() -> None:
    print("[2/4] transform_dataframe() against a structurally different sample merchant (local, no cloud) ...")
    raw_df = pd.read_csv("sample_data/samplepay_raw.csv", dtype=str, keep_default_na=False)
    config = load_merchant_config("samplepay")

    out = transform_dataframe(raw_df, config)

    expected_columns = [
        "name", "email", "id", "card.id", "card.transaction_ids", "card.number",
        "card.exp_year", "card.exp_month", "Branch_Code", "card.address_city", "Notes",
    ]
    assert list(out.columns) == expected_columns, f"unexpected columns: {list(out.columns)}"

    first = out.iloc[0]
    assert first["name"] == "Jane Sample"
    assert first["email"] == "jane.sample@example.test"
    assert first["id"] == "ACC-0001"
    assert first["card.id"] == "TOK-AAA111"
    assert first["card.transaction_ids"] == "" and first["card.number"] == "", (
        "copy_then_blank should leave the trailing targets empty"
    )
    assert (first["card.exp_year"], first["card.exp_month"]) == ("2027", "03"), (
        f"MM/YYYY split_date parsed wrong: {first['card.exp_year']}-{first['card.exp_month']}"
    )
    assert first["Branch_Code"] == "004521", f"zero_pad_if_length(5->6) parsed wrong: {first['Branch_Code']!r}"
    assert first["card.address_city"] == "Springfield"

    print(f"      OK -- {len(out)} rows, {len(out.columns)} columns; "
          f"MM/YYYY dates and a 5-to-6-digit repair both parsed correctly")


def test_reconcile_matches_live() -> None:
    print("[3/4] reconcile_dataframe() vs live/export_firestore_to_gcs.py, live Firestore data ...")
    from export_firestore_to_gcs import merge_card_numbers  # live/

    client = storage.Client(project=LIVE_PROJECT)
    bucket = client.bucket(LIVE_BUCKET)
    transformed_df = download_csv(bucket, "transformed/Transformed_January_2026.csv")

    db = firestore.Client(project=LIVE_PROJECT, database="(default)")
    docs = list(db.collection("012026_cardholders").stream())
    card_numbers = {doc.id: doc.to_dict().get("card_number", "") for doc in docs}
    reconciled_values = {card_id: {"card.number": number} for card_id, number in card_numbers.items()}

    config = load_merchant_config("cardcorp")
    production_out = reconcile_dataframe(transformed_df, reconciled_values, config)
    live_out = merge_card_numbers(transformed_df, card_numbers)

    assert production_out.equals(live_out), "reconciled values differ from the live pipeline's output"
    print(f"      OK -- {len(docs)} Firestore record(s), reconciled output byte-identical")


def test_end_to_end_on_test_bucket() -> None:
    print("[4/4] run_transform() + run_reconcile() end to end on a separate test bucket ...")
    test_bucket = env("PRODUCTION_TEST_BUCKET", required=True)

    client = storage.Client(project=LIVE_PROJECT)
    src_bucket = client.bucket(LIVE_BUCKET)
    dst_bucket = client.bucket(test_bucket)

    raw_blob = src_bucket.blob("raw/January 2026.csv")
    dst_bucket.blob("raw/cardcorp/January 2026.csv").upload_from_string(
        raw_blob.download_as_bytes(), content_type="text/csv"
    )
    print(f"      seeded gs://{test_bucket}/raw/cardcorp/January 2026.csv from the live bucket")

    transformed_uri = run_transform(test_bucket, "cardcorp", "raw/cardcorp/January 2026.csv")
    print(f"      transformed: {transformed_uri}")

    db = firestore.Client(project=LIVE_PROJECT, database="(default)")
    docs = list(db.collection("012026_cardholders").stream())
    reconciled_values = {
        doc.id: {"card.number": doc.to_dict().get("card_number", "")} for doc in docs
    }
    cleaned_uri = run_reconcile(
        test_bucket, "cardcorp", "transformed/cardcorp/Transformed_January_2026.csv", reconciled_values
    )
    print(f"      reconciled: {cleaned_uri}")


if __name__ == "__main__":
    test_transform_matches_live()
    test_transform_for_different_schema()
    test_reconcile_matches_live()
    test_end_to_end_on_test_bucket()
    print("\nAll production staging service tests passed.")
