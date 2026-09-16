"""Reconciliation store: BigQuery, in place of
production-v1.1.0/store_adapters.py's Firestore -- selected per merchant via
configs/<merchant>.json's "store" block, identically structured to v1.1.0's.

Firestore's single-field, Console-edited design has no equivalent here:
there is no human reviewer step for BigQuery to support, since
vault_reconciliation_adapters.py supplies the reconciled value
automatically. BigQuery's role instead is the "queryable at enterprise
level" requirement this directory exists to serve (see DESIGN.md) -- every
transformed and reconciled row lands here, de-identified, so that downstream
analytics or fraud-review queries never need to touch GCS CSVs directly.
Every column landed here, including the PAN token, remains the
deterministically encrypted value deidentify_adapters.py produced; nothing
in this module ever writes a re-identified value to BigQuery.
"""

from __future__ import annotations

import pandas as pd
from google.cloud import bigquery


def _table_ref(collection: str, config: dict) -> str:
    project_id = config["store"]["project"]
    dataset = config["store"]["dataset"]
    return f"{project_id}.{dataset}.{collection}"


def write_dataframe(df: pd.DataFrame, collection: str, config: dict) -> None:
    """Loads df into the merchant's BigQuery table, one table per
    <merchant>_MMYYYY_<prefix> collection identifier -- the identical
    per-merchant-per-month namespacing convention
    production-v1.1.0/merchant_config.py's collection_for_blob_name()
    establishes, reused here (see this directory's merchant_config.py) as
    the BigQuery table name rather than a Firestore collection name.
    Truncate-and-replace semantics: each invocation reflects the current
    state of transform_dataframe()/reconcile_dataframe()'s output for that
    collection, not an append."""
    client = bigquery.Client(project=config["store"]["project"])
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, _table_ref(collection, config), job_config=job_config)
    job.result()


def read_reconciled_values(merchant: str, collection: str, config: dict) -> dict[str, dict[str, str]]:
    """Returns identifier -> {field: value} for every field
    configs/<merchant>.json's reconciled_by declares, read directly from the
    merchant's BigQuery table. Structurally the same contract as
    production-v1.1.0/store_adapters.py's function of the same name, so that
    staging_service.py's reconcile_dataframe() (ported unmodified from
    v1.1.0) requires no changes to consume either store's output."""
    id_column = config["reconciled_by"]["id_column"]
    fields = config["reconciled_by"]["fields"]
    client = bigquery.Client(project=config["store"]["project"])
    query = f"SELECT `{id_column}`, {', '.join(f'`{f}`' for f in fields)} FROM `{_table_ref(collection, config)}`"
    rows = client.query(query).result()
    return {
        getattr(row, id_column): {field: getattr(row, field, "") for field in fields}
        for row in rows
    }
