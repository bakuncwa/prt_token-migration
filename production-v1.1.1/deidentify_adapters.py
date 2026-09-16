"""De-identification and gated re-identification of Primary Account Number
(PAN) values via Cloud Data Loss Prevention (DLP) and Cloud Key Management
Service (KMS).

production-v1.1.0's extraction_adapters.py avoids PCI DSS Requirement 3 scope
entirely by blanking any PAN-shaped column before a single byte leaves the
source system -- the correct design for a pipeline whose only PAN consumer is
a one-time, human-entered reconciliation field. This module addresses a
structurally different requirement (see DESIGN.md's two-condition test): an
enterprise merchant whose new PAN is supplied automatically by an
authoritative token vault API (vault_reconciliation_adapters.py) rather than
by a human, and whose reconciled dataset must remain queryable (BigQuery) for
more than one downstream consumer. Blanking is not an option when nothing
else in the pipeline will ever repopulate the value from a human source; the
value must instead be rendered safe-at-rest via reversible, deterministic
encryption rather than avoided.

Deterministic encryption (google.cloud.dlp_v2's CryptoDeterministicConfig) is
selected over format-preserving encryption or non-deterministic AES
specifically because the same input PAN must tokenize to the same output
value on every run -- otherwise vault_reconciliation_adapters.py's join
against the token vault's response, keyed by that token, would fail on every
record but the first.

Requisite environment variables (see SETUP.md Step 7):
  GCP_PROJECT            project against which the DLP API is invoked
  DLP_WRAPPED_AES_KEY     base64 AES-256 key, wrapped by the KMS key named in
                          configs/<merchant>.json's deidentify.kms_key
                          (`gcloud kms encrypt`; see SETUP.md)

Neither this module's DLP request shapes nor its KMS key have been verified
against a live GCP project's Cloud DLP/KMS APIs at the time of writing -- the
identical caveat live/extract_digitalocean.py and deploy_digitalocean.py
document for their own unprovisioned DigitalOcean credentials. Verify the
google-cloud-dlp SDK version's exact request field names before the first
live invocation.
"""

from __future__ import annotations

import pandas as pd
from google.cloud import dlp_v2

from merchant_config import env

# Column identifiers a source system's raw export could plausibly employ for
# a full PAN -- identical enumeration to production-v1.1.0/extraction_adapters.py's
# PAN_COLUMN_CANDIDATES, since a merchant's schema does not change based on
# which pipeline version processes it.
PAN_COLUMN_CANDIDATES = ("FullAccountNumber", "CardNumber", "PAN", "card.number")

_SURROGATE_INFO_TYPE = "PAN_TOKEN"


def _crypto_deterministic_config(config: dict) -> dlp_v2.CryptoDeterministicConfig:
    """Builds the deterministic-encryption transformation, keyed by a Cloud
    KMS-wrapped key -- see module docstring for why determinism, rather than
    a non-deterministic cipher, is required here."""
    kms_key_name = config["deidentify"]["kms_key"]
    return dlp_v2.CryptoDeterministicConfig(
        crypto_key=dlp_v2.CryptoKey(
            kms_wrapped=dlp_v2.KmsWrappedCryptoKey(
                wrapped_key=env("DLP_WRAPPED_AES_KEY", required=True).encode(),
                crypto_key_name=kms_key_name,
            )
        ),
        surrogate_info_type=dlp_v2.InfoType(name=_SURROGATE_INFO_TYPE),
    )


def _dataframe_to_table(df: pd.DataFrame) -> dlp_v2.Table:
    headers = [dlp_v2.FieldId(name=col) for col in df.columns]
    rows = [
        dlp_v2.Table.Row(values=[dlp_v2.Value(string_value=str(v)) for v in record])
        for record in df.itertuples(index=False)
    ]
    return dlp_v2.Table(headers=headers, rows=rows)


def _table_to_dataframe(table: dlp_v2.Table) -> pd.DataFrame:
    columns = [field.name for field in table.headers]
    rows = [[value.string_value for value in row.values] for row in table.rows]
    return pd.DataFrame(rows, columns=columns)


def _field_transformations(df: pd.DataFrame, config: dict) -> list[dlp_v2.FieldTransformation]:
    crypto_config = _crypto_deterministic_config(config)
    return [
        dlp_v2.FieldTransformation(
            fields=[dlp_v2.FieldId(name=col)],
            primitive_transformation=dlp_v2.PrimitiveTransformation(
                crypto_deterministic_config=crypto_config
            ),
        )
        for col in PAN_COLUMN_CANDIDATES
        if col in df.columns
    ]


def deidentify_dataframe(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """De-identifies every PAN-shaped column present in df via DLP's
    deterministic-encryption transform. Applied unconditionally within
    extraction_adapters.py's upload_raw_csv() -- this directory's equivalent
    of v1.1.0's _blank_pan_columns(), substituting reversible tokenization
    for blanking because vault_reconciliation_adapters.py requires the
    original value to remain joinable by a stable token."""
    transformations = _field_transformations(df, config)
    if not transformations:
        return df

    project_id = env("GCP_PROJECT", required=True)
    client = dlp_v2.DlpServiceClient()
    response = client.deidentify_content(
        request={
            "parent": f"projects/{project_id}/locations/global",
            "deidentify_config": dlp_v2.DeidentifyConfig(
                record_transformations=dlp_v2.RecordTransformations(field_transformations=transformations)
            ),
            "item": dlp_v2.ContentItem(table=_dataframe_to_table(df)),
        }
    )
    return _table_to_dataframe(response.item.table)


def reidentify_value(token: str, column: str, config: dict) -> str:
    """Reverses deidentify_dataframe()'s transform for a single value,
    invoked exclusively from sink_adapters.py's write-back step, immediately
    before the POST to the merchant's sink system -- see DESIGN.md's
    "Re-identification: gated to the write-back path exclusively" verdict.
    The returned plaintext PAN must never be persisted by the caller: not to
    GCS, not to BigQuery, not to logs."""
    if not token:
        return token

    project_id = env("GCP_PROJECT", required=True)
    client = dlp_v2.DlpServiceClient()
    df = pd.DataFrame({column: [token]})
    response = client.reidentify_content(
        request={
            "parent": f"projects/{project_id}/locations/global",
            "reidentify_config": dlp_v2.DeidentifyConfig(
                record_transformations=dlp_v2.RecordTransformations(
                    field_transformations=_field_transformations(df, config)
                )
            ),
            "item": dlp_v2.ContentItem(table=_dataframe_to_table(df)),
        }
    )
    reidentified = _table_to_dataframe(response.item.table)
    return reidentified.at[0, column]
