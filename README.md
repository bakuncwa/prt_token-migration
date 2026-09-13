# Token Migration ETL

A PCI-scoped ETL pipeline that migrates card-on-file data from a DigitalOcean-hosted
MIT (Merchant-Initiated Transaction) database into a new token vault, using Google
Cloud Storage as staging and Firestore as a narrow, single-purpose reconciliation
layer for the one field that can't be automated: the new card number (PAN).

## Architecture

![Token Migration ETL diagram](DigitalOcean%20Card%20Token%20Migration%20ETL%20%282%29.png)

```
DigitalOcean MIT DB --{GET}--> GCS raw/ --transform--> GCS transformed/
                                                              |
                                                              v
                                                   Firestore (card.number only)
                                                              |
                                            PaaS worker enters card.number by hand
                                                              |
                                                              v
                                    GCS transformed/ + Firestore --merge--> GCS cleaned/
                                                              |
                                                              v
                                              DigitalOcean MIT DB <--{POST}--
```

Every stage after the initial extract is event-driven: uploading a file to the right
GCS prefix, or writing/exporting a Firestore document, is enough to trigger the next
step automatically via Cloud Run functions (Eventarc). Nothing needs to be run by
hand except entering the reconciled card number itself.

## Why Firestore, and why only `card.number`

The automated transform (raw CSV → transformed CSV) can populate every column in the
new schema except the new card number -- that field is genuinely blank in the source
data and has to be entered by a person. Rather than exposing the full cardholder
record for that manual step, Firestore stores **only** `{"card_number": ...}`, keyed
by `card.id`. There is no other PII or PAN data in Firestore at any point, so there's
nothing else for a reviewer to see or accidentally change -- no read-only lock or
Security Rule is needed to protect fields that were never written there in the first
place. The full reconciled record (all transformed columns + whatever `card.number`
Firestore currently holds) is only ever reassembled at export time, by reading the
transformed CSV fresh from GCS and overlaying Firestore's value per `card.id`.

## Pipeline stages

| # | Stage | Script | Trigger | Reads | Writes |
|---|-------|--------|---------|-------|--------|
| 1 | Extract | `extract_digitalocean.py` | manual / scheduled | DigitalOcean MIT DB (via Kubernetes API) | `raw/<Month Year>.csv` |
| 2 | Transform | `cf_transform_on_upload.py` | GCS finalize on `raw/*.csv` | `raw/*.csv` | `transformed/Transformed_<Month>_<Year>.csv` |
| 3 | Load to Firestore | `cf_load_firestore_on_upload.py` | GCS finalize on `transformed/*.csv` | `transformed/*.csv` | Firestore `MMYYYY_cardholders` (card.number only) |
| 4 | Manual reconciliation | *(Firestore Console)* | human | -- | Firestore `card_number` field, per `card.id` |
| 5 | Export / reconcile | `export_firestore_to_gcs.py` | Firestore Console "Export" click, or any Firestore write | `transformed/*.csv` + Firestore | `cleaned/Reconciled_*.csv` |
| 6 | Sync back | `deploy_digitalocean.py` | GCS finalize on `cleaned/*.csv` | `cleaned/*.csv` | DigitalOcean MIT DB (via Kubernetes API) |

Stage 1 (`extract_digitalocean.py`) and the DigitalOcean-write leg of stage 6
(`push_records_to_digitalocean()` in `deploy_digitalocean.py`) are templates only --
no live DigitalOcean API write/read credentials are provisioned in this environment
yet, so those two legs document the intended request shape and raise clearly instead
of guessing at an endpoint.

Each month's dataset gets its own dated Firestore collection (`MMYYYY_cardholders`,
e.g. `052025_cardholders`), derived from the filename via `collection_naming.py`, so
re-processing an old month never collides with the current one.

## Column mapping

Renames/splits applied by `column_mapping.py` (source of truth:
`Data Transformation_TokenMigration.xlsx`); every other raw column is preserved
as-is.

| Raw column | Transformed column |
|---|---|
| `CustomerName` | `name` |
| `Email` | `Email` *(unchanged)* |
| `RegistrationId` | `id` |
| `UniqueId` | `card.id`, `card.transaction_ids`, `card.number` |
| `Expiry` (`YYYY-MM`) | `card.exp_year`, `card.exp_month` |
| `City` | `card.address_city` |
| `OPP_card.country` | `card.address_country` |
| `OPP_billing.street1` | `card.address_line1` |
| `State` | `card.address_state` |
| `Zip` | `card.address_zip` |

`card.transaction_ids` and `card.number` are emitted blank at transform time (no
source mapping exists for them) rather than fabricated. `card.number` is filled in
later, exclusively via the Firestore reconciliation step above.

## Deployed Cloud Run functions

All deployed as Cloud Run functions (Gen2 Cloud Functions), project
`cardcorp-token-migration`, region `europe-west2`, bucket `cardcorp-token-0dc1f93138`:

| Function | Entry point | Trigger |
|---|---|---|
| `transform-on-raw-upload` | `on_raw_uploaded` | GCS finalize, bucket-wide |
| `load-firestore-on-transformed-upload` | `on_transformed_uploaded` | GCS finalize, bucket-wide |
| `export-on-firestore-export-button` | `on_export_completed` | GCS finalize, bucket-wide (watches for Firestore's export completion marker under `cleaned/`) |
| `export-on-firestore-write` | `on_firestore_write` | Firestore document write, any `MMYYYY_cardholders` collection |
| `sync-paas-reconciled-to-digitalocean` | `on_paas_reconciled` | GCS finalize, bucket-wide (watches `cleaned/*.csv`) |

Each function's exact `gcloud functions deploy` command is documented in its own
script's module docstring.

## Highlights

- Spearheaded engineering modular 4-party payment model ETL pipelines via SFTP file
  transfer client migrations to the Revolut Bank acquirer API gateway.
- Architected Google Cloud Run functions via Eventarc for PCI-compliant card token
  migration, automating merchant-initiated transaction (MIT) PAN data cleaning and
  bidirectional DigitalOcean Kubernetes API synchronization.
- Automated Google Cloud Storage (GCS) bucket-to-staging-layer transformation into
  Firestore NoSQL database on upload for secure reconciliation without PII leakage --
  validated across 2,275 production records spanning 16 monthly cycles with zero
  pipeline errors.

## Local development

Every Cloud Run function has a corresponding manual-run script (`main()`) for local
testing without touching the deployed triggers -- see each file's module docstring
for required/optional environment variables and usage. `test_transform_local.py`
exercises the transform step against a local CSV with no cloud resources at all.

## Data handling

- All CSV I/O reads with `dtype=str, keep_default_na=False` throughout the pipeline,
  to avoid pandas silently coercing types (e.g. a BIN becoming an int and dropping
  leading zeros) or turning blank fields into `NaN`.
- Sensitive data (raw/transformed/cleaned CSVs, spreadsheets, credentials) is
  git-ignored and never committed -- see `.gitignore`.
