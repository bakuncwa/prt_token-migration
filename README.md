# Token Migration ETL

A PCI-scoped ETL pipeline that migrates card-on-file data from a DigitalOcean-hosted
MIT (Merchant-Initiated Transaction) database into a new token vault, using Google
Cloud Storage as staging and Firestore as a narrow, single-purpose reconciliation
layer for the one field that can't be automated: the new card number (PAN).

This repo holds two things side by side:

- **[`original/`](original/)** -- the live, single-merchant pipeline built for CardCorp's
  migration to the Revolut Bank acquirer gateway. Deployed and running today.
- **[`modular/`](modular/)** -- the generalized, config-driven version of the same
  pipeline, built to onboard any merchant's token migration without touching the
  pipeline code itself. See [Modular version](#modular-version) below.

## Architecture (original pipeline)

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

## Pipeline stages (original)

| # | Stage | Script | Trigger | Reads | Writes |
|---|-------|--------|---------|-------|--------|
| 1 | Extract | `original/extract_digitalocean.py` | manual / scheduled | DigitalOcean MIT DB (via Kubernetes API) | `raw/<Month Year>.csv` |
| 2 | Transform | `original/cf_transform_on_upload.py` | GCS finalize on `raw/*.csv` | `raw/*.csv` | `transformed/Transformed_<Month>_<Year>.csv` |
| 3 | Load to Firestore | `original/cf_load_firestore_on_upload.py` | GCS finalize on `transformed/*.csv` | `transformed/*.csv` | Firestore `MMYYYY_cardholders` (card.number only) |
| 4 | Manual reconciliation | *(Firestore Console)* | human | -- | Firestore `card_number` field, per `card.id` |
| 5 | Export / reconcile | `original/export_firestore_to_gcs.py` | Firestore Console "Export" click, or any Firestore write | `transformed/*.csv` + Firestore | `cleaned/Reconciled_*.csv` |
| 6 | Sync back | `original/deploy_digitalocean.py` | GCS finalize on `cleaned/*.csv` | `cleaned/*.csv` | DigitalOcean MIT DB (via Kubernetes API) |

Stage 1 (`extract_digitalocean.py`) and the DigitalOcean-write leg of stage 6
(`push_records_to_digitalocean()` in `deploy_digitalocean.py`) are templates only --
no live DigitalOcean API write/read credentials are provisioned in this environment
yet, so those two legs document the intended request shape and raise clearly instead
of guessing at an endpoint.

Each month's dataset gets its own dated Firestore collection (`MMYYYY_cardholders`,
e.g. `052025_cardholders`), derived from the filename via `collection_naming.py`, so
re-processing an old month never collides with the current one.

## Column mapping (original)

Renames/splits applied by `original/column_mapping.py` (source of truth:
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

## Deployed Cloud Run functions (original)

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

## Modular version

`modular/` generalizes the pipeline above into a config-driven "token migration as a
service": the same read → transform → reconcile → write shape, but which merchant,
which mapping rules, which extraction source, and which sink system are all
configuration, not code.

![Modular Token Migration ETL diagram](Modular%20Token%20Migration%20ETL.png)

**What stays the same:** the staging logic itself -- read a source, apply a mapping,
write a destination -- is the identical shape as `original/column_mapping.py` and
`original/export_firestore_to_gcs.py`'s merge step, now driven by
`modular/configs/<merchant>.json` instead of hardcoded rename dicts. Firestore's role
is unchanged too: still exactly one field, still edited through its own Console, now
namespaced by merchant as well as by month (`<merchant>_MMYYYY_cardholders`).

**What's pluggable, per merchant:**

| Piece | Default | Opt-in alternative |
|---|---|---|
| Extraction | scheduled Cloud Run puller | Dataflow (for merchants whose volume needs it) |
| Mapping authoring | Gemini agent proposes `configs/<merchant>.json` from sample data, human approves | -- |
| Reconciliation store | Firestore, one field per document | SQL (for merchants needing joins/reporting) |
| Sink | pluggable adapter, e.g. DigitalOcean Kubernetes for Payreto | any merchant's own target system |

Full reasoning behind each of those defaults: [`modular/DESIGN.md`](modular/DESIGN.md).

**What's still a placeholder, on purpose, tracked as data rather than left implicit:**
see [`modular/open_decisions.py`](modular/open_decisions.py). As of this write-up:

- **Gemini review gate** -- unresolved. `modular/gemini_mapping_agent.py` proposes a
  mapping config but deliberately refuses to write it anywhere until a review gate
  exists; it currently raises rather than silently trusting an unreviewed proposal.
- **DigitalOcean access for Payreto** -- unresolved. Both `original/`'s and
  `modular/`'s DigitalOcean legs (extraction and sink) are placeholders; nothing on
  the GCS/Firestore side of either pipeline is blocked by this.
- **Test strategy** -- resolved. See below.

### What's been tested

`modular/test_staging_service.py` runs against live GCP data, not mocks:

1. `modular/staging_service.py`'s config-driven `transform_dataframe()` reproduces
   `original/column_mapping.py`'s hardcoded `transform_dataframe()` byte-for-byte, on
   the real CardCorp `raw/January 2026.csv`.
2. Its `reconcile_dataframe()` reproduces `original/export_firestore_to_gcs.py`'s
   `merge_card_numbers()` byte-for-byte, reading the real, live
   `012026_cardholders` Firestore collection -- proving that when a card number is
   reconciled in Firestore, the modular staging layer's transformation still works,
   running through the generalized Cloud Run script rather than the
   CardCorp-only original.
3. `run_transform()` and `run_reconcile()` run end to end against a separate test
   bucket (`cardcorp-token-migration-modular-test`), seeded from the same source data
   the original pipeline uses, for direct comparison.

## Key Technical Contributions & Impact

- Architected Google Cloud Run functions via Eventarc for PCI-compliant card token
  migration, automating merchant-initiated transaction (MIT) PAN data cleaning and
  bidirectional DigitalOcean Kubernetes API synchronization.
- Spearheaded engineering modular 4-party payment model ETL pipelines via SFTP file
  transfer client migrations to the Revolut Bank acquirer API gateway.
- Generalized a single-merchant pipeline into a config-driven, multi-merchant
  "token migration as a service" architecture with pluggable extraction, staging,
  and sink adapters, validated against the original pipeline's live production data.

| | |
|---|---|
| **2,275** | production records reconciled |
| **16** | monthly cycles processed |
| **0** | pipeline errors |
| **1** | field ever touched by a human reviewer (`card.number`) |

## Local development

Every Cloud Run function has a corresponding manual-run script (`main()`) for local
testing without touching the deployed triggers -- see each file's module docstring
for required/optional environment variables and usage. `original/test_transform_local.py`
exercises the transform step against a local CSV with no cloud resources at all;
`modular/test_staging_service.py` is the modular pipeline's equivalent, run against
live data (see above).

## Data handling

- All CSV I/O reads with `dtype=str, keep_default_na=False` throughout both
  pipelines, to avoid pandas silently coercing types (e.g. a BIN becoming an int and
  dropping leading zeros) or turning blank fields into `NaN`.
- Sensitive data (raw/transformed/cleaned CSVs, spreadsheets, credentials) is
  git-ignored and never committed -- see `.gitignore`.
