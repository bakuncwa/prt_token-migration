# Token Migration ETL

A PCI-scoped ETL pipeline that migrates card-on-file data from a DigitalOcean-hosted
MIT (Merchant-Initiated Transaction) database into a new token vault, using Google
Cloud Storage as staging and Firestore as a narrow, single-purpose reconciliation
layer for the one field that cannot be automated: the new card number (PAN).

## Index

- **I.** **[`live/`](live/)** -- the live, single-merchant pipeline built for
  CardCorp's migration to the Revolut Bank acquirer gateway. Deployed and running
  today.
- **II.** **[`production/`](production/)** -- the generalized, config-driven version
  of the same pipeline, built to onboard any merchant's token migration without
  modifying the pipeline code. See [Production version](#production-version).
- **III.** **[`SETUP.md`](SETUP.md)** -- setup and deployment reference:
  authentication, required IAM roles, environment variables, and the exact `gcloud`
  commands to deploy and debug both pipelines.
- **IV.** [Architecture (live pipeline)](#architecture-live-pipeline)
- **V.** [Firestore scope: why only `card.number`](#firestore-scope-why-only-cardnumber)
- **VI.** [Pipeline stages (live)](#pipeline-stages-live)
- **VII.** [Column mapping (live)](#column-mapping-live)
- **VIII.** [Deployed Cloud Run functions (live)](#deployed-cloud-run-functions-live)
- **IX.** [Production version](#production-version)
- **X.** [Cost comparison](#cost-comparison)
- **XI.** [Key Technical Contributions & Impact](#key-technical-contributions--impact)
- **XII.** [Data handling](#data-handling)

## Architecture (live pipeline)

![Token Migration ETL diagram](DigitalOcean%20Card%20Token%20Migration%20ETL%20%282%29.png)

```
DigitalOcean MIT DB --{GET}--> GCS raw/ --transform--> GCS transformed/
                                                              |
                                                              v
                                                   Firestore (card.number only)
                                                              |
                                        PaaS worker enters card.number manually
                                                              |
                                                              v
                                    GCS transformed/ + Firestore --merge--> GCS cleaned/
                                                              |
                                                              v
                                              DigitalOcean MIT DB <--{POST}--
```

Every stage after the initial extract is event-driven: uploading a file to the
correct GCS prefix, or writing or exporting a Firestore document, triggers the next
step automatically through Cloud Run functions (Eventarc). No stage requires manual
execution except entering the reconciled card number itself.

## Firestore scope: why only `card.number`

The automated transform (raw CSV to transformed CSV) populates every column in the
new schema except the new card number: that field is genuinely blank in the source
data and must be entered by a person. Rather than exposing the full cardholder record
for that manual step, Firestore stores **only** `{"card_number": ...}`, keyed by
`card.id`. Firestore holds no other PII or PAN data at any point, so there is nothing
else for a reviewer to see or modify -- no read-only lock or Security Rule is
required to protect fields that were never written there. The full reconciled record
(every transformed column plus whatever `card.number` Firestore currently holds) is
reassembled only at export time, by reading the transformed CSV fresh from GCS and
overlaying Firestore's value per `card.id`.

## Pipeline stages (live)

| # | Stage | Script | Trigger | Reads | Writes |
|---|-------|--------|---------|-------|--------|
| 1 | Extract | `live/extract_digitalocean.py` | manual / scheduled | DigitalOcean MIT DB (via Kubernetes API) | `raw/<Month Year>.csv` |
| 2 | Transform | `live/cf_transform_on_upload.py` | GCS finalize on `raw/*.csv` | `raw/*.csv` | `transformed/Transformed_<Month>_<Year>.csv` |
| 3 | Load to Firestore | `live/cf_load_firestore_on_upload.py` | GCS finalize on `transformed/*.csv` | `transformed/*.csv` | Firestore `MMYYYY_cardholders` (card.number only) |
| 4 | Manual reconciliation | *(Firestore Console)* | human | -- | Firestore `card_number` field, per `card.id` |
| 5 | Export / reconcile | `live/export_firestore_to_gcs.py` | Firestore Console "Export" click, or any Firestore write | `transformed/*.csv` + Firestore | `cleaned/Reconciled_*.csv` |
| 6 | Sync back | `live/deploy_digitalocean.py` | GCS finalize on `cleaned/*.csv` | `cleaned/*.csv` | DigitalOcean MIT DB (via Kubernetes API) |

Stage 1 (`extract_digitalocean.py`) and the DigitalOcean-write leg of stage 6
(`push_records_to_digitalocean()` in `deploy_digitalocean.py`) are templates only:
no live DigitalOcean API write or read credentials are provisioned in this
environment, so both legs document the intended request shape and raise explicitly
rather than assuming an endpoint.

Each month's dataset gets its own dated Firestore collection (`MMYYYY_cardholders`,
for example `052025_cardholders`), derived from the filename by
`collection_naming.py`, so reprocessing an earlier month does not collide with the
current one.

## Column mapping (live)

Renames and splits applied by `live/column_mapping.py` (source of truth:
`Data Transformation_TokenMigration.xlsx`); every other raw column is preserved
unchanged.

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
source mapping exists for them) rather than fabricated. `card.number` is populated
later, exclusively through the Firestore reconciliation step described above.

## Deployed Cloud Run functions (live)

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
script's module docstring; a placeholder-driven, deployment-agnostic version of the
same commands is in [`SETUP.md`](SETUP.md).

## Production version

`production/` implements the same read, transform, reconcile, write shape as the
pipeline above, but the merchant, the mapping rules, the extraction source, and the
sink system are all configuration, not code.

![Production Token Migration ETL diagram](Production%20Token%20Migration%20ETL.png)

**Invariant components:** the staging logic itself -- read a source, apply a
mapping, write a destination -- is the same shape as `live/column_mapping.py` and
`live/export_firestore_to_gcs.py`'s merge step, now driven by
`production/configs/<merchant>.json` instead of hardcoded rename dictionaries.
Firestore's role is unchanged: still exactly one field, still edited through its own
Console, now namespaced by merchant as well as by month
(`<merchant>_MMYYYY_cardholders`).

**PCI scoping extends to the extraction boundary:** in production, a source system
such as DigitalOcean's MIT database is expected to return the PAN column already
blank, the same assumption `live/column_mapping.py` documents for CardCorp's raw
export. `production/extraction_adapters.py` does not trust that assumption silently
-- `upload_raw_csv()` blanks any PAN-shaped column (`FullAccountNumber`,
`CardNumber`, `PAN`, `card.number`) unconditionally before a byte reaches GCS,
regardless of which extraction adapter fetched the data.

**Configurable components, per merchant:**

| Piece | Default | Opt-in alternative |
|---|---|---|
| Extraction | scheduled Cloud Run puller | Dataflow (for merchants whose volume requires it) |
| Mapping authoring | Gemini agent proposes `configs/<merchant>.json` from sample data, human approves | -- |
| Reconciliation store | Firestore, one field per document | SQL (for merchants requiring joins or reporting) |
| Sink | pluggable adapter, for example DigitalOcean Kubernetes for Payreto | any merchant's own target system |

Full reasoning behind each default: [`production/DESIGN.md`](production/DESIGN.md).

**Outstanding implementation decisions,** tracked as structured data rather than
left implicit: see [`production/open_decisions.py`](production/open_decisions.py).
Date edited: 2026-09-13.

1. **Gemini review gate** -- unresolved. `production/gemini_mapping_agent.py`
   proposes a mapping config but deliberately refuses to write it anywhere until a
   review gate exists; it raises rather than trusting an unreviewed proposal.
2. **DigitalOcean access for Payreto** -- unresolved. Both `live/`'s and
   `production/`'s DigitalOcean legs (extraction and sink) are placeholders; neither
   pipeline's GCS/Firestore side is blocked by this.
3. **Test strategy** -- resolved. See below.

### Test Results

`production/test_staging_service.py` runs four checks, two of them independent of
any cloud credential:

1. `production/staging_service.py`'s config-driven `transform_dataframe()`
   reproduces `live/column_mapping.py`'s hardcoded `transform_dataframe()`
   byte-for-byte, on the real CardCorp `raw/January 2026.csv`. Requires live GCP
   credentials.
2. `transform_dataframe()` against `production/configs/samplepay.json`: a synthetic
   merchant with a structurally different schema from CardCorp's -- different column
   names, a different `Expiry` date format (`MM/YYYY` instead of CardCorp's
   `YYYY-MM`), and a different zero-padding repair length. Runs entirely locally
   against `production/sample_data/samplepay_raw.csv`, with no cloud dependency.
   This check caught a real defect before it shipped: `_split_date()` declared
   `source_format` in the config schema but ignored it, hardcoded to CardCorp's
   date shape. Fixed in `staging_service.py`; the function now parses per the
   declared format.
3. `reconcile_dataframe()` reproduces `live/export_firestore_to_gcs.py`'s
   `merge_card_numbers()` byte-for-byte, reading the real, live
   `012026_cardholders` Firestore collection -- confirming that when a card number
   is reconciled in Firestore, the production staging layer's transformation still
   works, running through the generalized Cloud Run script rather than the
   CardCorp-only live pipeline. Requires live GCP credentials.
4. `run_transform()` and `run_reconcile()` run end to end against a separate test
   bucket (`cardcorp-token-migration-production-test`), seeded from the same source
   data the live pipeline uses, for direct comparison. Requires live GCP
   credentials.

## Cost comparison

For adoption decisions evaluated on a per-merchant basis, the pluggable design of
`production/` ties operating cost to the adapters a given merchant selects, not to
the codebase itself. Every merchant on the default path (Cloud Run puller,
Firestore, no Dataflow, no SQL) incurs approximately the same cost as the live
pipeline does today. Dataflow and Cloud SQL are the two line items that shift the
cost basis from near-zero to a material recurring monthly expense, and both are
opt-in per merchant rather than pipeline-wide.

The live pipeline's actual measured volume -- 2,275 records across 16 monthly
Firestore collections, a few dozen function invocations per month, well under
100 MB of total CSV storage -- is used below as the baseline, rather than an assumed
volume.

| Cost driver | Live (CardCorp, measured) | Production, default path (per merchant, same volume) | Production, opted into Dataflow + Cloud SQL (per merchant) |
|---|---|---|---|
| Compute (Cloud Run functions) | \$0.00 -- a few dozen invocations/month against a free tier of 2,000,000 requests, 180,000 vCPU-seconds, and 360,000 GiB-seconds/month | \$0.00 -- free tier is project-wide, not per merchant; a few dozen invocations per merchant per month stays far under the shared limit through dozens of merchants | Same as default path |
| Reconciliation store | \$0.00 -- 2,275 documents (tens of KB) against a daily free tier of 1 GiB stored, 50,000 reads, 20,000 writes, 20,000 deletes | \$0.00 at comparable per-merchant volume -- roughly 20+ merchants could each fully re-read their dataset on the same day before the shared daily read quota is touched | + Cloud SQL: no free tier. Smallest shared-core instance (`db-f1-micro`-equivalent) runs approximately \$7-\$10/month in compute alone, before storage, backup, and network -- a flat cost per opted-in merchant regardless of usage |
| Extraction | included above | included above | + Dataflow: no free tier. One small worker (1 vCPU, 3.75 GiB) for a 10-minute monthly batch run costs roughly \$0.056/vCPU-hour x 0.167 hour + \$0.003557/GiB-hour x 3.75 GiB x 0.167 hour, approximately \$0.01-\$0.02 per run -- the compute cost itself is negligible; the material cost is operating a second execution substrate at all |
| Mapping authoring | manual, developer time only | Gemini agent, flash-tier pricing (\$0.30/1M input tokens, \$2.50/1M output tokens): a single onboarding call of roughly 5,000 input and 1,000 output tokens costs about \$0.004, run once per merchant onboarding | Same |
| CI/CD | manual `gcloud functions deploy` | Cloud Build, within the 120 free build-minutes/day (e2-standard-2) at this deploy frequency | Same |
| Storage (GCS) | approximately \$0.002/month (well under 100 MB at \$0.020/GB/month) | scales linearly with merchant count at the same per-merchant rate | Same |
| **Estimated monthly total** | **approximately \$0.00** | **approximately \$0.00 per merchant**, until dozens of merchants are onboarded | **approximately \$7-\$10/month per merchant on Cloud SQL**, plus a few cents per merchant per month on Dataflow |

**Recommendation for adoption:** default every new merchant to the low-cost path;
approve Dataflow or Cloud SQL for a specific merchant only when volume or downstream
reporting genuinely requires it, matching `production/DESIGN.md`'s verdicts. This
keeps the marginal cost of onboarding merchant *N+1* close to zero unless that
merchant specifically needs the higher-cost adapters.

Figures above use published Google Cloud list pricing at time of writing, applied to
this project's own measured usage, not a vendor quote. Actual costs depend on
region, committed-use discounts, and real traffic; verify with the
[GCP Pricing Calculator](https://cloud.google.com/products/calculator) before
committing budget.

Sources: [Cloud Run pricing](https://cloud.google.com/run/pricing),
[Firestore pricing](https://cloud.google.com/firestore/pricing),
[Cloud Storage pricing](https://cloud.google.com/storage/pricing),
[Dataflow pricing](https://cloud.google.com/dataflow/pricing),
[Cloud SQL pricing](https://cloud.google.com/sql/pricing),
[Cloud Build pricing](https://cloud.google.com/build/pricing),
[Gemini API pricing](https://ai.google.dev/gemini-api/docs/pricing).

## Key Technical Contributions & Impact

- Architected Google Cloud Run functions via Eventarc for PCI-compliant card token
  migration, automating merchant-initiated transaction (MIT) PAN data cleaning and
  bidirectional DigitalOcean Kubernetes API synchronization.
- Spearheaded engineering modular 4-party payment model ETL pipelines via SFTP file
  transfer client migrations to the Revolut Bank acquirer API gateway.
- Generalized a single-merchant pipeline into a config-driven, multi-merchant
  "token migration as a service" architecture with pluggable extraction, staging,
  and sink adapters, validated against both the live pipeline's real production
  data and a structurally different synthetic merchant schema.

![Key technical contribution metrics](Key%20Technical%20Contributions%20Stats.png)

Each metric above is a standalone count, not a share of a common denominator;
accordingly, each is rendered as its own single-category pie (an unfilled,
full-circle wedge) rather than combined into one proportional pie, which would
misrepresent five incommensurable quantities as parts of a single whole.

## Data handling

- All CSV I/O reads with `dtype=str, keep_default_na=False` throughout both
  pipelines, to prevent pandas from silently coercing types (for example, a BIN
  becoming an integer and dropping leading zeros) or converting blank fields to
  `NaN`.
- Sensitive data (raw, transformed, and cleaned CSVs, spreadsheets, credentials) is
  git-ignored and never committed; see `.gitignore`. The one exception is
  `production/sample_data/`, which holds only synthetic, non-real test fixture data.
