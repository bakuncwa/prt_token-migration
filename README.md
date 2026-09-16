# Card Token Migration Pipeline for PCI DSS-Compliant MIT Data & Reconciliation

This repository documents a Payment Card Industry Data Security Standard
(PCI DSS)¹-scoped Extract, Transform, Load (ETL) pipeline engineered to
effect the migration of card-on-file data from a DigitalOcean-hosted Merchant
Initiated Transaction (MIT) database to a newly provisioned token vault. The
architecture employs Google Cloud Storage as an intermediate staging layer and
Google Cloud Firestore as a narrowly scoped, single-purpose reconciliation
layer, the latter reserved exclusively for the one field that cannot be
populated through automation: the newly issued card number, i.e., the Primary
Account Number (PAN).

## Index

- **I.** **[`live/`](live/)** -- the production-deployed, single-merchant
  implementation engineered for a card token migration to the Revolut Bank
  acquirer gateway. Currently deployed and operational.
- **II.** **[`production-v1.1.0/`](production-v1.1.0/)** -- a generalized, configuration-driven
  extension of the same pipeline, engineered to facilitate the onboarding of
  additional merchants' token migrations without modification of the underlying
  pipeline code. See [Production version (v1.1.0)](#production-version-v110-blank-and-manual).
- **III.** **[`production-v1.1.1/`](production-v1.1.1/)** -- a de-identification/
  re-identification extension of the same pipeline, engineered for a
  structurally different requirement: enterprise-scale, fully automated
  reconciliation against an authoritative token vault API, with the reconciled
  dataset retained and queryable. Deployed for scalability demonstration only
  -- no merchant is onboarded onto it. See [Production version
  (v1.1.1)](#production-version-v111-de-identification-and-re-identification)
  and [`production-v1.1.1/DESIGN.md`](production-v1.1.1/DESIGN.md).
- **IV.** **[`SETUP.md`](SETUP.md)** -- the setup and deployment reference,
  comprising authentication procedures, requisite IAM roles, environment
  variables, and the precise `gcloud` commands required to deploy and diagnose
  all three implementations.
- **V.** [Technology stack](#technology-stack)
- **VI.** [Architecture (live pipeline)](#architecture-live-pipeline)
- **VII.** [Firestore scope: rationale for restriction to `card.number`](#firestore-scope-rationale-for-restriction-to-cardnumber)
- **VIII.** [Pipeline stages (live)](#pipeline-stages-live)
- **IX.** [Column mapping (live)](#column-mapping-live)
- **X.** [Deployed Cloud Run functions (live)](#deployed-cloud-run-functions-live)
- **XI.** [Production version (v1.1.0)](#production-version-v110-blank-and-manual)
- **XII.** [Production version (v1.1.1)](#production-version-v111-de-identification-and-re-identification)
- **XIII.** [Cost comparison](#cost-comparison)
- **XIV.** [Key Technical Contributions & Impact](#key-technical-contributions--impact)
- **XV.** [References](#references)

## Technology stack

| Layer | Technology | Role within this repository |
|---|---|---|
| Compute | Google Cloud Run functions (Generation 2), Eventarc | Every pipeline stage subsequent to extraction; triggered by GCS finalize and Firestore write events, in `live/` and `production-v1.1.0/`; Pub/Sub events in `production-v1.1.1/` |
| Staging storage | Google Cloud Storage (GCS) | `raw/`, `transformed/`, and `cleaned/` prefixes -- the intermediate staging layer between the source MIT database and the token vault |
| Reconciliation store | Google Cloud Firestore | Narrowly scoped, single-field (`card.number`) manual reconciliation layer, namespaced per month (live) or per merchant and month (`production-v1.1.0/`) |
| Reconciliation store (v1.1.1 only) | Google BigQuery | Queryable, de-identified reconciliation table, namespaced per merchant and month; see [Production version (v1.1.1)](#production-version-v111-de-identification-and-re-identification) |
| De-identification / re-identification (v1.1.1 only) | Google Cloud Data Loss Prevention (DLP), Cloud Key Management Service (KMS) | Deterministic-encryption tokenization of PAN values, keyed by a KMS-wrapped crypto key; reversed exclusively at the write-back step, never exposed to BigQuery or a human -- see [`production-v1.1.1/deidentify_adapters.py`](production-v1.1.1/deidentify_adapters.py) |
| Event backbone (v1.1.1 only) | Google Cloud Pub/Sub | Decouples automated vault reconciliation from write-back synchronization, in place of v1.1.0's Firestore-write trigger |
| Source / sink system | DigitalOcean Kubernetes API | The DigitalOcean-hosted Merchant Initiated Transaction (MIT) database, both read (extraction) and written (post-reconciliation synchronization) |
| Extraction (optional, high-volume) | Google Cloud Dataflow (Apache Beam) | Pluggable `extraction_adapters.py` adapter, selected per merchant in place of the default scheduled Cloud Run puller; the expected default for any merchant genuinely onboarded onto `production-v1.1.1/` |
| Mapping-config authoring | Google Gemini API | `production-v1.1.0/gemini_mapping_agent.py` proposes `configs/<merchant>.json` from representative sample data, subject to human review |
| CI/CD | Google Cloud Build | Automated deployment of Cloud Run functions on push, within the free build-minutes tier |
| Language / runtime | Python 3.12, pandas, Functions Framework | Shared implementation language across every stage of the live and both production pipelines |
| Tooling | Cloud Shell, `gcloud` CLI, `doctl`, `kubectl` | Deployment, diagnostics, and DigitalOcean Kubernetes cluster operations (see [`SETUP.md`](SETUP.md)) |

## Architecture (live pipeline)

![Token Migration ETL diagram](DigitalOcean%20Card%20Token%20Migration%20ETL%20%282%29.png)

```
DigitalOcean MIT DB --{GET}--> GCS raw/ --transform--> GCS transformed/
                                                              |
                                                              v
                                                   Firestore (card.number only)
                                                              |
                              PaaS (Payments as a Service) worker enters card.number manually
                                                              |
                                                              v
                                    GCS transformed/ + Firestore --merge--> GCS cleaned/
                                                              |
                                                              v
                                              DigitalOcean MIT DB <--{POST}--
```

Every stage subsequent to the initial extraction is event-driven: the act of
uploading a file to the appropriate Google Cloud Storage (GCS) prefix, or of
writing to or exporting a Firestore document, is sufficient to trigger the
subsequent stage automatically, by means of Cloud Run functions (Eventarc). No
stage necessitates manual execution, with the sole exception of the manual entry
of the reconciled card number.

## Firestore scope: rationale for restriction to `card.number`

The automated transformation procedure (raw CSV to transformed CSV) populates
every column within the new schema with the exception of the new card number:
this field is, by design, blank in the source data and must therefore be entered
by a human operator. Rather than exposing the complete cardholder record for the
purpose of this manual procedure, Firestore is architected to store **exclusively**
`{"card_number": ...}`, indexed by `card.id`. Firestore contains no other
personally identifiable information (PII) or PAN data at any point in the
pipeline's operation; consequently, no additional attribute is exposed to a human
reviewer, obviating the requirement for a read-only lock or Security Rule to
protect fields that are, by construction, never written to this store. The
complete reconciled record -- comprising every transformed column together with
whatever value `card.number` currently holds in Firestore -- is reassembled
exclusively at export time, through a process that reads the transformed CSV
afresh from GCS and overlays Firestore's value, indexed by `card.id`.

## Pipeline stages (live)

| # | Stage | Script | Trigger | Reads | Writes |
|---|-------|--------|---------|-------|--------|
| 1 | Extraction | `live/extract_digitalocean.py` | manual / scheduled | DigitalOcean MIT DB (via Kubernetes API) | `raw/<Month Year>.csv` |
| 2 | Transformation | `live/cf_transform_on_upload.py` | GCS finalize on `raw/*.csv` | `raw/*.csv` | `transformed/Transformed_<Month>_<Year>.csv` |
| 3 | Load to Firestore | `live/cf_load_firestore_on_upload.py` | GCS finalize on `transformed/*.csv` | `transformed/*.csv` | Firestore `MMYYYY_cardholders` (card.number only) |
| 4 | Manual reconciliation | *(Firestore Console)* | human operator | -- | Firestore `card_number` field, indexed by `card.id` |
| 5 | Export / reconciliation | `live/export_firestore_to_gcs.py` | Firestore Console "Export" invocation, or any Firestore write operation | `transformed/*.csv` + Firestore | `cleaned/Reconciled_*.csv` |
| 6 | Synchronization | `live/deploy_digitalocean.py` | GCS finalize on `cleaned/*.csv` | `cleaned/*.csv` | DigitalOcean MIT DB (via Kubernetes API) |

Stage 1 (`extract_digitalocean.py`) and the DigitalOcean-write component of Stage
6 (`push_records_to_digitalocean()`, within `deploy_digitalocean.py`) constitute
templates exclusively: no live DigitalOcean API write or read credentials have
been provisioned within this environment. Accordingly, both components document
the intended request specification and raise an explicit exception rather than
presuming the existence of an endpoint.

Each monthly dataset is assigned its own dated Firestore collection
(`MMYYYY_cardholders`; for example, `052025_cardholders`), derived from the
filename through `collection_naming.py`, such that the reprocessing of a prior
month does not produce a collision with the current month's collection.

## Column mapping (live)

Renaming and splitting operations are applied by `live/column_mapping.py`
(authoritative source: `Data Transformation_TokenMigration.xlsx`); every
remaining raw column is preserved without modification.

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

`card.transaction_ids` and `card.number` are emitted as blank fields at
transformation time, as no corresponding source mapping exists for either;
neither value is fabricated. `card.number` is subsequently populated exclusively
through the Firestore reconciliation procedure described above.

## Deployed Cloud Run functions (live)

All functions are deployed as Cloud Run functions (Generation 2 Cloud Functions),
under project `cardcorp-token-migration`, region `europe-west2`, bucket
`<BUCKET_NAME>`:

| Function | Entry point | Trigger |
|---|---|---|
| `transform-on-raw-upload` | `on_raw_uploaded` | GCS finalize, bucket-wide |
| `load-firestore-on-transformed-upload` | `on_transformed_uploaded` | GCS finalize, bucket-wide |
| `export-on-firestore-export-button` | `on_export_completed` | GCS finalize, bucket-wide (monitors for Firestore's export completion marker under `cleaned/`) |
| `export-on-firestore-write` | `on_firestore_write` | Firestore document write operation, applicable to any `MMYYYY_cardholders` collection |
| `sync-paas-reconciled-to-digitalocean` | `on_paas_reconciled` | GCS finalize, bucket-wide (monitors `cleaned/*.csv`) |
| `replicate-raw-to-production` | `on_raw_uploaded_replicate` | GCS finalize, bucket-wide (monitors `raw/*.csv`); mirrors each object into the production pipeline's dedicated bucket, under `raw/pilot/`, so the reference merchant continues to exercise `production-v1.1.0/`'s generalized staging service against authentic data |

The precise `gcloud functions deploy` command for each function is documented
within that function's own module docstring; a placeholder-parameterized,
deployment-agnostic formulation of the equivalent commands is provided in
[`SETUP.md`](SETUP.md).

## Production version (v1.1.0: blank-and-manual)

`production-v1.1.0/` implements a read, transform, reconcile, write architecture
structurally identical to that of the pipeline described above; however, the
merchant identity, mapping rules, extraction source, and sink system are
rendered as configuration rather than as code.

![Production Token Migration ETL diagram](Production%20Token%20Migration%20ETL.png)

**Invariant components:** the staging logic proper -- the reading of a source,
the application of a mapping, and the writing of a destination -- is
structurally identical to that of `live/column_mapping.py` and the merge
procedure within `live/export_firestore_to_gcs.py`, and is driven instead by
`production-v1.1.0/configs/<merchant>.json` in place of hardcoded rename dictionaries.
Firestore's function within the architecture remains unaltered: it retains
responsibility for exactly one field, remains editable exclusively through its
own Console interface, and is now namespaced by merchant in addition to month
(`<merchant>_MMYYYY_cardholders`).

**Deployment triggers mirror the live pipeline exactly:** `production-v1.1.0/` is
deployed as its own set of Cloud Run functions, each triggered by the
identical event class as its live-pipeline counterpart -- a GCS finalize event
upon upload to `raw/`, a Firestore document-write event upon manual
reconciliation, and a GCS finalize event upon the resulting write to
`cleaned/`, which in turn automates the synchronization of the reconciled
dataset back to DigitalOcean, generalizing the live pipeline's
`sync-paas-reconciled-to-digitalocean`:

| Function | Entry point | Trigger |
|---|---|---|
| `staging-on-raw-upload` | `on_raw_uploaded` | GCS finalize, bucket-wide (monitors `raw/<merchant>/*.csv`) |
| `staging-on-firestore-write` | `on_firestore_write` | Firestore document write operation, applicable to any `<merchant>_MMYYYY_<prefix>` collection |
| `staging-on-cleaned-upload` | `on_cleaned_uploaded` | GCS finalize, bucket-wide (monitors `cleaned/<merchant>/*.csv`); dispatches to the merchant's configured `sink_adapter`, e.g. DigitalOcean Kubernetes |

`production-v1.1.0/` is deployed against its own dedicated GCS bucket, distinct from
the live pipeline's, so that its merchant-namespaced `raw/<merchant>/` layout
can never collide with the live bucket's flat, bucket-wide `raw/` trigger.
The reference merchant (`configs/pilot.json`) is kept populated with authentic
data via `replicate-raw-to-production` (see [Deployed Cloud Run functions
(live)](#deployed-cloud-run-functions-live)), rather than by writing into the
live bucket's own `raw/` prefix.

See [`production-v1.1.0/sink_adapters.py`](production-v1.1.0/sink_adapters.py) for
`on_cleaned_uploaded`'s implementation and
[`SETUP.md`](SETUP.md#6-deployment-procedure-production-pipeline) for the
exact `gcloud functions deploy` commands.

**PCI scoping is extended to the extraction boundary:** within a production
deployment, a source system such as DigitalOcean's MIT database is expected to
return the PAN column in an already-blanked state, an assumption consistent with
that documented by `live/column_mapping.py` for CardCorp's raw export.
`production-v1.1.0/extraction_adapters.py` does not accept this assumption without
independent verification: `upload_raw_csv()` unconditionally blanks any
PAN-shaped column (`FullAccountNumber`, `CardNumber`, `PAN`, `card.number`) prior
to the transmission of any byte to GCS, irrespective of which extraction adapter
retrieved the underlying data.

**Configurable components, per merchant:**

| Component | Default | Optional alternative |
|---|---|---|
| Extraction | scheduled Cloud Run puller | Dataflow (for merchants whose data volume necessitates it) |
| Mapping authorship | Gemini agent proposes `configs/<merchant>.json` from representative sample data, subject to human approval | -- |
| Reconciliation store | Firestore, one field per document | -- |
| Sink | pluggable adapter, for example DigitalOcean Kubernetes for Payreto² | any merchant-specific target system |

The complete rationale underlying each default is documented in
[`production-v1.1.0/DESIGN.md`](production-v1.1.0/DESIGN.md).

**Outstanding implementation decisions,** maintained as structured data rather
than as implicit assumption: see
[`production-v1.1.0/open_decisions.py`](production-v1.1.0/open_decisions.py). Date edited:
2026-09-13.

1. **Gemini review gate** -- unresolved. `production-v1.1.0/gemini_mapping_agent.py`
   proposes a mapping configuration but is deliberately constrained from
   persisting it under any circumstance until a formal review gate has been
   established; the module raises an exception rather than presuming an
   unreviewed proposal to be trustworthy.
2. **DigitalOcean credentials for Payreto** -- unresolved. `production-v1.1.0/`'s
   extraction and sink adapters call the DigitalOcean Kubernetes API directly
   and require `<MERCHANT>_SOURCE_TOKEN`, `<MERCHANT>_SOURCE_CLUSTER`,
   `DIGITALOCEAN_TOKEN`, and `<MERCHANT>_DO_CLUSTER_NAME` to be present in the
   deployment environment; `live/`'s equivalent components remain
   unimplemented placeholders. Neither pipeline's GCS/Firestore-facing
   components are obstructed by this limitation.
3. **Test strategy** -- resolved. See the following subsection.

### Test Results

`production-v1.1.0/test_staging_service.py` executes four verification procedures, two
of which are independent of any cloud credential:

1. `production-v1.1.0/staging_service.py`'s configuration-driven `transform_dataframe()`
   reproduces `live/column_mapping.py`'s hardcoded `transform_dataframe()`
   output byte-for-byte, evaluated against the authentic CardCorp
   `raw/January 2026.csv`. Requires live Google Cloud Platform (GCP) credentials.
2. `transform_dataframe()` is evaluated against
   `production-v1.1.0/configs/samplepay.json`: a synthetic merchant possessing a
   structurally distinct schema relative to CardCorp's -- differing column
   names, a differing `Expiry` date format (`MM/YYYY` in place of CardCorp's
   `YYYY-MM`), and a differing zero-padding repair length. This procedure
   executes entirely locally, against `production-v1.1.0/sample_data/samplepay_raw.csv`,
   with no cloud dependency whatsoever. This verification procedure identified a
   genuine defect prior to deployment: `_split_date()` declared `source_format`
   within the configuration schema yet disregarded it, remaining hardcoded to
   CardCorp's date format. The defect has since been remediated within
   `staging_service.py`; the function now parses input according to the
   declared format.
3. `reconcile_dataframe()` reproduces `live/export_firestore_to_gcs.py`'s
   `merge_card_numbers()` output byte-for-byte, evaluated against the
   authentic, live `012026_cardholders` Firestore collection -- thereby
   confirming that, subsequent to the reconciliation of a card number within
   Firestore, the production staging layer's transformation procedure continues
   to function correctly when executed through the generalized Cloud Run
   script, as distinct from the CardCorp-specific live pipeline. Requires live
   GCP credentials.
4. `run_transform()` and `run_reconcile()` are executed end to end against an
   independent test bucket (`cardcorp-token-migration-production-test`),
   populated from the identical source data employed by the live pipeline, to
   permit direct comparison. Requires live GCP credentials.

## Production version (v1.1.1: de-identification and re-identification)

`production-v1.1.1/` addresses a structurally different requirement from
either pipeline above: an enterprise merchant whose reconciliation must be
**fully automated** (no human enters a card number) and whose reconciled PAN
must remain **queryable** by more than one downstream consumer. v1.1.0's
blank-and-manual design -- never storing a PAN, and bounding the one
unavoidable manual field to a single human, a single collection, a single
month -- has no equivalent here: there is no human step left to bound
exposure to, and BigQuery, unlike Firestore's single-field document, is
inherently a queryable store. This directory answers that requirement by
protecting the PAN cryptographically instead of avoiding storing it,
following Google Cloud's reference architecture for de-identification and
re-identification of PII using Cloud DLP³:

![De-identification/re-identification Token Migration ETL diagram](De-identification%20Re-identification%20Token%20Migration%20ETL.svg)

```
DigitalOcean MIT DB --{GET, Dataflow}--> GCS raw/ (PAN, DLP-tokenized via KMS key)
                                                              |
                                                              v
                                         transformed/ --load--> BigQuery (queryable, tokenized)
                                                              |
                                                              v
                                Token vault API supplies new PAN token automatically
                                                              |
                                                              v
                                     BigQuery reconciled table --Pub/Sub--> re-identify (KMS)
                                                              |
                                                              v
                                              DigitalOcean MIT DB <--{POST}--
```

**When this directory applies, and when it does not:** see
[`production-v1.1.1/DESIGN.md`](production-v1.1.1/DESIGN.md)'s two-condition
test in full. In short, both of the following must hold, not just one:

1. Volume or continuity already justifies a Dataflow execution substrate
   (`production-v1.1.0/DESIGN.md`'s extraction verdict).
2. The reconciled PAN must be retained, queryable, and automatically
   reversible for more than one downstream consumer -- not bounded to a
   single manual field.

Absent both conditions, `production-v1.1.0/` remains the correct, lower-scope
default: it satisfies PCI DSS Requirement 3 by never storing a PAN at all,
rather than by storing one and protecting it, which is both a lower-scope and
a lower-operating-cost posture. This directory's Cloud KMS key management,
Cloud DLP template administration, and re-identification audit surface (PCI
DSS Req. 3.6, Req. 10) are real, ongoing compliance obligations that v1.1.0
simply does not carry.

**Component summary** (see each module's own docstring for the complete
rationale):

| Component | v1.1.0 | v1.1.1 |
|---|---|---|
| PAN at extraction boundary | blanked (`extraction_adapters.py`) | DLP deterministic-encryption tokenized, KMS-wrapped key (`deidentify_adapters.py`) |
| Reconciliation source | human, Firestore Console | authoritative token vault API, automated (`vault_reconciliation_adapters.py`) |
| Reconciliation store | Firestore, one field per document | BigQuery, queryable table (`store_adapters.py`) |
| Event backbone | GCS finalize + Firestore write | GCS finalize + Pub/Sub (`staging_service.py`) |
| Re-identification | not applicable (PAN never stored) | gated exclusively to the write-back step, never persisted (`sink_adapters.py`) |

**Deployment status:** this directory's three Cloud Run functions
(`staging-on-raw-upload-v111`, `staging-on-vault-reconciliation-v111`,
`staging-on-reconciled-event-v111` -- see
[`SETUP.md`](SETUP.md#7-deployment-procedure-production-pipeline-v111-de-identification-and-re-identification-dormant))
are deployed but deliberately left unwired to any live GCS bucket or Pub/Sub
subscription: no merchant is onboarded onto this pipeline, and
`configs/reference.json` is a synthetic schema demonstration, not a live
merchant. It exists to demonstrate the pattern is ready the moment a merchant
actually crosses the two-condition threshold above, at no incremental
Dataflow/DLP/KMS/BigQuery usage cost until that happens.

## Cost comparison

For adoption decisions evaluated on a per-merchant basis, the pluggable
architecture of `production-v1.1.0/` ties operating expenditure to the adapters
selected by a given merchant, rather than to the codebase itself. Every
merchant operating on the default configuration path (Cloud Run puller,
Firestore, absent Dataflow) incurs a cost approximately equivalent to that
incurred by the live pipeline at present. Dataflow constitutes the sole cost
component that shifts the operating expenditure from a near-zero basis toward
a material recurring expense, and remains an optional selection exercised on
a per-merchant basis, rather than being imposed pipeline-wide.

The live pipeline's empirically measured usage volume -- 2,275 records across
16 monthly Firestore collections, several dozen function invocations per month,
and substantially under 100 MB of aggregate CSV storage -- is employed below as
the baseline for estimation, in preference to an assumed or hypothetical volume.

| Cost driver | Live (CardCorp, measured) | Production, default path (per merchant, equivalent volume) | Production, with Dataflow selected (per merchant) |
|---|---|---|---|
| Compute (Cloud Run functions) | \$0.00 -- several dozen invocations per month, evaluated against a free tier of 2,000,000 requests, 180,000 vCPU-seconds, and 360,000 GiB-seconds per month | \$0.00 -- the free tier applies at the project level rather than per merchant; several dozen invocations per merchant per month remains substantially under the shared threshold across dozens of merchants | Equivalent to the default path |
| Reconciliation store | \$0.00 -- 2,275 documents (tens of kilobytes) evaluated against a daily free tier of 1 GiB stored, 50,000 reads, 20,000 writes, and 20,000 deletes | \$0.00 at a comparable per-merchant volume -- an estimated 20 or more merchants could each execute a complete dataset re-read on a single day prior to the shared daily read quota being exhausted | Equivalent to the default path |
| Extraction | included above | included above | + Dataflow: no free tier applies. A single small worker (1 vCPU, 3.75 GiB) executing a 10-minute monthly batch operation incurs approximately \$0.056 per vCPU-hour x 0.167 hour, plus \$0.003557 per GiB-hour x 3.75 GiB x 0.167 hour, yielding approximately \$0.01-\$0.02 per execution -- the compute expenditure itself is negligible; the material expenditure arises from operating a second execution substrate at all |
| Mapping authorship | manual; developer time exclusively | Gemini agent, flash-tier pricing (\$0.30 per 1M input tokens, \$2.50 per 1M output tokens): a single onboarding invocation of approximately 5,000 input and 1,000 output tokens incurs a cost of approximately \$0.004, executed once per merchant onboarding event | Equivalent |
| CI/CD | manual `gcloud functions deploy` invocation | Cloud Build, within the 120 free build-minutes per day (e2-standard-2) at the present deployment frequency | Equivalent |
| Storage (GCS) | approximately \$0.002 per month (substantially under 100 MB, at \$0.020 per GB per month) | scales linearly with merchant count, at the equivalent per-merchant rate | Equivalent |
| **Estimated monthly total** | **approximately \$0.00** | **approximately \$0.00 per merchant**, until dozens of merchants have been onboarded | **approximately \$0.00 per merchant**, plus several cents per merchant per month for Dataflow |

**Recommendation for adoption:** every newly onboarded merchant should default to
the low-cost configuration path; Dataflow should be approved for a given
merchant exclusively where data volume or throughput requirements genuinely
necessitate it, consistent with the verdicts documented in
`production-v1.1.0/DESIGN.md`. This practice maintains the marginal cost of onboarding
merchant *N+1* at a near-zero basis, unless that merchant possesses a specific
requirement for Dataflow's autoscaling extraction.

**`production-v1.1.1/`'s standby cost:** with no merchant onboarded, this
directory's incremental monthly cost is the flat per-key-version fee for the
KMS key referenced in `configs/reference.json` (approximately \$0.06/month),
plus Cloud Run's near-zero cost for three deployed-but-untriggered functions;
BigQuery, DLP, Pub/Sub, and Dataflow incur no charge absent actual usage. The
moment a merchant is onboarded, costs scale per DESIGN.md's two-condition
test -- Dataflow, DLP record transforms, and BigQuery storage/query costs
apply, in contrast to `production-v1.1.0/`'s near-zero default path.

The figures presented above employ published Google Cloud list pricing as of
the time of writing, applied to this project's own empirically measured usage;
they do not constitute a vendor quotation. Actual costs are dependent upon
region, committed-use discount arrangements, and observed traffic; verification
against the
[GCP Pricing Calculator](https://cloud.google.com/products/calculator) is
recommended prior to the commitment of budget.

Sources: [Cloud Run pricing](https://cloud.google.com/run/pricing),
[Firestore pricing](https://cloud.google.com/firestore/pricing),
[Cloud Storage pricing](https://cloud.google.com/storage/pricing),
[Dataflow pricing](https://cloud.google.com/dataflow/pricing),
[Cloud Build pricing](https://cloud.google.com/build/pricing),
[Cloud KMS pricing](https://cloud.google.com/kms/pricing),
[Cloud DLP pricing](https://cloud.google.com/sensitive-data-protection/pricing),
[BigQuery pricing](https://cloud.google.com/bigquery/pricing),
[Pub/Sub pricing](https://cloud.google.com/pubsub/pricing),
[Gemini API pricing](https://ai.google.dev/gemini-api/docs/pricing).

## Key Technical Contributions & Impact

- Spearheaded engineering modular 4-party payment model ETL pipelines via SFTP
  file transfer client migrations to the Revolut Bank acquirer API gateway.
- Architected Google Cloud Run functions via Eventarc for PCI-compliant card
  token migration, automating merchant-initiated transaction (MIT) PAN data
  cleaning and bidirectional DigitalOcean Kubernetes API synchronization.
  Reengineered to package a modular, config-driven, multi-merchant pipeline
  extension (`production-v1.1.0/`) for Payments-as-a-Service reuse across MIT data
  -- architected on Cloud Run, Firestore, and Cloud Storage, with a pluggable
  Dataflow (Apache Beam) extraction adapter for autoscaled, high-volume
  onboarding, Cloud Build CI/CD, and Cloud Shell/CLI-invoked, Gemini-assisted
  mapping-config authoring.
- Automated Google Cloud Storage (GCS) bucket-to-staging-layer transformation
  into Firestore NoSQL database on upload for secure reconciliation without
  PII leakage -- validated across 2,275 production records spanning 16
  monthly cycles with zero pipeline errors.
- Designed a de-identification/re-identification extension
  (`production-v1.1.1/`) adapting Google Cloud's reference architecture for
  PII de-identification via Cloud DLP to enterprise-scale, fully automated
  PAN reconciliation against an authoritative token vault API -- Cloud DLP
  deterministic-encryption tokenization keyed by a Cloud KMS-wrapped crypto
  key, BigQuery for queryable reconciled data, and Pub/Sub as the
  event-driven backbone, gated behind an explicit two-condition adoption
  test so the lower-scope, near-zero-cost v1.1.0 pipeline remains the
  default for every merchant that does not require it.

![Key technical contribution metrics](Key%20Technical%20Contributions%20Stats.png)

## References

1. PCI Security Standards Council. (2024). *Payment Card Industry Data
   Security Standard* (Version 4.0.1).
   https://www.pcisecuritystandards.org/standards/pci-dss/.
2. Payreto Services Inc. (n.d.). *About us*.
   https://www.payreto.com/about-us/.
3. Google Cloud. (n.d.). *De-identification and re-identification of PII in
   large-scale datasets using Cloud DLP*.
   https://docs.cloud.google.com/architecture/de-identification-re-identification-pii-using-cloud-dlp.
   `production-v1.1.1/` adapts this reference architecture to the card token
   migration domain; see `production-v1.1.1/DESIGN.md` for the adaptation's
   rationale and the two-condition test gating its adoption.
