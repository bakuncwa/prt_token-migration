# Developer Setup and Deployment Reference

## Prerequisites

- Python 3.12.
- The [`gcloud` CLI](https://cloud.google.com/sdk/docs/install), authenticated
  against a Google Cloud Platform (GCP) project for which billing has been
  enabled.
- Within that project, the following APIs must be enabled: Cloud Firestore
  (Native mode), Cloud Storage, Cloud Functions, Cloud Run, Eventarc, and Cloud
  Build
  (`gcloud services enable firestore.googleapis.com storage.googleapis.com cloudfunctions.googleapis.com run.googleapis.com eventarc.googleapis.com cloudbuild.googleapis.com`).
- A Firestore database provisioned in **Native mode** within the target region
  (`gcloud firestore databases create --location=<REGION> --type=firestore-native`).

## 1. Authentication Procedure

This procedure necessitates the maintenance of two distinct credential stores:
the `gcloud` CLI's own session credentials, and the Application Default
Credentials (ADC) retrieved at runtime by the Python client libraries
(`google-cloud-storage`, `google-cloud-firestore`, and related packages). These
credential stores expire independently and therefore require separate renewal
procedures whenever a script fails with a "Reauthentication failed" or "could
not automatically determine credentials" exception:

```bash
gcloud auth login                          # CLI session credentials
gcloud auth application-default login      # credentials read by the Python client libraries
gcloud config set project <PROJECT_ID>
```

To assume the identity of a specific IAM principal in lieu of the default user
account -- for instance, to verify the permissions granted to a deployed
function's runtime service account:

```bash
gcloud config set account <SERVICE_ACCOUNT_EMAIL>
```

## 2. Requisite IAM Roles

**Requisite for the principal executing the subsequent `gcloud` commands** (a
developer account or a CI/CD service account):

| Role | Rationale |
|---|---|
| `roles/cloudfunctions.developer` | enables deployment and modification of Cloud Functions |
| `roles/run.admin` | Generation 2 functions are deployed as Cloud Run services at the infrastructure level |
| `roles/iam.serviceAccountUser` | requisite for deployment commands to assume the identity of the function's runtime service account |
| `roles/storage.admin` | enables bucket creation and object read/write operations for manual verification |
| `roles/datastore.user` | enables read/write access to Firestore documents for manual verification |

**Requisite for the deployed functions' runtime service account** (default:
`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`, unless overridden at
deployment time by the `--service-account` flag):

| Role | Rationale |
|---|---|
| `roles/storage.objectAdmin` | enables read/write access to the staging bucket's objects |
| `roles/datastore.user` | enables read/write access to Firestore documents |
| `roles/datastore.viewer` | `live/export_firestore_to_gcs.py`'s `on_export_completed` function queries the Firestore Admin API for export operations (`datastore.operations.list`/`get`) to determine which collection(s) a given Console export was scoped to; absent this role, the query fails closed, and the function defaults to exporting every dated collection |

The `roles/editor` role, applied at the project level, satisfies all
requirements enumerated above and reflects the scope of CardCorp's current
deployment; while acceptable for a single-project development environment,
this scope exceeds what should be granted within a production deployment.

## 3. Repository Acquisition and Dependency Installation

```bash
git clone <REPO_URL>
cd <REPO_DIRECTORY_NAME>

python3 -m venv .venv
source .venv/bin/activate

pip install -r live/requirements.txt   # to work on the live pipeline
pip install -r production/requirements.txt    # to work on the production pipeline
```

## 4. Environment Variable Reference

Each script retrieves these values by means of the `env()` function invoked at
the outset of its `main()` routine; consult each file's own docstring for the
complete, authoritative enumeration. The variables common to both pipelines
are enumerated below:

| Variable | Used by | Example |
|---|---|---|
| `GCS_BUCKET` | both pipelines | `<BUCKET_NAME>` |
| `FIRESTORE_DATABASE` | both pipelines | `(default)` |
| `MERCHANT` | production only | `<MERCHANT_ID>`, e.g. `pilot` |
| `TRANSFORMED_BLOB_NAME` | manual/CLI runs | `transformed/Transformed_<Month>_<Year>.csv` |
| `RAW_BLOB_NAME` | manual/CLI runs | `raw/<Month Year>.csv` (live) / `raw/<MERCHANT_ID>/<Month Year>.csv` (production) |
| `DIGITALOCEAN_TOKEN`, `DO_CLUSTER_NAME` | DigitalOcean legs (placeholders until access is granted) | -- |

## 5. Deployment Procedure: Live Pipeline

This procedure deploys six Generation 2 Cloud Run functions: one per pipeline
stage, plus a sixth that replicates newly uploaded raw data into the
production pipeline's dedicated bucket (see Step 6) for the reference
merchant. Each command below must be executed from within the `live/`
directory:

```bash
cd live

gcloud functions deploy transform-on-raw-upload \
  --gen2 --runtime=python312 --region=<REGION> \
  --source=. --entry-point=on_raw_uploaded \
  --trigger-bucket=<BUCKET_NAME> \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

gcloud functions deploy load-firestore-on-transformed-upload \
  --gen2 --runtime=python312 --region=<REGION> \
  --source=. --entry-point=on_transformed_uploaded \
  --trigger-bucket=<BUCKET_NAME> \
  --set-env-vars=FIRESTORE_DATABASE="(default)" \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

gcloud functions deploy export-on-firestore-export-button \
  --gen2 --runtime=python312 --region=<REGION> \
  --source=. --entry-point=on_export_completed \
  --trigger-bucket=<BUCKET_NAME> \
  --set-env-vars=FIRESTORE_DATABASE="(default)" \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

gcloud functions deploy export-on-firestore-write \
  --gen2 --runtime=python312 --region=<REGION> \
  --source=. --entry-point=on_firestore_write \
  --trigger-event-filters="type=google.cloud.firestore.document.v1.written" \
  --trigger-event-filters="database=(default)" \
  --trigger-event-filters-path-pattern="document={collection}/{docId}" \
  --trigger-location=<REGION> \
  --set-env-vars=GCS_BUCKET=<BUCKET_NAME>,FIRESTORE_DATABASE="(default)" \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

gcloud functions deploy sync-paas-reconciled-to-digitalocean \
  --gen2 --runtime=python312 --region=<REGION> \
  --source=. --entry-point=on_paas_reconciled \
  --trigger-bucket=<BUCKET_NAME> \
  --set-secrets=DIGITALOCEAN_TOKEN=<SECRET_NAME>:latest \
  --set-env-vars=DO_CLUSTER_NAME=<DO_CLUSTER_NAME> \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

gcloud functions deploy replicate-raw-to-production \
  --gen2 --runtime=python312 --region=<REGION> \
  --source=. --entry-point=on_raw_uploaded_replicate \
  --trigger-bucket=<BUCKET_NAME> \
  --set-env-vars=PRODUCTION_BUCKET=<PRODUCTION_BUCKET_NAME>,PRODUCTION_MERCHANT=pilot \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3
```

`replicate-raw-to-production` (`live/replicate_raw_to_production.py`) mirrors
each newly uploaded `raw/*.csv` object into
`gs://<PRODUCTION_BUCKET_NAME>/raw/pilot/`, so that the reference merchant
(`configs/pilot.json`, see Step 6) continues to exercise the production
pipeline's generalized `staging_service.py` against authentic data, without
requiring a merchant subfolder to ever exist within the live bucket itself.

## 6. Deployment Procedure: Production Pipeline

**Architectural constraint:** the Python Cloud Functions buildpack imposes a
requirement that the entry-point file be named `main.py` and reside at the
source root; `staging_service.py`, as written, does not satisfy this
requirement. The recommended remediation is the construction of a deployment
directory containing a single-line `main.py` module that re-exports the
requisite entry points, rather than renaming the source file itself:

```bash
STAGE=$(mktemp -d)
cp production/staging_service.py production/merchant_config.py production/store_adapters.py \
   production/sink_adapters.py production/requirements.txt "$STAGE/"
cp -r production/configs "$STAGE/"
{
  echo 'from staging_service import on_raw_uploaded, on_firestore_write  # noqa: F401'
  echo 'from sink_adapters import on_cleaned_uploaded  # noqa: F401'
} > "$STAGE/main.py"

gcloud functions deploy staging-on-raw-upload \
  --gen2 --runtime=python312 --region=<REGION> \
  --source="$STAGE" --entry-point=on_raw_uploaded \
  --trigger-bucket=<BUCKET_NAME> \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

gcloud functions deploy staging-on-firestore-write \
  --gen2 --runtime=python312 --region=<REGION> \
  --source="$STAGE" --entry-point=on_firestore_write \
  --trigger-event-filters="type=google.cloud.firestore.document.v1.written" \
  --trigger-event-filters="database=(default)" \
  --trigger-event-filters-path-pattern="document={collection}/{docId}" \
  --trigger-location=<REGION> \
  --set-env-vars=GCS_BUCKET=<BUCKET_NAME>,FIRESTORE_DATABASE="(default)" \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3

gcloud functions deploy staging-on-cleaned-upload \
  --gen2 --runtime=python312 --region=<REGION> \
  --source="$STAGE" --entry-point=on_cleaned_uploaded \
  --trigger-bucket=<BUCKET_NAME> \
  --set-secrets=DIGITALOCEAN_TOKEN=<SECRET_NAME>:latest \
  --set-env-vars=<MERCHANT>_DO_CLUSTER_NAME=<DO_CLUSTER_NAME> \
  --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3
```

The third function, `staging-on-cleaned-upload`, mirrors the live pipeline's
`sync-paas-reconciled-to-digitalocean` (see Step 5 above), generalized to any
merchant whose `configs/<merchant>.json` declares `"sink_adapter":
"digitalocean_kubernetes"`: it fires on the identical `cleaned/` GCS finalize
event that `on_firestore_write` produces, and requires one
`<MERCHANT>_DO_CLUSTER_NAME` environment variable per such merchant onboarded,
in addition to the shared `DIGITALOCEAN_TOKEN` secret.

**The onboarding of a new merchant does not necessitate a redeployment,**
unless that merchant is the first to select a given sink adapter's required
credential. The addition of `production/configs/<MERCHANT_ID>.json` (see
`production/configs/pilot.json` for the requisite schema) takes effect upon
the subsequent invocation, inasmuch as the configuration is read from disk at
runtime rather than embedded within the deployed image at build time; a
merchant selecting `digitalocean_kubernetes` as its sink does, however,
require `staging-on-cleaned-upload` to be redeployed with that merchant's
`<MERCHANT>_DO_CLUSTER_NAME` variable added.

## 7. Manual and Local Execution Procedures

```bash
# Live pipeline
cd live
GCS_BUCKET=<BUCKET_NAME> python transform_load_gcs.py
GCS_BUCKET=<BUCKET_NAME> python load_to_firestore.py
GCS_BUCKET=<BUCKET_NAME> python export_firestore_to_gcs.py
python test_transform_local.py <RAW_CSV_PATH> <OUTPUT_CSV_PATH>   # no cloud resources needed

# Production pipeline
cd production
GCS_BUCKET=<BUCKET_NAME> MERCHANT=<MERCHANT_ID> RAW_BLOB_NAME="raw/<MERCHANT_ID>/<Month Year>.csv" \
  python staging_service.py transform
GCS_BUCKET=<BUCKET_NAME> MERCHANT=<MERCHANT_ID> TRANSFORMED_BLOB_NAME="transformed/<MERCHANT_ID>/Transformed_<Month>_<Year>.csv" \
  python staging_service.py reconcile

PRODUCTION_TEST_BUCKET=<TEST_BUCKET_NAME> python test_staging_service.py   # requires live GCP data, see the script's docstring
```

## 8. Diagnostic Command Reference

```bash
# List deployed functions and their trigger type
gcloud functions list --v2 --project=<PROJECT_ID>

# Tail a function's logs
gcloud functions logs read <FUNCTION_NAME> --gen2 --region=<REGION> --project=<PROJECT_ID> --limit=50

# List Firestore collections and rough doc counts (no built-in gcloud command --
# use the Python client, or browse the Console's Data viewer)
python3 -c "
from google.cloud import firestore
db = firestore.Client(project='<PROJECT_ID>', database='(default)')
for c in db.collections():
    print(c.id, sum(1 for _ in c.stream()))
"

# List objects under a prefix
gcloud storage ls gs://<BUCKET_NAME>/<PREFIX>/

# Confirm the runtime service account's IAM roles
gcloud projects get-iam-policy <PROJECT_ID> \
  --flatten="bindings[].members" \
  --filter="bindings.members:<SERVICE_ACCOUNT_EMAIL>" \
  --format="table(bindings.role)"
```
