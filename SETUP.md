# Developer setup

## Prerequisites

- Python 3.12
- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install), authenticated against a
  GCP project with billing enabled
- On that project: Cloud Firestore (Native mode), Cloud Storage, Cloud Functions,
  Cloud Run, Eventarc, and Cloud Build APIs enabled
  (`gcloud services enable firestore.googleapis.com storage.googleapis.com cloudfunctions.googleapis.com run.googleapis.com eventarc.googleapis.com cloudbuild.googleapis.com`)
- A Firestore database in **Native mode** already created in the target region
  (`gcloud firestore databases create --location=<REGION> --type=firestore-native`)

## 1. Authenticate

This procedure involves two distinct credential stores: the `gcloud` CLI's own
session, and the Application Default Credentials (ADC) that the Python client
libraries (`google-cloud-storage`, `google-cloud-firestore`, and related packages)
read at runtime. Both expire independently and require separate renewal when a
script fails with a "Reauthentication failed" or "could not automatically determine
credentials" error:

```bash
gcloud auth login                          # CLI session credentials
gcloud auth application-default login      # credentials read by the Python client libraries
gcloud config set project <PROJECT_ID>
```

To operate as a specific IAM principal rather than the default user account -- for
example, to verify the permissions available to a deployed function's runtime
service account:

```bash
gcloud config set account <SERVICE_ACCOUNT_EMAIL>
```

## 2. Required IAM roles

**Required for the principal executing the following `gcloud` commands** (a
developer account or a CI/CD service account):

| Role | Why |
|---|---|
| `roles/cloudfunctions.developer` | deploy/update Cloud Functions |
| `roles/run.admin` | Gen2 functions deploy as Cloud Run services under the hood |
| `roles/iam.serviceAccountUser` | required for deploy commands to impersonate the function's runtime service account |
| `roles/storage.admin` | create buckets, read/write objects for manual testing |
| `roles/datastore.user` | read/write Firestore documents for manual testing |

**For the deployed functions' runtime service account** (default:
`<PROJECT_NUMBER>-compute@developer.gserviceaccount.com`, unless `--service-account`
overrides it at deploy time):

| Role | Why |
|---|---|
| `roles/storage.objectAdmin` | read/write the staging bucket's objects |
| `roles/datastore.user` | read/write Firestore documents |
| `roles/datastore.viewer` | `live/export_firestore_to_gcs.py`'s `on_export_completed` looks up Firestore Admin API export operations (`datastore.operations.list`/`get`) to read which collection(s) a Console export was scoped to -- without this role that lookup fails closed and it falls back to exporting every dated collection |

`roles/editor` on the project satisfies all requirements listed above and reflects
CardCorp's current deployment; this scope is acceptable for a single-project
sandbox but exceeds what a production deployment should grant.

## 3. Clone and install

```bash
git clone <REPO_URL>
cd <REPO_DIRECTORY_NAME>

python3 -m venv .venv
source .venv/bin/activate

pip install -r live/requirements.txt   # to work on the live pipeline
pip install -r production/requirements.txt    # to work on the production pipeline
```

## 4. Environment variables

Every script reads these via `env()` at the top of its `main()` -- see each file's
own docstring for the full, authoritative list. The common ones:

| Variable | Used by | Example |
|---|---|---|
| `GCS_BUCKET` | both pipelines | `<BUCKET_NAME>` |
| `FIRESTORE_DATABASE` | both pipelines | `(default)` |
| `MERCHANT` | production only | `<MERCHANT_ID>`, e.g. `cardcorp` |
| `TRANSFORMED_BLOB_NAME` | manual/CLI runs | `transformed/Transformed_<Month>_<Year>.csv` |
| `RAW_BLOB_NAME` | manual/CLI runs | `raw/<Month Year>.csv` (live) / `raw/<MERCHANT_ID>/<Month Year>.csv` (production) |
| `DIGITALOCEAN_TOKEN`, `DO_CLUSTER_NAME` | DigitalOcean legs (placeholders until access is granted) | -- |

## 5. Deploy: live pipeline

Five Cloud Run functions (Gen2), one per stage. Execute each command from `live/`:

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
```

## 6. Deploy: production pipeline

**Constraint:** the Python Cloud Functions buildpack requires the entry-point file
to be named `main.py` at the source root; `staging_service.py` does not satisfy this
requirement as written. Stage a deploy directory containing a one-line `main.py`
that re-exports the required entry points, rather than renaming the source file
itself:

```bash
STAGE=$(mktemp -d)
cp production/staging_service.py production/merchant_config.py production/store_adapters.py \
   production/requirements.txt "$STAGE/"
cp -r production/configs "$STAGE/"
echo 'from staging_service import on_raw_uploaded, on_firestore_write  # noqa: F401' > "$STAGE/main.py"

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
```

**Onboarding a new merchant does not require a redeployment.** Adding
`production/configs/<MERCHANT_ID>.json` (see `production/configs/cardcorp.json` for
the required schema) takes effect on the next invocation, since the configuration
is read from disk at runtime rather than embedded in the deployed image at build
time.

## 7. Manual and local execution

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

## 8. Reference commands for diagnostics

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
