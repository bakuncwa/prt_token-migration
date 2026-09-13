"""Trigger deployment of the Token Migration ETL service to DigitalOcean,
and sync PaaS-reconciled data back to the DigitalOcean MIT Database.

Template only -- no live DigitalOcean credentials are configured in this
environment, so neither leg of this script runs automatically yet. Fill
in the env vars below and run/deploy once access exists.

## 1. Image rollout (main(), unchanged)

Targets the DigitalOcean Kubernetes cluster shown in the ETL diagram
("DigitalOcean Kubernetes API"): authenticates doctl, pulls kubeconfig
for the target cluster, then rolls the ETL deployment to the given
image tag and waits for the rollout to finish. Requires `doctl` and
`kubectl` on the runner.

Required environment variables:
  DIGITALOCEAN_TOKEN     DO API token (scope: read/write)
  DO_CLUSTER_NAME         DOKS cluster name or ID
  K8S_NAMESPACE           namespace the ETL deployment lives in
  K8S_DEPLOYMENT_NAME     Deployment name to roll
  K8S_CONTAINER_NAME      container name within the pod spec to update
  IMAGE                   full image ref to deploy, e.g. registry/etl:1.2.3

Usage:
  DIGITALOCEAN_TOKEN=... DO_CLUSTER_NAME=... K8S_NAMESPACE=... \\
  K8S_DEPLOYMENT_NAME=... K8S_CONTAINER_NAME=... IMAGE=... \\
  python deploy_digitalocean.py

## 2. Reconciliation write-back (on_paas_reconciled(), Cloud Function)

Covers the reverse-sync leg on the right side of the diagram: once
export_firestore_to_gcs.py's on_firestore_write Cloud Function writes
the dot-restored, reconciled dataset into gs://<bucket>/cleaned/, this
Cloud Function fires automatically (Eventarc, GCS finalize) and POSTs
the reconciled rows back to the DigitalOcean MIT Database via the
DigitalOcean Kubernetes API. Like fetch_mit_records() in
extract_digitalocean.py, push_records_to_digitalocean() is a placeholder
-- no live DigitalOcean API write access is configured in this
environment yet.

Required environment variables:
  DIGITALOCEAN_TOKEN   DO API token (scope: read/write)
  DO_CLUSTER_NAME       DOKS cluster name or ID fronting the MIT Database

Deploy (2nd gen, from the scripts/ directory). --min-instances=0 and a
low --max-instances cap keep this inside the Cloud Run/Cloud Functions
Always Free tier:
  gcloud functions deploy sync-paas-reconciled-to-digitalocean \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_paas_reconciled \\
    --trigger-bucket=cardcorp-token-0dc1f93138 \\
    --set-env-vars=DO_CLUSTER_NAME=... \\
    --set-secrets=DIGITALOCEAN_TOKEN=digitalocean-token:latest \\
    --memory=256Mi --timeout=60s --min-instances=0 --max-instances=3
"""

from __future__ import annotations

import io
import os
import subprocess
import sys

import functions_framework
import pandas as pd
from cloudevents.http import CloudEvent
from google.cloud import storage

CLEANED_PREFIX = "cleaned/"


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def run(cmd: list[str]) -> None:
    print("+", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main() -> None:
    token = env("DIGITALOCEAN_TOKEN")
    cluster = env("DO_CLUSTER_NAME")
    namespace = env("K8S_NAMESPACE")
    deployment = env("K8S_DEPLOYMENT_NAME")
    container = env("K8S_CONTAINER_NAME")
    image = env("IMAGE")

    run(["doctl", "auth", "init", "--access-token", token])
    run(["doctl", "kubernetes", "cluster", "kubeconfig", "save", cluster])
    run(
        [
            "kubectl",
            "-n",
            namespace,
            "set",
            "image",
            f"deployment/{deployment}",
            f"{container}={image}",
        ]
    )
    run(
        [
            "kubectl",
            "-n",
            namespace,
            "rollout",
            "status",
            f"deployment/{deployment}",
            "--timeout=180s",
        ]
    )
    print(f"Deployed {image} to {deployment} in {namespace} on cluster {cluster}")


def download_reconciled_csv(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def push_records_to_digitalocean(token: str, cluster: str, df: pd.DataFrame) -> None:
    """POST reconciled rows back to the MIT Database through the
    DigitalOcean Kubernetes API.

    Placeholder: DigitalOcean write access has not been provisioned yet.
    Replace the body once the actual DOKS-fronted MIT Database write
    endpoint is known, e.g.:

        resp = requests.post(
            f"https://{cluster}.k8s.ondigitalocean.com/mit/records",
            headers={"Authorization": f"Bearer {token}"},
            json=df.to_dict(orient="records"),
            timeout=30,
        )
        resp.raise_for_status()
    """
    raise SystemExit(
        "push_records_to_digitalocean() is a placeholder -- no "
        "DigitalOcean API write access is configured yet. Implement it "
        "once DIGITALOCEAN_TOKEN write access is granted."
    )


@functions_framework.cloud_event
def on_paas_reconciled(event: CloudEvent) -> None:
    """Cloud Function (2nd gen): fires the moment on_firestore_write
    (export_firestore_to_gcs.py) writes a reconciled file back to
    cleaned/, and syncs it back to DigitalOcean -- no script invocation
    required."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not blob_name.startswith(CLEANED_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {CLEANED_PREFIX} CSV)")
        return

    token = env("DIGITALOCEAN_TOKEN")
    cluster = env("DO_CLUSTER_NAME")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    print(f"Reconciled file detected: gs://{bucket_name}/{blob_name}")
    df = download_reconciled_csv(bucket, blob_name)
    print(f"  {len(df)} rows, {len(df.columns)} columns")

    push_records_to_digitalocean(token, cluster, df)
    print(f"Synced {len(df)} reconciled rows back to DigitalOcean MIT Database (cluster {cluster})")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)
