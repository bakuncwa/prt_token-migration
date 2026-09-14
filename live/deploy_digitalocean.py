"""Triggers deployment of the Token Migration ETL service to
DigitalOcean, and synchronizes PaaS-reconciled data back to the
DigitalOcean MIT Database.

Template exclusively -- no live DigitalOcean credentials are
configured within this environment, such that neither leg of this
script executes automatically at present. Populate the environment
variables enumerated below and execute/deploy once access has been
provisioned.

## 1. Image rollout (main(), unaltered)

Targets the DigitalOcean Kubernetes cluster depicted within the ETL
diagram ("DigitalOcean Kubernetes API"): authenticates doctl, retrieves
the kubeconfig for the target cluster, then rolls the ETL deployment
to the specified image tag and awaits the rollout's completion.
Requires `doctl` and `kubectl` upon the runner.

Required environment variables:
  DIGITALOCEAN_TOKEN     DigitalOcean API token (scope: read/write)
  DO_CLUSTER_NAME         DOKS cluster name or identifier
  K8S_NAMESPACE           namespace within which the ETL deployment resides
  K8S_DEPLOYMENT_NAME     Deployment name to roll
  K8S_CONTAINER_NAME      container name within the pod specification to update
  IMAGE                   complete image reference to deploy, for example registry/etl:1.2.3

Usage:
  DIGITALOCEAN_TOKEN=... DO_CLUSTER_NAME=... K8S_NAMESPACE=... \\
  K8S_DEPLOYMENT_NAME=... K8S_CONTAINER_NAME=... IMAGE=... \\
  python deploy_digitalocean.py

## 2. Reconciliation write-back (on_paas_reconciled(), Cloud Function)

Addresses the reverse-synchronization leg on the right side of the
diagram: once export_firestore_to_gcs.py's on_firestore_write Cloud
Function writes the reconciled dataset into gs://<bucket>/cleaned/,
this Cloud Function is invoked automatically (Eventarc, GCS finalize)
and transmits the reconciled rows back to the DigitalOcean MIT
Database via the DigitalOcean Kubernetes API. As with
fetch_mit_records() within extract_digitalocean.py,
push_records_to_digitalocean() constitutes a placeholder -- no live
DigitalOcean API write access is configured within this environment.

Required environment variables:
  DIGITALOCEAN_TOKEN   DigitalOcean API token (scope: read/write)
  DO_CLUSTER_NAME       DOKS cluster name or identifier fronting the MIT Database

Deployment (Generation 2, executed from the scripts/ directory).
--min-instances=0 together with a low --max-instances ceiling
maintain this function within the Cloud Run/Cloud Functions Always
Free tier:
  gcloud functions deploy sync-paas-reconciled-to-digitalocean \\
    --gen2 --runtime=python312 --region=europe-west2 \\
    --source=. --entry-point=on_paas_reconciled \\
    --trigger-bucket=<BUCKET_NAME> \\
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
    """Retrieves environment variable `name`, raising explicitly if
    it is absent."""
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def run(cmd: list[str]) -> None:
    """Executes a subprocess command, echoing it prior to execution
    and raising if it exits with a non-zero status."""
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
    print(f"Deployed {image} to {deployment} within {namespace} on cluster {cluster}")


def download_reconciled_csv(bucket: storage.Bucket, blob_name: str) -> pd.DataFrame:
    """Downloads and parses the reconciled CSV object from the
    specified bucket."""
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def push_records_to_digitalocean(token: str, cluster: str, df: pd.DataFrame) -> None:
    """Transmits reconciled rows back to the MIT Database via the
    DigitalOcean Kubernetes API.

    Placeholder: DigitalOcean write access has not yet been
    provisioned. Replace the body once the authentic DOKS-fronted MIT
    Database write endpoint is known, for example:

        resp = requests.post(
            f"https://{cluster}.k8s.ondigitalocean.com/mit/records",
            headers={"Authorization": f"Bearer {token}"},
            json=df.to_dict(orient="records"),
            timeout=30,
        )
        resp.raise_for_status()
    """
    raise SystemExit(
        "push_records_to_digitalocean() constitutes a placeholder -- no "
        "DigitalOcean API write access is presently configured. Implement it "
        "once DIGITALOCEAN_TOKEN write access has been granted."
    )


@functions_framework.cloud_event
def on_paas_reconciled(event: CloudEvent) -> None:
    """Cloud Function (Generation 2): invoked the moment
    on_firestore_write (export_firestore_to_gcs.py) writes a
    reconciled file back to cleaned/, synchronizing it to DigitalOcean
    -- no script invocation is required."""
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
    print(f"Synchronized {len(df)} reconciled rows back to the DigitalOcean MIT Database (cluster {cluster})")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}", file=sys.stderr)
        sys.exit(e.returncode)
