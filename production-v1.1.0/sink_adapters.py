"""Pluggable synchronization of cleaned/<merchant>/*.csv back to a
merchant's target system. Selected per merchant via
configs/<merchant>.json's "sink_adapter" field.

digitalocean_kubernetes constitutes the reference instance: a
DigitalOcean-hosted Merchant Initiated Transaction (MIT) database
operated by Payreto Services Inc., a payment service provider and
financial outsourcing operator (see
https://www.payreto.com/about-us/), ported from the live pipeline's
deploy_digitalocean.py:push_records_to_digitalocean().

on_cleaned_uploaded (below) generalizes that same live pipeline
module's on_paas_reconciled Cloud Run function: it is deployed as its
own Eventarc-triggered entry point, invoked the moment
staging_service.py's on_firestore_write writes a merchant's reconciled
dataset to cleaned/<merchant>/*.csv, and dispatches to sync() with no
manual script invocation required.
"""

from __future__ import annotations

import io

import functions_framework
import pandas as pd
import requests
from cloudevents.http import CloudEvent
from google.cloud import storage

from merchant_config import env, load_merchant_config

CLEANED_PREFIX = "cleaned/"


def sync(merchant: str, config: dict, df: pd.DataFrame) -> None:
    """Dispatches according to config["sink_adapter"]; introduce a case
    here to onboard a new merchant's target system."""
    adapter = config["sink_adapter"]
    if adapter == "digitalocean_kubernetes":
        _sync_to_digitalocean_kubernetes(merchant, config, df)
        return
    raise SystemExit(f"Unrecognized sink_adapter {adapter!r} for merchant {merchant!r}")


def _sync_to_digitalocean_kubernetes(merchant: str, config: dict, df: pd.DataFrame) -> None:
    """Transmits reconciled rows via a POST request against the
    DigitalOcean Kubernetes API, authenticating with DIGITALOCEAN_TOKEN
    and <MERCHANT>_DO_CLUSTER_NAME -- structurally identical to the
    live pipeline's push_records_to_digitalocean()."""
    token = env("DIGITALOCEAN_TOKEN", required=True)
    cluster = env(f"{merchant.upper()}_DO_CLUSTER_NAME", required=True)
    resp = requests.post(
        f"https://{cluster}.k8s.ondigitalocean.com/{merchant}/records",
        headers={"Authorization": f"Bearer {token}"},
        json=df.to_dict(orient="records"),
        timeout=30,
    )
    resp.raise_for_status()


@functions_framework.cloud_event
def on_cleaned_uploaded(event: CloudEvent) -> None:
    """Cloud Run function (Generation 2): Google Cloud Storage (GCS)
    finalize trigger, generalizing the live pipeline's
    deploy_digitalocean.py:on_paas_reconciled to any onboarded
    merchant. Derives the merchant identifier from the blob path
    (cleaned/<merchant>/<file>.csv) and dispatches the reconciled rows
    to that merchant's configured sink_adapter via sync() above."""
    bucket_name = event.data["bucket"]
    blob_name = event.data["name"]

    if not blob_name.startswith(CLEANED_PREFIX) or not blob_name.lower().endswith(".csv"):
        print(f"Ignoring gs://{bucket_name}/{blob_name} (not a {CLEANED_PREFIX} CSV)")
        return
    parts = blob_name[len(CLEANED_PREFIX) :].split("/", 1)
    if len(parts) != 2:
        print(f"Ignoring gs://{bucket_name}/{blob_name} (no merchant segment present)")
        return
    merchant, _filename = parts
    config = load_merchant_config(merchant)

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_as_bytes()
    df = pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)

    print(f"Reconciled file detected for merchant {merchant!r}: gs://{bucket_name}/{blob_name}")
    print(f"  {len(df)} rows, {len(df.columns)} columns")
    sync(merchant, config, df)
    print(f"Synchronized {len(df)} reconciled row(s) to merchant {merchant!r}'s sink ({config['sink_adapter']})")
