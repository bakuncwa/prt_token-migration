"""Decisions this pipeline needs before it's production-ready, kept as
data (not just prose) so any script or Cloud Run function can check
`OPEN_DECISIONS` and refuse to silently guess at an unresolved one --
see gemini_mapping_agent.py's use of REVIEW_GATE, for example.

Update an item's "status" to "resolved" (and fill in "resolution") once
a real decision is made; leave the item in place as a record of what
was decided and why, rather than deleting it.
"""

from __future__ import annotations

OPEN_DECISIONS: list[dict] = [
    {
        "id": "gemini_review_gate",
        "title": "Review gate for Gemini-proposed mapping configs",
        "status": "unresolved",
        "detail": (
            "Where the proposed configs/<merchant>.json diff surfaces (PR comment? "
            "a review CLI step? Cloud Shell output only?), and who has to sign off "
            "before staging_service.py can be redeployed with it -- gemini_mapping_agent.py "
            "currently only prints the diff and refuses to write the config file itself."
        ),
        "resolution": None,
    },
    {
        "id": "production_test_strategy",
        "title": "Dry-run only vs. a separate seeded GCS bucket",
        "status": "resolved",
        "detail": (
            "Whether the production pipeline's first real test run is dry-run only, or "
            "needs its own separate GCS bucket seeded from the same CardCorp source "
            "data used by the live pipeline, for a side-by-side comparison."
        ),
        "resolution": (
            "Resolved for initial validation, two ways: (1) a separate bucket "
            "(cardcorp-token-migration-production-test), seeded from the same raw/ and "
            "transformed/ CardCorp CSVs already in the live pipeline's bucket, and "
            "reconciled against the same live Firestore data, proving the production "
            "staging service reproduces the live pipeline exactly; (2) a synthetic "
            "second merchant (configs/samplepay.json, sample_data/samplepay_raw.csv) "
            "with a genuinely different schema -- different column names, a different "
            "Expiry date format, a different repair field length -- run locally with no "
            "cloud dependency, proving the staging service generalizes beyond CardCorp "
            "rather than only reproducing it. See production/test_staging_service.py. "
            "Still open: whether a *production* deployment for a second real merchant "
            "gets a bucket of its own or a namespaced prefix in a shared bucket."
        ),
    },
    {
        "id": "digitalocean_payreto_access",
        "title": "DigitalOcean Kubernetes API credentials for Payreto",
        "status": "unresolved",
        "detail": (
            "production/extraction_adapters.py and production/sink_adapters.py call the "
            "DigitalOcean Kubernetes API directly (see _extract_via_cloud_run_puller() and "
            "_sync_to_digitalocean_kubernetes()); each requires <MERCHANT>_SOURCE_TOKEN, "
            "<MERCHANT>_SOURCE_CLUSTER, DIGITALOCEAN_TOKEN, and <MERCHANT>_DO_CLUSTER_NAME "
            "to be set in the deployment environment for Payreto's DigitalOcean-hosted MIT "
            "database (https://www.payreto.com/about-us/). The live pipeline's "
            "extract_digitalocean.py and deploy_digitalocean.py remain unimplemented "
            "placeholders on the DigitalOcean leg. Nothing on the GCS/Firestore side of "
            "either pipeline is blocked by this; only the two ends that touch DigitalOcean are."
        ),
        "resolution": None,
    },
]


def require_resolved(decision_id: str) -> dict:
    """Raise clearly if code tries to depend on a decision that hasn't
    been made yet, instead of silently proceeding with a guess."""
    for decision in OPEN_DECISIONS:
        if decision["id"] == decision_id:
            if decision["status"] != "resolved":
                raise SystemExit(
                    f"Open decision {decision_id!r} ({decision['title']}) is not resolved yet: "
                    f"{decision['detail']}"
                )
            return decision
    raise SystemExit(f"No such decision {decision_id!r}")
