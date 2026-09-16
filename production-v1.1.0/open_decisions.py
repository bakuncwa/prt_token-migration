"""Decisions this pipeline requires resolution of prior to attaining
production readiness, maintained as structured data rather than as
unstructured prose, such that any script or Cloud Run function may
examine `OPEN_DECISIONS` and decline to silently presume an
unresolved item -- see gemini_mapping_agent.py's invocation of
require_resolved(), for example.

Update an item's "status" field to "resolved" (populating
"resolution" accordingly) once an actual decision has been reached;
retain the item in place as a record of what was decided and why,
rather than removing it.
"""

from __future__ import annotations

OPEN_DECISIONS: list[dict] = [
    {
        "id": "gemini_review_gate",
        "title": "Review gate for Gemini-proposed mapping configurations",
        "status": "unresolved",
        "detail": (
            "Where the proposed configs/<merchant>.json differential is to surface (a pull "
            "request comment? a dedicated review CLI step? Cloud Shell output exclusively?), "
            "and who is authorized to approve it prior to staging_service.py's redeployment "
            "-- gemini_mapping_agent.py presently confines itself to printing the differential "
            "and declines to write the configuration file directly."
        ),
        "resolution": None,
    },
    {
        "id": "production_test_strategy",
        "title": "Dry-run exclusively versus a separate, seeded GCS bucket",
        "status": "resolved",
        "detail": (
            "Whether the production pipeline's initial real test execution should be "
            "dry-run exclusively, or should employ a dedicated, separate GCS bucket seeded "
            "from the same reference-merchant source data used by the live pipeline, for "
            "direct comparison."
        ),
        "resolution": (
            "Resolved for initial validation, by two methods: (1) a dedicated bucket "
            "(cardcorp-token-migration-production-test), seeded from the same raw/ and "
            "transformed/ reference CSVs already present in the live pipeline's bucket, and "
            "reconciled against the identical live Firestore data, demonstrating that the "
            "production staging service reproduces the live pipeline precisely; (2) a "
            "synthetic second merchant (configs/samplepay.json, "
            "sample_data/samplepay_raw.csv) possessing a genuinely distinct schema -- "
            "differing column names, a differing Expiry date format, a differing repair "
            "field length -- executed locally with no cloud dependency, demonstrating that "
            "the staging service generalizes beyond the reference merchant rather than "
            "merely reproducing it. See production-v1.1.0/test_staging_service.py. Still "
            "unresolved: whether a *production* deployment for a second, genuinely new "
            "merchant is provisioned with a dedicated bucket or a namespaced prefix within "
            "a shared bucket."
        ),
    },
    {
        "id": "digitalocean_payreto_access",
        "title": "DigitalOcean Kubernetes API credentials for Payreto",
        "status": "unresolved",
        "detail": (
            "production-v1.1.0/extraction_adapters.py and production-v1.1.0/sink_adapters.py invoke the "
            "DigitalOcean Kubernetes API directly (see _extract_via_cloud_run_puller() and "
            "_sync_to_digitalocean_kubernetes()); each requires <MERCHANT>_SOURCE_TOKEN, "
            "<MERCHANT>_SOURCE_CLUSTER, DIGITALOCEAN_TOKEN, and <MERCHANT>_DO_CLUSTER_NAME "
            "to be present within the deployment environment, for Payreto's "
            "DigitalOcean-hosted MIT database (https://www.payreto.com/about-us/). The "
            "Cloud Run wiring itself is in place -- sink_adapters.py:on_cleaned_uploaded is "
            "deployable as staging-on-cleaned-upload (see SETUP.md Step 6), triggered by the "
            "identical cleaned/ GCS finalize event that on_firestore_write produces -- so "
            "this decision now blocks exclusively on the credentials themselves, not on "
            "absent Cloud Run scaffolding. The live pipeline's extract_digitalocean.py and "
            "deploy_digitalocean.py remain unimplemented placeholders on the DigitalOcean "
            "leg. Neither pipeline's GCS/Firestore-facing components are obstructed by this "
            "limitation; only the two components that interface with DigitalOcean are."
        ),
        "resolution": None,
    },
]


def require_resolved(decision_id: str) -> dict:
    """Raises explicitly if code attempts to depend upon a decision
    that has not yet been resolved, rather than proceeding silently on
    the basis of an assumption."""
    for decision in OPEN_DECISIONS:
        if decision["id"] == decision_id:
            if decision["status"] != "resolved":
                raise SystemExit(
                    f"Open decision {decision_id!r} ({decision['title']}) remains unresolved: "
                    f"{decision['detail']}"
                )
            return decision
    raise SystemExit(f"No such decision exists: {decision_id!r}")
