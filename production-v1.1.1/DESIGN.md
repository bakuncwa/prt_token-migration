# Production Pipeline v1.1.1: De-identification/Re-identification Design Decisions

This document evaluates this directory's departure from
[`production-v1.1.0/`](../production-v1.1.0/)'s core architectural premise --
that Personal Account Number (PAN) data should never be stored, in any form,
at any pipeline stage -- and records the two-condition test that gates when
this directory's design should be selected over v1.1.0's for a given
merchant.

## Verdict: v1.1.0 remains the default; v1.1.1 is an explicit opt-in

**v1.1.0's blank-and-manual design is the correct default for every merchant
onboarded to date**, and should remain so: it satisfies PCI DSS Requirement 3
by never storing a PAN in any automated system rather than by protecting a
stored PAN, which is both a lower-scope and a lower-operating-cost posture
(see [`production-v1.1.0/README` cost table](../README.md#cost-comparison)).

**v1.1.1 is justified exclusively when both of the following hold:**

1. **Volume or continuity already justifies a Dataflow execution substrate**,
   per [`production-v1.1.0/DESIGN.md`](../production-v1.1.0/DESIGN.md)'s
   extraction verdict -- continuous extraction, or extraction at a volume a
   single Cloud Run request cannot complete within one timeout interval.
2. **The reconciled PAN must be retained, queryable, and automatically
   reversible for more than one downstream consumer** -- for example,
   analytics or fraud review querying BigQuery, or a token vault API that
   supplies the newly issued PAN programmatically rather than through a
   single human's Firestore entry, such that no manual reconciliation step
   exists to bound PAN exposure to one narrowly-scoped field.

A merchant satisfying only one condition should adopt v1.1.0 with the
Dataflow extraction adapter (`extraction_adapters.py`'s `dataflow` case,
already present in v1.1.0), not this directory. Absent both conditions, this
directory imposes Cloud KMS key management (rotation, HSM-backed keys),
Cloud DLP template administration, and a re-identification audit surface
(PCI DSS Req. 3.6, Req. 10) that v1.1.0 has no need to carry.

## De-identification substrate: Dataflow + Cloud DLP + Cloud KMS

**Verdict:** PAN values are rendered safe-at-rest via Cloud DLP's
deterministic-encryption transform (`deidentify_adapters.py`), keyed by a
Cloud KMS-wrapped crypto key, executed within the same Dataflow job already
justified by condition (1) above -- not a separate execution substrate.
Deterministic encryption (rather than format-preserving encryption or
straight AES) is selected specifically because the same input PAN must
tokenize to the same value on every run, which is what permits
`vault_reconciliation_adapters.py`'s join against the token vault's response
to remain keyed by a stable token rather than a per-run value.

## Reconciliation source: automated token vault API, not manual entry

**Verdict:** `vault_reconciliation_adapters.py` retrieves the newly issued
PAN programmatically from an authoritative token vault API, replacing
v1.1.0's human-in-Firestore step entirely. This is the change that makes
"fully automated, enterprise-scale" possible -- a human editing a Firestore
document does not scale past a few hundred records a month, and reintroduces
exactly the PAN-exposure question v1.1.0 was designed to avoid by scoping
that exposure to one person, one field, one month. No vault credentials have
been provisioned in this environment; see `vault_reconciliation_adapters.py`
for the placeholder this decision currently resolves to (structurally
identical to `live/extract_digitalocean.py` and `deploy_digitalocean.py`'s
unimplemented-until-credentialed DigitalOcean legs).

## Reconciliation store: BigQuery, not Firestore

**Verdict:** `store_adapters.py`'s `bigquery` store type lands de-identified,
reconciled rows in a queryable table, satisfying the "queryable at enterprise
level" requirement this directory exists to serve. Firestore's single-field,
Console-edited design (v1.1.0's `store_adapters.py`) has no equivalent
here -- there is no human reviewer step for BigQuery to support.

## Re-identification: gated to the write-back path exclusively

**Verdict:** `deidentify_adapters.py`'s `reidentify_value()` is invoked
exclusively from `sink_adapters.py`'s write-back step, immediately before the
POST to DigitalOcean, and its result is never persisted -- not to GCS, not to
BigQuery, not to logs. No human and no analyst-facing query path ever
receives a re-identified value; BigQuery access for analytics purposes
remains scoped to the de-identified columns. This mirrors the reference
architecture's separation between "Security admins" (own the KMS
key/DLP template) and "Security analysts" (query de-identified BigQuery
data only) -- except here the "re-identifying" party is a single-purpose
service account on the write-back path, not a human, consistent with this
directory's automation requirement.

## Deployment status

This directory's Cloud Run functions are deployed (see
[`SETUP.md`](../SETUP.md#7-deployment-procedure-production-pipeline-v111-de-identificationre-identification-dormant))
but deliberately left unwired to any live GCS/Pub/Sub trigger -- no merchant
is onboarded onto this pipeline. It exists to demonstrate the pattern is
ready for the volume/queryability threshold described above, at zero
incremental Dataflow/DLP/KMS/BigQuery cost until a merchant actually crosses
it. See `configs/reference.json`, marked non-production for this reason.
