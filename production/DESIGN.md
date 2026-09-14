# Production Pipeline: Architectural Design Decisions

This document evaluates the merits of a general-purpose "token migration as a
service" pipeline relative to the CardCorp-to-Revolut Bank instance from which
it was generalized. The complete diagrammatic representation and supporting
rationale are published alongside this work; the verdicts articulated below
correspond precisely to the implementation contained within this directory.

## Extraction: Dataflow Versus a Scheduled Cloud Run Puller

**Verdict:** the default configuration for each merchant should be a scheduled
Cloud Run job (`extraction_adapters.py`, `cloud_run_puller`); Dataflow
(`dataflow`) should be offered exclusively as an explicit opt-in, reserved for
a merchant whose data volume or structural characteristics necessitate it.

The extraction procedure, as presently constituted, comprises a single GET
request executed monthly, retrieving approximately 140 rows on average --
2,275 rows in aggregate across 16 monthly cycles for CardCorp alone.
Dataflow's autoscaling worker pool and windowing model address computational
problems that this pipeline does not, at present, possess, at the expense of
introducing a second execution substrate alongside Cloud Run. Dataflow's
adoption becomes justified once a given merchant requires continuous
extraction, or extraction at a volume that a single Cloud Run request cannot
complete within one timeout interval.

## Staging and Transformation: Python With Gemini-Modifiable Configuration Versus Dataprep

**Verdict:** a declarative, per-merchant mapping configuration
(`configs/<merchant>.json`), proposed by a Gemini agent
(`gemini_mapping_agent.py`) and subject to human review prior to deployment,
constitutes the more modular architectural choice. Dataprep, to the extent it
is employed at all, should function exclusively as an optional, human-facing
profiling instrument during merchant onboarding, rather than as the
pipeline's execution engine.

A Gemini agent is capable of interpreting a declarative configuration in
substantially the same manner as it interprets source code: presented with
the current field schema and representative sample rows, the agent proposes
a differential, which a human reviewer subsequently approves prior to
deployment. This arrangement preserves the transformation logic's
compatibility with version control diffing, automated testing, and
deployment via conventional continuous integration and continuous deployment
(CI/CD) infrastructure. Dataprep's recipes, by contrast, are persisted in a
proprietary flow format that is not readily amenable to modification by a
large language model agent, nor to review within the context of a pull
request; furthermore, Google's continued investment in Dataprep has
demonstrably diminished in recent years.

## Reconciliation Store: Firestore Versus SQL

**Verdict:** Firestore should remain the default reconciliation store
(`store_adapters.py`, `firestore`), namespaced per merchant and per period
(`<merchant>_MMYYYY_<prefix>`). A SQL-backed store (`sql`) should remain an
explicit, unimplemented placeholder pending a specific merchant's requirement
for relational joins or reporting capabilities that Firestore cannot provide.

The reconciliation procedure constitutes, in its entirety, a single-field
lookup by key, subject to human editing through a graphical interface --
precisely the functionality that Firestore's own Console provides without
additional cost, and without the operational burden of a virtual machine,
an Identity-Aware Proxy (IAP) tunnel, or a managed password file.

## Outstanding Decisions

These are maintained as structured data rather than as unstructured prose;
see `open_decisions.py`.
