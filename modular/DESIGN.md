# Modular pipeline: design decisions

Weighs a general-purpose "token migration as a service" pipeline against the
CardCorp → Revolut Bank case it's generalized from. Full diagrams and reasoning:
see the architecture review published alongside this work; the verdicts below are
what the code in this directory actually implements.

## Extraction: Dataflow vs. a scheduled Cloud Run puller

**Verdict:** default to a scheduled Cloud Run job per merchant (`extraction_adapters.py`,
`cloud_run_puller`); offer Dataflow (`dataflow`) as an explicit opt-in only for a
merchant whose source volume or shape needs it.

Today's extraction is one GET call a month for roughly 140 rows on average — 2,275
rows across 16 monthly cycles for CardCorp alone. Dataflow's autoscaling worker pool
and windowing model solve problems this pipeline doesn't have yet, at the cost of a
second execution substrate alongside Cloud Run. Dataflow earns its place once a
merchant extracts continuously, or at a volume a single Cloud Run request can't
finish inside one timeout window.

## Staging & transform: Python + Gemini-modifiable config vs. Dataprep

**Verdict:** a declarative per-merchant mapping config (`configs/<merchant>.json`),
proposed by a Gemini agent (`gemini_mapping_agent.py`) and reviewed by a human before
deploy, is the more modular choice. Dataprep, if used at all, is an optional
human-facing profiling step during onboarding — not the pipeline's execution engine.

A Gemini agent can read a declarative config the same way it reads code: shown
current fields and sample rows, it proposes a diff, and a human approves before it's
live. That keeps the transform git-diffable, testable, and deployable through
ordinary CI/CD. Dataprep's recipes live in a proprietary flow format that isn't
naturally something an LLM agent edits or a pull request reviews, and Google's
investment in Dataprep has visibly slowed in recent years.

## Reconciliation store: Firestore vs. SQL

**Verdict:** Firestore stays the default (`store_adapters.py`, `firestore`),
namespaced per merchant per period (`<merchant>_MMYYYY_<prefix>`). SQL
(`sql`) is an explicit, unimplemented placeholder until a specific merchant's
downstream systems need joins or reporting Firestore can't do.

Reconciliation is a single-field lookup by key, edited by a human through a UI —
exactly what Firestore's own Console already gives for free, with no VM, IAP
tunnel, or password file to manage.

## Open decisions

Tracked as data, not just prose — see `open_decisions.py`.
