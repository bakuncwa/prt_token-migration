"""Gemini-assisted authoring of configs/<merchant>.json -- the CLI/Cloud
Shell-prompted step for onboarding a new merchant, per DESIGN.md's
"staging & transform" verdict: a Gemini agent reads a declarative
mapping config the same way it reads code, proposing a diff from a
merchant's field list and sample rows rather than editing a proprietary
Dataprep recipe an LLM (or a pull request) can't easily review.

Placeholder: no live Gemini/Vertex AI API access is configured in this
environment, and -- per open_decisions.py's "gemini_review_gate" item --
the review gate itself (where the diff surfaces, who signs off) isn't
decided yet. So this intentionally stops at *printing* a proposed
config rather than writing configs/<merchant>.json directly: nothing
should reach the live pipeline without a human approving it somewhere,
and "somewhere" isn't built yet.

Intended usage once wired up (Cloud Shell or local CLI):
  python gemini_mapping_agent.py propose --merchant newmerchant \\
    --sample raw/newmerchant/sample.csv --target-schema card.id,card.number,...
"""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd

from open_decisions import require_resolved


def describe_fields(sample_df: pd.DataFrame, n: int = 5) -> dict:
    """What actually gets handed to the Gemini agent: current field
    names plus a small sample of real values per field -- exactly the
    "lists current fields and sample data" input the design calls for,
    nothing more (no full dataset, to keep prompt payloads small and
    avoid sending more PII than the mapping step needs)."""
    return {col: sample_df[col].head(n).tolist() for col in sample_df.columns}


def propose_mapping_config(merchant: str, sample_df: pd.DataFrame, target_schema: list[str]) -> dict:
    """Placeholder for the actual Gemini call. Replace the body with a
    real prompt (field names + describe_fields() output + target_schema)
    against the Gemini/Vertex AI API once access is configured -- the
    response should be validated against configs/cardcorp.json's schema
    shape before it's ever shown as a proposal, let alone approved.
    """
    raise SystemExit(
        f"gemini_mapping_agent.py has no live Gemini API access configured -- "
        f"can't propose a mapping for merchant {merchant!r} yet. "
        f"Fields seen: {list(sample_df.columns)}. "
        f"Target schema: {target_schema}."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    propose = sub.add_parser("propose", help="Propose a new merchant's mapping config")
    propose.add_argument("--merchant", required=True)
    propose.add_argument("--sample", required=True, help="Path to a local sample CSV")
    propose.add_argument("--target-schema", required=True, help="Comma-separated target field names")

    args = parser.parse_args()

    # Fails loudly rather than silently proceeding without a review gate --
    # see open_decisions.py.
    require_resolved("gemini_review_gate")

    sample_df = pd.read_csv(args.sample, dtype=str, keep_default_na=False)
    target_schema = args.target_schema.split(",")
    proposed = propose_mapping_config(args.merchant, sample_df, target_schema)
    print(json.dumps(proposed, indent=2))


if __name__ == "__main__":
    try:
        main()
    except SystemExit as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
