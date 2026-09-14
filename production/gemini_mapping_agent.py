"""Gemini-assisted authorship of configs/<merchant>.json -- the
CLI/Cloud Shell-prompted procedure for onboarding a new merchant, per
DESIGN.md's "staging and transformation" verdict: a Gemini agent
interprets a declarative mapping configuration in substantially the
same manner as it interprets source code, proposing a differential
from a merchant's field list and representative sample rows, rather
than requiring modification of a proprietary Dataprep recipe that a
large language model agent (or a pull request) cannot readily review.

Authenticates via GEMINI_API_KEY. Per open_decisions.py's
"gemini_review_gate" item, the review gate itself (where the proposed
differential surfaces, and who is authorized to approve it) has not
yet been determined; accordingly, this module confines itself to
*printing* the proposed configuration rather than writing
configs/<merchant>.json directly -- nothing reaches the live pipeline
absent human approval through a mechanism not yet constructed. This
gate constitutes a governance decision, independent of whether the
Gemini invocation itself is operative.

Usage (Cloud Shell or local CLI):
  python gemini_mapping_agent.py propose --merchant <MERCHANT_ID> \\
    --sample raw/<MERCHANT_ID>/sample.csv --target-schema card.id,card.number,...
"""

from __future__ import annotations

import argparse
import json
import sys

import google.generativeai as genai
import pandas as pd

from merchant_config import env
from open_decisions import require_resolved

MODEL_NAME = "gemini-1.5-flash"


def describe_fields(sample_df: pd.DataFrame, n: int = 5) -> dict:
    """Constitutes the actual payload transmitted to the Gemini agent:
    current field names together with a small sample of representative
    values per field -- precisely the "lists current fields and sample
    data" input the design specifies, and nothing further (the
    complete dataset is deliberately excluded, both to constrain prompt
    payload size and to avoid transmitting more personally identifiable
    information than the mapping procedure requires)."""
    return {col: sample_df[col].head(n).tolist() for col in sample_df.columns}


def propose_mapping_config(merchant: str, sample_df: pd.DataFrame, target_schema: list[str]) -> dict:
    """Prompts Gemini with the merchant's current field names, a sample
    of representative values per field (describe_fields()), and the
    target schema, requesting a mapping in the configs/<merchant>.json
    schema (see configs/pilot.json for the reference structure) as a
    JSON response. The response is parsed and returned strictly as a
    proposal; see this module's docstring for the rationale behind
    main() never writing it to disk automatically."""
    genai.configure(api_key=env("GEMINI_API_KEY", required=True))
    model = genai.GenerativeModel(MODEL_NAME)

    prompt = (
        "Propose a mapping configuration for the token migration staging "
        "service, in the exact JSON schema employed by "
        "configs/<merchant>.json (renames, expansions of type "
        "copy_then_blank or split_date, repairs of type "
        "zero_pad_if_length). Given the following source fields and "
        f"representative values: {json.dumps(describe_fields(sample_df))}. "
        f"Target schema fields: {target_schema}. Respond with the JSON "
        "configuration exclusively, without surrounding prose."
    )
    response = model.generate_content(prompt)
    return json.loads(response.text)


def main() -> None:
    """Parses command-line arguments and executes the propose
    subcommand, subject to the governance gate described in this
    module's docstring."""
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    propose = sub.add_parser("propose", help="Propose a mapping configuration for a new merchant")
    propose.add_argument("--merchant", required=True)
    propose.add_argument("--sample", required=True, help="Path to a local sample CSV")
    propose.add_argument("--target-schema", required=True, help="Comma-separated target field names")

    args = parser.parse_args()

    # Raises explicitly rather than proceeding silently absent a review
    # gate -- see open_decisions.py.
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
