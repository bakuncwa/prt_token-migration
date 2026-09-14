"""Gemini-assisted authoring of configs/<merchant>.json -- the CLI/Cloud
Shell-prompted step for onboarding a new merchant, per DESIGN.md's
"staging & transform" verdict: a Gemini agent reads a declarative
mapping config the same way it reads code, proposing a diff from a
merchant's field list and sample rows rather than editing a proprietary
Dataprep recipe an LLM (or a pull request) cannot easily review.

Authenticates with GEMINI_API_KEY. Per open_decisions.py's
"gemini_review_gate" item, the review gate itself (where the proposed
diff surfaces, who signs off) is not yet decided; accordingly, this
stops at *printing* the proposed config rather than writing
configs/<merchant>.json directly -- nothing reaches the live pipeline
without a human approving it somewhere, and "somewhere" is not built
yet. That gate is a governance decision, independent of whether the
Gemini call itself is live.

Usage (Cloud Shell or local CLI):
  python gemini_mapping_agent.py propose --merchant newmerchant \\
    --sample raw/newmerchant/sample.csv --target-schema card.id,card.number,...
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
    """What actually gets handed to the Gemini agent: current field
    names plus a small sample of real values per field -- exactly the
    "lists current fields and sample data" input the design calls for,
    nothing more (no full dataset, to keep prompt payloads small and
    avoid sending more PII than the mapping step needs)."""
    return {col: sample_df[col].head(n).tolist() for col in sample_df.columns}


def propose_mapping_config(merchant: str, sample_df: pd.DataFrame, target_schema: list[str]) -> dict:
    """Prompts Gemini with the merchant's current field names, a sample
    of real values per field (describe_fields()), and the target
    schema, requesting a configs/<merchant>.json-shaped mapping (see
    configs/cardcorp.json) as a JSON response. The response is parsed
    and returned as a proposal only -- see this module's docstring for
    why main() never writes it to disk automatically."""
    genai.configure(api_key=env("GEMINI_API_KEY", required=True))
    model = genai.GenerativeModel(MODEL_NAME)

    prompt = (
        "Propose a mapping config for the token migration staging service, "
        "in the exact JSON shape used by configs/<merchant>.json (renames, "
        "expansions with type copy_then_blank or split_date, repairs with "
        "type zero_pad_if_length). Given these source fields and sample "
        f"values: {json.dumps(describe_fields(sample_df))}. Target schema "
        f"fields: {target_schema}. Respond with only the JSON config, no "
        "surrounding prose."
    )
    response = model.generate_content(prompt)
    return json.loads(response.text)


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
