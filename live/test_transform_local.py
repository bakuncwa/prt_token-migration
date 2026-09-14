"""Local dry run of the Token Migration transformation procedure -- no
GCS involvement.

Reads a raw CSV from disk, applies the identical rename/split rules
employed by transform_load_gcs.py (column_mapping.transform_dataframe),
and writes the transformed CSV to disk. Useful for verifying the
transformation procedure prior to the provisioning of any cloud
resources.

Usage:
  python test_transform_local.py [raw_csv_path] [output_csv_path]
"""

from __future__ import annotations

import sys

import pandas as pd

from column_mapping import transform_dataframe

DEFAULT_RAW = "../May 2025.csv"
DEFAULT_OUT = "../Cleaned_May_2025_test_output.csv"


def main() -> None:
    raw_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RAW
    out_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT

    raw_df = pd.read_csv(raw_path, dtype=str, keep_default_na=False)
    print(f"Read {len(raw_df)} rows, {len(raw_df.columns)} columns from {raw_path}")

    transformed_df = transform_dataframe(raw_df)
    print(f"Transformed to {len(transformed_df)} rows, {len(transformed_df.columns)} columns")

    transformed_df.to_csv(out_path, index=False)
    print(f"Wrote transformed CSV to {out_path}")


if __name__ == "__main__":
    main()
