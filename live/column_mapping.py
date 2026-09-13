"""Column mapping for the Token Migration ETL transform.

Source of truth: "Data Transformation_TokenMigration.xlsx" and the
"Token Migration ETL" diagram (Transformed PAN MIT Data block).

Every raw column is preserved in its original position; only the columns
listed in the xlsx are renamed or split. Two columns present in the
Cleaned_Test1 sample (card.transaction_ids, card.number) have no source
mapping in the xlsx -- they are emitted as empty rather than fabricated,
since the raw export contains no real token id or PAN data for them
(FullAccountNumber is blank in the source; inventing values from the BIN
would create a misleading fake card number).

AccountNumberLast4 is repaired in place (still under its raw name, not
renamed/expanded) if it comes through as exactly 3 digits -- see
_restore_account_number_last4_leading_zero().
"""

import pandas as pd

# raw column name -> new column name, for simple one-to-one renames
SIMPLE_RENAMES = {
    "CustomerName": "name",
    "RegistrationId": "id",
    "City": "card.address_city",
    "OPP_card.country": "card.address_country",
    "OPP_billing.street1": "card.address_line1",
    "State": "card.address_state",
    "Zip": "card.address_zip",
    # Email is listed in the xlsx as Email -> Email (unchanged), no-op.
}

# raw column name -> ordered list of new columns it expands into
EXPANSIONS = {
    "UniqueId": ["card.id", "card.transaction_ids", "card.number"],
    "Expiry": ["card.exp_year", "card.exp_month"],
}


def _split_expiry(value: str) -> tuple[str, str]:
    """Raw format is "YYYY-MM". Split properly rather than relying on
    spreadsheet auto-formatting (which drops the MM leading zero, as seen
    in the Cleaned_Test1 sample)."""
    if isinstance(value, str) and len(value) == 7 and value[4] == "-":
        year, month = value.split("-")
        if year.isdigit() and month.isdigit():
            return year, month.zfill(2)
    return "", ""


def _restore_account_number_last4_leading_zero(value: str) -> str:
    """AccountNumberLast4 should always be 4 digits. Spreadsheet tools
    that treat the raw source as a number (rather than text) silently
    drop a leading zero -- "0123" becomes "123". A 3-character value is
    the unambiguous signature of that: restore the dropped zero. Any
    other length is left as-is, since we can't safely infer what's
    missing (and 4-character values are already correct)."""
    if isinstance(value, str) and len(value) == 3 and value.isdigit():
        return "0" + value
    return value


def transform_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Apply the raw -> transformed column rename/split rules, in column order."""
    out = {}
    for raw_col in raw_df.columns:
        series = raw_df[raw_col]
        if raw_col == "UniqueId":
            new_id, txn_ids, number = EXPANSIONS["UniqueId"]
            out[new_id] = series
            out[txn_ids] = ""
            out[number] = ""
        elif raw_col == "Expiry":
            exp_year, exp_month = EXPANSIONS["Expiry"]
            split = series.map(_split_expiry)
            out[exp_year] = split.map(lambda t: t[0])
            out[exp_month] = split.map(lambda t: t[1])
        elif raw_col == "AccountNumberLast4":
            out[raw_col] = series.map(_restore_account_number_last4_leading_zero)
        elif raw_col in SIMPLE_RENAMES:
            out[SIMPLE_RENAMES[raw_col]] = series
        else:
            out[raw_col] = series
    return pd.DataFrame(out)
