"""Column mapping specification for the Token Migration ETL
transformation procedure.

Authoritative source: "Data Transformation_TokenMigration.xlsx" and
the "Token Migration ETL" diagram (Transformed PAN MIT Data block).

Every raw column is preserved in its original position; exclusively
those columns enumerated within the spreadsheet are renamed or split.
Two columns present in the Cleaned_Test1 sample (card.transaction_ids,
card.number) possess no corresponding source mapping within the
spreadsheet -- these are emitted as empty fields rather than
fabricated values, inasmuch as the raw export contains no authentic
token identifier or Primary Account Number (PAN) data for them
(FullAccountNumber is blank in the source data; the derivation of
values from the Bank Identification Number, or BIN, would produce a
misleading, fictitious card number).

AccountNumberLast4 is repaired in place (retained under its raw
identifier, neither renamed nor expanded) should it arrive as exactly
three digits -- see _restore_account_number_last4_leading_zero().
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
    # Email is enumerated within the spreadsheet as Email -> Email
    # (unaltered); no operation is required.
}

# raw column name -> ordered list of new columns into which it expands
EXPANSIONS = {
    "UniqueId": ["card.id", "card.transaction_ids", "card.number"],
    "Expiry": ["card.exp_year", "card.exp_month"],
}


def _split_expiry(value: str) -> tuple[str, str]:
    """The raw format is "YYYY-MM". This performs the split
    explicitly, rather than relying upon spreadsheet auto-formatting
    (which discards the month's leading zero, as observed within the
    Cleaned_Test1 sample)."""
    if isinstance(value, str) and len(value) == 7 and value[4] == "-":
        year, month = value.split("-")
        if year.isdigit() and month.isdigit():
            return year, month.zfill(2)
    return "", ""


def _restore_account_number_last4_leading_zero(value: str) -> str:
    """AccountNumberLast4 is expected to comprise exactly four digits.
    Spreadsheet tools that interpret the raw source as numeric (rather
    than textual) silently discard a leading zero -- "0123" becomes
    "123". A three-character value is the unambiguous signature of
    this occurrence: the discarded zero is restored accordingly. Any
    other length is left unmodified, inasmuch as the missing content
    cannot be reliably inferred (and four-character values are already
    correct)."""
    if isinstance(value, str) and len(value) == 3 and value.isdigit():
        return "0" + value
    return value


def transform_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Applies the raw-to-transformed column rename and split rules,
    preserving column order."""
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
