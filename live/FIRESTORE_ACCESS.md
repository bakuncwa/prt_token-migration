# Viewing and Editing the Transformed Dataset (Firestore)

This document describes an alternative to `DATABASE_ACCESS.md` (PostgreSQL
with Adminer): the identical transformed dataset, loaded instead into
Firestore Native mode. This approach requires no virtual machine, no
Identity-Aware Proxy (IAP) tunnel, and no password to manage; records may be
browsed and edited directly within the Google Cloud Platform (GCP) Console
using the operator's standard Google credentials.

- **Database:** `(default)`, Native mode, location `europe-west2` (project
  `cardcorp-token-migration`), provisioned with `freeTier: true`.
- **Collection structure:** `MMYYYY_cardholders`, one collection per calendar
  month -- for example, May 2025's dataset resides within
  `052025_cardholders`. The collection identifier is derived from the
  transformed filename by `collection_naming.py`, such that the reloading of
  a prior month's data does not produce a collision with the current month.
  Each document corresponds to a single row, with the document identifier
  equal to that row's `card.id`; consequently, a repeated execution of the
  loader overwrites, or upserts, existing rows within that month's
  collection, rather than duplicating them.
- **Document content:** each document contains exactly one field,
  `card_number`; every other column (name, address, transaction metadata,
  and so forth) is deliberately never transmitted to Firestore, and instead
  remains within the transformed CSV in Google Cloud Storage (GCS).
  `card.number` constitutes the sole field that the automated transformation
  procedure cannot populate, inasmuch as the raw Primary Account Number
  (PAN) is blank within the source data; accordingly, it is the only field a
  PaaS worker is required to view or enter within this interface. The
  complete reconciled record is reassembled at export time within
  `export_firestore_to_gcs.py`, through the merging of this value into the
  transformed CSV by `card.id`; see `merge_card_numbers()` therein.
- **Cost:** Firestore's Always Free daily quota comprises 1 GiB of storage
  and 50,000 reads, 20,000 writes, and 20,000 deletes; the present usage of
  24 rows, together with manual browsing, remains substantially below this
  threshold. The resultant cost is \$0.00 per month, subject to no time
  limitation and requiring no virtual machine to remain in operation.

## Accessing the Web Interface (Cloud Console)

Navigate to the following address, substituting `052025_cardholders` with
the calendar month of interest:

https://console.cloud.google.com/firestore/databases/-default-/data/panel/052025_cardholders?project=cardcorp-token-migration

Authentication requires a Google account possessing IAM access to the
`cardcorp-token-migration` project (Owner or Editor role, or, at minimum,
`roles/datastore.user`). Selection of any document permits inline editing of
its fields; the "Add document" control permits the creation of a new
document.

## Reloading From an Updated Transformed CSV

1. Execute `transform_load_gcs.py` to regenerate the transformed CSV within
   GCS, in the event that the raw source data has changed.
2. Execute the loader:

   ```
   GCS_BUCKET=cardcorp-token-0dc1f93138 python3 load_to_firestore.py
   ```

Inasmuch as documents are indexed by `card.id`, this procedure constitutes an
**upsert** operation: rows whose `card.id` remains present have their
`card_number` field overwritten with the transformed CSV's corresponding
value (any manually entered card number for that row is consequently lost),
whereas documents corresponding to a `card.id` no longer present within the
CSV are retained rather than deleted. Should the removal of obsolete rows
upon reload be required, the script may be modified to execute a
delete-then-load procedure instead; such a modification should be requested
explicitly.

## Firestore Versus PostgreSQL/Adminer (`DATABASE_ACCESS.md`)

| | Firestore | PostgreSQL + Adminer |
|---|---|---|
| Maintenance | none required (fully managed) | virtual machine ownership required |
| Access | Cloud Console, standard credentials | IAP tunnel with database password |
| Cost | \$0.00, subject to no time or region limitation | \$0.00, `e2-micro` Always Free tier |
| Data model | NoSQL document store | relational table, accessed via SQL |
| Editing interface | per-document, per-field | spreadsheet-style grid |

Both alternatives are populated from the identical transformed CSV within
GCS and may be maintained concurrently, or one may be discontinued once a
preference has been determined.
