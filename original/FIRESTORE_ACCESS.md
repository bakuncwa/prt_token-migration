# Viewing/editing the transformed data (Firestore)

Alternative to `DATABASE_ACCESS.md` (Postgres + Adminer): the same
transformed dataset loaded into Firestore Native mode instead. No VM, no
IAP tunnel, no password to manage -- browse/edit directly in the GCP
Console with your normal Google login.

- **Database**: `(default)`, Native mode, location `europe-west2`
  (project `cardcorp-token-migration`), created with `freeTier: true`.
- **Collection**: `MMYYYY_cardholders`, one per month -- e.g. May 2025's
  data lives in `052025_cardholders`. Derived from the transformed
  filename by `collection_naming.py`, so re-loading an old month never
  collides with the current one. One document per row, document ID =
  the row's `card.id`, so re-running the loader overwrites/upserts
  existing rows within that month's collection instead of duplicating
  them.
- **What's actually in each document**: only one field, `card_number` --
  every other column (name, address, transaction metadata, ...) is
  intentionally never sent to Firestore at all; it stays in the
  transformed CSV in GCS. `card.number` is the one field the automated
  transform can never populate (the raw PAN is blank in the source), so
  this is the only thing a PaaS worker needs to see or enter here. The
  full reconciled record is reassembled at export time in
  `export_firestore_to_gcs.py` by merging this value back into the
  transformed CSV by `card.id` -- see `merge_card_numbers()` there.
- **Cost**: Firestore's Always Free daily quota is 1 GiB storage / 50K
  reads / 20K writes / 20K deletes -- 24 rows and manual browsing is
  nowhere close to that. $0/month, no time limit, no VM to leave running.

## Open the web UI (Cloud Console)

Go to (replace `052025_cardholders` with the month you want):

https://console.cloud.google.com/firestore/databases/-default-/data/panel/052025_cardholders?project=cardcorp-token-migration

Log in with a Google account that has IAM access to the
`cardcorp-token-migration` project (Owner/Editor, or at minimum
`roles/datastore.user`). Click any document to edit its fields inline,
or use "Add document" to create a new one.

## Reload from a fresh transformed CSV

1. Run `transform_load_gcs.py` to (re)produce the transformed CSV in
   GCS, if the raw source changed.
2. Run the loader:

   ```
   GCS_BUCKET=cardcorp-token-0dc1f93138 python3 load_to_firestore.py
   ```

Because documents are keyed by `card.id`, this **upserts**: rows whose
`card.id` still exists get their `card_number` overwritten with the
transformed CSV's value (any manually-entered card number for that row
is lost), but documents for `card.id`s no longer in the CSV are left in
place rather than deleted. If you need old rows removed on reload too,
ask and the script can be changed to a delete-then-load pass instead.

## Firestore vs. Postgres/Adminer (`DATABASE_ACCESS.md`)

| | Firestore | Postgres + Adminer |
|---|---|---|
| Maintenance | none (managed) | you own the VM |
| Access | Cloud Console, normal login | IAP tunnel + DB password |
| Cost | $0, no time/region limit | $0, `e2-micro` Always Free |
| Data model | NoSQL documents | relational table, SQL |
| Editing | per-document/field | spreadsheet-like grid |

Both are loaded from the same transformed CSV in GCS and can be kept
side by side, or you can drop one once you decide which fits better.
