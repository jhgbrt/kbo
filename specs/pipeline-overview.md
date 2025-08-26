# Pipeline Overview

This document outlines the end-to-end data pipeline for the KBO dataset, from CSV publication to API serving, including full and incremental imports, the Company JSON cache, and Full-Text Search (FTS).

Key constraints
- KBO API is read-only for clients; writes only occur during import.
- Offline import is acceptable. Two strategies:
  - Short downtime window (e.g., <= 15 minutes) while importing into the live DB.
  - Blue/green switch: build a new DB file offline, then switch the API connection atomically to the new file.
- SQLite is the persistence engine. Swapping DB files is a supported deployment pattern.

Data artifacts
- Relational tables (imported from CSV).
- CompanyDocuments: one JSON payload per enterprise (source of truth for read-projection and for FTS).
- FTS5 virtual table + mapping table for free-text search (derived from CompanyDocuments).

Full import pipeline
1) Create empty database
   - Ensure schema is present (EF migrations or bootstrap SQL).
2) Import CSV files
   - Bulk-insert into relational tables.
   - Apply PRAGMAs for speed (WAL, synchronous=NORMAL, temp_store=MEMORY, tuned cache_size).
3) Build CompanyDocuments (JSON cache)
   - One row per enterprise.
   - Batch generation to control memory (5k–20k).
   - Compute ETag (SHA-256 hex), JsonVersion, UpdatedAt (UTC), SourceImportId.
   - Upsert via INSERT OR REPLACE.
   - Optional: include pre-flattened FTS fields (fts.*) for fast extraction.
4) Rebuild FTS5 from CompanyDocuments
   - Drop+create contentless FTS + sidecar mapping table.
   - Populate mapping (ordered by EnterpriseNumber) and FTS columns from JSON.
   - Tokenizer: unicode61 remove_diacritics 2; tokenchars '.-/' ; prefix '2 3 4' (prefix helps name queries).

Incremental import pipeline
- Goal: update only affected enterprises and avoid full rebuilds where possible.
1) Apply incremental CSVs
   - Delete + insert changed rows per table.
   - Track impacted enterprise keys S across tables.
     - If only child IDs are known (EstablishmentNumber, Branch Id), resolve to owning EnterpriseNumber via JOINs.
2) Update CompanyDocuments for S only
   - Recompose payloads for impacted enterprises; upsert them.
   - Optionally delete orphans if needed.
3) Update FTS for S only
   - Use the same JSON-driven logic, filtered by the impacted set S.
   - In a single transaction, delete + reinsert FTS/doc rows for S (preserving mapping consistency).

Query approach for unstructured text
- Input is verbatim and may be noisy (unknown language/format).
- Tokenization (app-side):
  - Lowercase; extract tokens matching the FTS policy (letters/digits plus .-/), keep length ? 2.
  - Keep 4-digit numbers (postal codes). Drop long digit runs (?7) to avoid card/IBAN noise.
  - Cap to N tokens (e.g., 12).
- MATCH building:
  - Unscoped MATCH across all columns; apply '*' only to alphabetic tokens (prefix helps names), not to numerics.
- Ranking:
  - Use bm25 column weights to prioritize: company_name > city_* > postal_code > street_* > activity_desc_*.
  - Example weights: (5.0, 3.0, 1.0, 1.0, 4.0, 4.0, 2.5, 0.5, 0.5, 0.5, 0.5).

Why build FTS from CompanyDocuments?
- Single projection step (CompanyDocuments) becomes the source of truth for both the API and FTS.
- Simplifies FTS rebuild (straight json_extract calls).
- Makes incremental FTS straightforward: reuse the same enterprise-level JSON.

Recommended JSON additions for FTS
- Optionally pre-compute flattened text fields under a dedicated object (for fast extraction):
  - payload.fts.companyName
  - payload.fts.commercialName
  - payload.fts.street.nl / payload.fts.street.fr
  - payload.fts.city.nl / payload.fts.city.fr
  - payload.fts.postalCode
  - payload.fts.activity.nl / .fr / .de / .en

Operational notes
- Keep supporting B-tree indexes on hot columns (EntityNumber, CodeId, etc.).
- Wrap FTS rebuild/updates in a single transaction; use PRAGMAs for speed.
- For blue/green: produce the full DB file offline; an atomic file swap flips the API instantly.

References
- See specs/company-json-cache.md for JSON schema and build details.
- See specs/fts5-plan.md for FTS schema, tokenization, matching, and JSON-driven rebuild/update strategies.
