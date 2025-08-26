# Spec: Company JSON cache materialized by EnterpriseNumber

Purpose
- Speed up "get all company info by EnterpriseNumber (KboNr)" by serving a prebuilt JSON document instead of a large EF query with many joins.
- Build and maintain this JSON as part of the import pipeline.
- Act as the single source of truth for FTS (FTS is built from this JSON).

In scope
- SQLite, .NET 9, EF Core.
- Import path produces/refreshes a cache table with one row per enterprise.
- Read path (API) loads JSON by EnterpriseNumber; optionally projects it to requested language.
- FTS rebuild and incremental updates read from this JSON (no heavy joins at FTS time).

Out of scope
- External search (covered by FTS5 plan).
- Real-time change tracking beyond import batches.

Data model
- Table name: CompanyDocuments
- One row per enterprise.

DDL (SQLite)
- Create once at import start if not exists.

```sql
CREATE TABLE IF NOT EXISTS CompanyDocuments (
  EnterpriseNumber TEXT PRIMARY KEY NOT NULL,
  Payload          TEXT NOT NULL CHECK (json_valid(Payload)), -- UTF-8 JSON
  JsonVersion      INTEGER NOT NULL,                          -- schema version
  ETag             TEXT NOT NULL,                             -- SHA-256 hex of Payload
  UpdatedAt        TEXT NOT NULL,                             -- ISO-8601 UTC
  SourceImportId   TEXT NULL                                  -- import batch id
);
CREATE INDEX IF NOT EXISTS IX_CompanyDocuments_UpdatedAt ON CompanyDocuments(UpdatedAt);
```

JSON payload contract (v1)
- Language-independent document containing all known facts for the enterprise and related entities.
- Code values are normalized with multilingual descriptions to allow projection at read-time.
- Optional: flattened fields for FTS under `fts` to speed FTS rebuild/updates.

Top-level shape (v1)
```json
{
  "enterpriseNumber": "0476.893.174",
  "startDate": "YYYY-MM-DD",
  "juridicalForm": { "id": 0, "category": "JuridicalForm", "code": "...", "descriptions": {"NL":"...","FR":"...","EN":"...","DE":"..."} },
  "juridicalFormCAC": { "id": 0, "category": "JuridicalForm", "code": "...", "descriptions": {"NL":"...","FR":"...","EN":"...","DE":"..."} },
  "juridicalSituation": { "id": 0, "category": "JuridicalSituation", "code": "...", "descriptions": {"NL":"...","FR":"...","EN":"...","DE":"..."} },
  "typeOfEnterprise": { "id": 0, "category": "TypeOfEnterprise", "code": "...", "descriptions": {"NL":"...","FR":"...","EN":"...","DE":"..."} },

  "denominations": [ /* ... */ ],
  "addresses":     [ /* ... */ ],
  "contacts":      [ /* ... */ ],
  "activities":    [ /* ... */ ],
  "establishments": [ { "establishmentNumber": "x", "startDate": "YYYY-MM-DD" } ],
  "branches":       [ { "id": "x", "startDate": "YYYY-MM-DD" } ],

  "fts": {
    "companyName": "...",
    "commercialName": "...",
    "street": { "nl": "...", "fr": "..." },
    "city":   { "nl": "...", "fr": "..." },
    "postalCode": "...",
    "activity": { "nl": "...", "fr": "...", "de": "...", "en": "..." }
  }
}
```

Notes
- Keep all multilingual descriptions so API can project per requested language without refetching DB.
- For API compatibility, a lightweight projection can map this JSON to the current Company DTO (picking names/descriptions using the requested language fallback).
- The `fts` object is optional but recommended to streamline FTS rebuild/update using json_extract.

Build strategy (import-time)
- Strategy v1: full rebuild after a successful import (simple and deterministic). Incremental updates can follow later.
- Steps
  1) Create table if needed.
  2) Generate payloads in batches (e.g., 5k–20k enterprises per batch) to control memory.
  3) For each enterprise, compose the JSON document by aggregating related rows:
     - Enterprises (one)
     - Establishments, Branches
     - Addresses, Denominations, Contacts, Activities (joined via EnterpriseNumber and mapped IDs for establishments/branches)
     - Codes + CodeDescription for all involved code IDs
     - Optional: compute and add `fts.*` fields based on the aggregated data
  4) Compute ETag = SHA-256 of Payload (hex). UpdatedAt = now (UTC). JsonVersion = 1. SourceImportId = current import id.
  5) Upsert row: INSERT OR REPLACE INTO CompanyDocuments(...).
  6) After completion, delete orphans: rows in CompanyDocuments whose EnterpriseNumber no longer exists in Enterprises.

Implementation options
- SQL-first using SQLite JSON1 (fastest, less app CPU):
  - Use CTEs to pre-aggregate per enterprise
  - Build nested structures with json_object/json_array/json_group_array
  - Optionally produce `fts` sub-object fields as part of the payload CTE
- App-first (C#/EF) aggregator (simpler to implement):
  - Query necessary sets and group them in memory by enterprise.
  - Use System.Text.Json to write the exact v1 shape (ordered properties for deterministic ETag).
  - Upsert in bulk with parameterized INSERT OR REPLACE.

FTS coupling
- Full rebuild: populate FTS from CompanyDocuments (prefer `fts.*` path if present, else derive on the fly).
- Incremental: recompute JSON for impacted enterprises and update FTS for the same set within a single transaction.

ETag and versioning
- JsonVersion starts at 1. Bump when shape changes.
- ETag computed from JSON text (WriteIndented=false, deterministic property order) to ensure stable hash.

Operational considerations
- Rebuild cost dominated by aggregation of child tables; indexes on EntityNumber mitigate.
- Batch size tuning and single-writer transactions for responsiveness.
- Blue/green: build a complete DB (relational + CompanyDocuments + FTS) offline and swap files atomically.

Validation
- Import step validates json_valid(Payload) and ensures arrays/objects are present where expected.
- CLI command to dump a CompanyDocuments row by EnterpriseNumber and validate against live EF aggregation.

Backfill and cleanup
- After a successful import, run a full rebuild (phase 1).
- Delete stale rows: DELETE FROM CompanyDocuments WHERE EnterpriseNumber NOT IN (SELECT EnterpriseNumber FROM Enterprises).

See also
- specs/pipeline-overview.md for the full pipeline.
- specs/fts5-plan.md for FTS schema and rebuild details.
