# Full Text Search (FTS5) Integration Plan

This document describes how FTS5 integrates with the import pipeline, leveraging the CompanyDocuments JSON cache as its source.

## Goal

Find Enterprises by free text. EnterpriseNumber is the unique key for results. Even if matches come from establishment/branch data, the result maps to the owning Enterprise.

Primary ranking objective
- Prioritize matches on company name and city (any language), then postal code, then other fields.

## What we index (for matching)

- Company names (legal, commercial, branch names; collapsed to the Enterprise)
- Address (street, city, postal code; NL/FR; collapsed to the Enterprise)
- Activity descriptions (NACE; NL/FR/DE/EN; collapsed to the Enterprise)

## Design

- CompanyDocuments is the single source of truth for both the API and FTS.
- Contentless FTS5 table that stores only indexed text columns.
- Sidecar mapping table that stores rowid → EnterpriseNumber.
- During rebuild, read from CompanyDocuments and insert:
  1) EnterpriseNumber into the mapping table ordered by EnterpriseNumber
  2) The corresponding text columns into the FTS table using the same ordering

This keeps FTS rowids aligned with EnterpriseNumber without duplicating content or scanning many relational tables at rebuild time.

## Schema (virtual tables)

```sql
-- Mapping table for rowid → EnterpriseNumber
CREATE TABLE IF NOT EXISTS companies_locations_doc (
  rowid INTEGER PRIMARY KEY,   -- must match FTS rowid sequence
  enterprise_number TEXT NOT NULL UNIQUE
);

-- Contentless FTS with only the indexed text columns
CREATE VIRTUAL TABLE IF NOT EXISTS companies_locations_fts USING fts5(
  company_name,        -- prioritize
  commercial_name,     -- prioritize (lower than legal name)
  street_nl,
  street_fr,
  city_nl,             -- prioritize
  city_fr,             -- prioritize
  postal_code,         -- prioritize (lower than city)
  activity_desc_nl,
  activity_desc_fr,
  activity_desc_de,
  activity_desc_en,
  content='',
  tokenize = "unicode61 remove_diacritics 2 tokenchars '.-/'",
  prefix = '2 3 4' -- prefix search primarily benefits names
);
```

## Rebuild strategy (post-import)

- Rebuild after import in a single transaction.
- Use DROP TABLE + CREATE TABLE/CREATE VIRTUAL TABLE (faster than DELETE for large tables).
- Use PRAGMAs for speed during rebuild.

Recommended PRAGMAs:

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = -50000;  -- ~50MB page cache (negative is KB)
```

### Rebuild from CompanyDocuments

If `fts.*` exists inside payload (preferred):

```sql
BEGIN IMMEDIATE TRANSACTION;
DROP TABLE IF EXISTS companies_locations_doc;
CREATE TABLE companies_locations_doc (rowid INTEGER PRIMARY KEY, enterprise_number TEXT NOT NULL UNIQUE);
DROP TABLE IF EXISTS companies_locations_fts;
CREATE VIRTUAL TABLE companies_locations_fts USING fts5(
  company_name, commercial_name, street_nl, street_fr, city_nl, city_fr, postal_code,
  activity_desc_nl, activity_desc_fr, activity_desc_de, activity_desc_en,
  content='', tokenize = 'unicode61 remove_diacritics 2 tokenchars ''./''', prefix='2 3 4'
);

INSERT INTO companies_locations_doc(enterprise_number)
SELECT EnterpriseNumber FROM CompanyDocuments ORDER BY EnterpriseNumber;

INSERT INTO companies_locations_fts (
  company_name, commercial_name, street_nl, street_fr, city_nl, city_fr, postal_code,
  activity_desc_nl, activity_desc_fr, activity_desc_de, activity_desc_en
)
SELECT
  json_extract(Payload,'$.fts.companyName'),
  json_extract(Payload,'$.fts.commercialName'),
  json_extract(Payload,'$.fts.street.nl'), json_extract(Payload,'$.fts.street.fr'),
  json_extract(Payload,'$.fts.city.nl'),   json_extract(Payload,'$.fts.city.fr'),
  json_extract(Payload,'$.fts.postalCode'),
  json_extract(Payload,'$.fts.activity.nl'), json_extract(Payload,'$.fts.activity.fr'),
  json_extract(Payload,'$.fts.activity.de'), json_extract(Payload,'$.fts.activity.en')
FROM CompanyDocuments ORDER BY EnterpriseNumber;
COMMIT;
```

If `fts.*` is not present (fallback):
- Use the legacy relational aggregation CTE from the previous version, or extend the JSON to derive text on the fly with json_each (slower).

## Incremental updates

- Maintain a set S of impacted EnterpriseNumbers during incremental import (resolve from child IDs where needed).
- For S:
  - Upsert CompanyDocuments for each EnterpriseNumber in S.
  - In a single transaction: delete + reinsert corresponding rows in companies_locations_doc and companies_locations_fts from JSON (same SELECT as above with a WHERE EnterpriseNumber IN (SELECT ...) filter).
- This avoids full rebuilds and keeps FTS consistent with CompanyDocuments.

## Querying unstructured input (language-agnostic)

Goal: Accept verbatim, noisy text (unknown format), prioritize company name and city without language detection.

1) Tokenization (generic, safe)
- Normalize to lowercase (diacritics removed by tokenizer).
- Extract tokens with the same policy as the index: letters/digits plus . - /
- Keep tokens length ≥ 2; cap to N distinct tokens (e.g., N=12) to keep MATCH small.
- Keep 4-digit numeric tokens (likely BE postal codes). Drop very long digit runs (≥7) to avoid card/IBAN noise.

2) MATCH building (no language decision)
- Use an unscoped MATCH that searches all columns; rely on bm25 column weights for prioritization.
- Apply prefix only to alphabetic tokens (benefits names with prefix='2 3 4'); do not add '*' to numeric tokens like postal codes.
- Resulting MATCH example for tokens t1..tn: "(t1* OR t2* OR ... OR tn)" with '*' only on alpha tokens.

3) Ranking and prioritization
- Use bm25 with per-column weights to bias name and city highest, postal next, then street, then activities:

```sql
SELECT d.enterprise_number AS EnterpriseNumber,
       bm25(f,
         5.0,  -- company_name
         3.0,  -- commercial_name
         1.0,  -- street_nl
         1.0,  -- street_fr
         4.0,  -- city_nl
         4.0,  -- city_fr
         2.5,  -- postal_code
         0.5,  -- activity_desc_nl
         0.5,  -- activity_desc_fr
         0.5,  -- activity_desc_de
         0.5   -- activity_desc_en
       ) AS score
FROM companies_locations_fts f
JOIN companies_locations_doc d ON d.rowid = f.rowid
WHERE f MATCH @match
ORDER BY score
LIMIT 25;
```

Notes
- Unscoped MATCH searches all columns; weights enforce the priority (name > city > postal > street > activity).
- If you later add merged all-language columns (e.g., city), adjust the weights accordingly and scope to fewer columns.

4) Example (verbatim noisy input)
- Input: "Betaling Bancontact 29/03/25 - 10.41 uur - ALBERT HEIJN 3132 HASS 3511 - HASSELT - BEL Kaartnummer 5229 62XX XXXX 0390"
- Tokens after generic rules: ["albert","heijn","3511","hasselt"]
- MATCH (unscoped, prefix on alpha): "(albert* OR heijn* OR 3511 OR hasselt)"
- Weighted ranking favors name and city hits; postal_code contributes secondary signal.

5) Practical caps and fallbacks
- Cap tokens (e.g., 12). If empty after filtering, return no results or a safe default.
- Optional light re-rank in app: add small bonuses if both a name token and a city token matched.

## Querying pattern

```sql
SELECT d.enterprise_number AS EnterpriseNumber,
       bm25(f, 5.0,3.0,1.0,1.0,4.0,4.0,2.5,0.5,0.5,0.5,0.5) AS score
FROM companies_locations_fts f
JOIN companies_locations_doc d ON d.rowid = f.rowid
WHERE f MATCH @match
ORDER BY score
LIMIT 25;
```

## References
- specs/company-json-cache.md for JSON schema and build details.
- specs/pipeline-overview.md for full pipeline.
