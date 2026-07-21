# Vector store consolidation — ChromaDB vs Postgres/pgvector

**Status:** investigation complete, no code changed. Written 2026-07-21.

**Headline:** the premise of audit finding #1 is wrong. There is **one** live embedding
store (Postgres/pgvector). ChromaDB is not a dependency, is not imported anywhere, and
cannot be queried by any code path in this repo. What actually exists is a **32 MB
git-tracked corpse** of a pre-migration Chroma index plus three identifier names that
still say "chroma". The consolidation already happened; the cleanup did not.

---

## 1. The three "Chroma" references — all naming vestiges, zero Chroma usage

| Reference | What it actually is | Live? |
|---|---|---|
| `backend/utils/field_clean.py:693` `build_inventory_chroma_document()` | A plain string builder. Returns a labeled text summary of a car. No Chroma involvement. | **Live**, but only as a fallback (below) |
| `backend/vector/listings_semantic.py:185-187` | Imports `build_inventory_chroma_document` as the fallback when the prose document comes out empty. | **Live** |
| `backend/vector/ingest_master_specs.py:257` `row_chroma_metadata()` | Builds the JSONB metadata dict. Its own docstring says "Row metadata stored as JSONB". Writes to Postgres. | **Live** |

Evidence that no Chroma client exists:

```
$ grep -rn "chromadb\|import chroma\|PersistentClient" --include="*.py" .   # → no hits
$ grep -i chroma requirements.txt                                          # → no hits
$ python -c "import chromadb"                                              # → ModuleNotFoundError
```

The repo already knows this. `scripts/security_check.sh:57`:

```
# chromadb: not in requirements.txt / app code (pgvector only); ignore orphan venv installs (CVE-2026-45829)
```

Two more vestiges the audit missed:

- `backend/vector/pgvector_service.py:54-56` — `get_persist_dir()`, docstring
  *"Deprecated alias: was Chroma persist directory"*. Re-exported from
  `backend/vector/__init__.py:3-5`, so it is part of the package's public surface.
- `backend/vector/catalog_service.py:18-21` — `master_catalog_persist_dir()` delegates to
  `vector_data_dir()`. Its only consumer is `ingest_master_specs.py:322,359`, where the
  value is used **solely inside a log message**. The call has a side effect
  (`vector_data_dir()` does `mkdir(parents=True, exist_ok=True)` at
  `pgvector_service.py:50`), so running the ingest creates an empty `./data/vectors/`
  directory that nothing ever writes to.

### Entry points for the live paths

- `build_semantic_listing_document()` → imported at module level by
  `backend/vector/pgvector_service.py:26`, called from `_reindex_listings()` (line 291).
  Reachable from: `python -m backend.vector reindex` (`backend/vector/__main__.py`),
  `backend/scripts/reindex_vectors.py`, and the post-scan reindex hook in
  `backend/dev/routes.py:106-114`.
- `build_inventory_chroma_document()` → only reachable through the
  `is_effectively_empty(text)` fallback at `listings_semantic.py:183-187`, i.e. for cars
  whose every prose segment came out blank.
- `row_chroma_metadata()` → `ingest_master_specs.ingest()`, entry point
  `python -m backend.vector.ingest_master_specs --reindex`. **Manual CLI only** — no cron,
  no route, no scanner call. The scanner only *checks* whether the resulting table is
  populated (`backend/scanner/post_scan/pipeline.py:570-580`) and skips enrichment with a
  log line if it is not.

---

## 2. Do the two stores index the same corpus? — Yes, and that is the point

`ingest_master_specs.py` does **not** write to Chroma. Lines 318-343:

```python
from backend.vector.pgvector_service import master_spec_truncate, master_spec_upsert_batch
...
master_spec_upsert_batch(batch_ids, batch_docs, batch_meta, vectors)
```

It downloads EPA `vehicles.csv`, filters model years 2024-2026, dedupes to one "standard
config" per make/model/year, embeds with `all-MiniLM-L6-v2` (`ingest_master_specs.py:50`),
and upserts into the Postgres table `master_spec_embeddings`
(`pgvector_service.py:40`, `665`). `MasterCatalog` in `catalog_service.py` reads that same
table (`master_catalog_query`, line 59/98) with the same model (`_MODEL_NAME` line 14).

The on-disk Chroma index contains **the same corpus**. Each HNSW segment's
`index_metadata.pickle` is keyed by ids of the form `epa-48305`, `epa-46899`, … which is
exactly the id scheme `ingest_master_specs.py:351` emits
(`rid = f"epa-{r.get('epa_id')}"`). So `backend/vector/chroma_master_catalog/` is a
snapshot of the EPA master spec catalog as it existed *before* the pgvector migration —
same content, same embedding model, same id space, different backend.

pgvector additionally serves five other tables that Chroma never held
(`pgvector_service.py:35-40`): `listing_semantic_embeddings`,
`dealer_semantic_embeddings`, `bmw_normalized_embeddings`, `bmw_partial_embeddings`,
`car_knowledge_embeddings`.

---

## 3. Can they disagree? Is there cross-store fallback?

**No, on both counts.** There is no Chroma client, so nothing can read
`chroma_master_catalog/`; the two indexes cannot be compared, let alone disagree at
runtime. There is no code that queries one store and falls back to the other.

The only fallback in the area is pgvector → SQL, not pgvector → Chroma
(`backend/utils/hybrid_search.py:36-47`): when `PGVECTOR_URL`/`DATABASE_URL` is unset or
`query_cars` raises, `_semantic_car_ids` returns `[]` and the caller degrades to plain SQL
search. `backend/routes/health.py:101-105` likewise treats pgvector as optional
("skipped" when unconfigured).

**However** — there is a real, in-pgvector inconsistency worth recording, and it is the
one thing in this area that can actually produce divergent embeddings. Listing documents
are built by *two different* builders with different shapes:

- `build_semantic_listing_document()` — prose, up to 8k chars, includes packages/options.
- `build_inventory_chroma_document()` — compact `LABEL: value` form.

The second is used for any car whose prose document is effectively empty
(`listings_semantic.py:183-187`), so a small tail of the corpus is embedded in a different
document *style* than the rest, inside the same `listing_semantic_embeddings` table and
the same vector space. That is a recall-quality question, not a two-store question, and it
is out of scope for this consolidation — flagged here so it does not get lost.

---

## 4. Is the Chroma index fresh? — It is frozen and duplicated 4×

`backend/vector/chroma_master_catalog/` — 32 MB, **tracked in git** (21 paths present in
`.git/index`, including `chroma.sqlite3`):

```
chroma.sqlite3                                12,853,248 bytes
5d11cf6a-…/  9d739b88-…/  e25aeb48-…/  fb4ad7cc-…/     4 HNSW segments, 5,028,000 bytes each
```

All four segments hold the **identical set of 3,000 `epa-*` ids** (verified: sorted unique
id lists from all four `index_metadata.pickle` files hash to the same MD5
`d6369f77…`), but the `data_level0.bin` payloads differ. That is four rebuilds of the same
collection left behind by repeated `--reindex` runs, i.e. ~20 MB of the 32 MB is redundant
even by Chroma's own standards.

**When was it last written, and by what?** Filesystem mtimes are all
`Jun 10 15:12:50 2026`, identical to every other file in the checkout — that is the clone
timestamp, not a write. The honest answer is: **it was last written by a version of
`ingest_master_specs.py` that no longer exists in the tree, before the pgvector migration;
the exact date is only recoverable from git history, which I was not permitted to query.**
What *is* certain is that nothing in the current tree can have written it — there is no
chromadb import and the package is not installed.

Why it survived: `.gitignore:24-26` ignores `backend/data/chroma/` and `data/chroma/`, but
this index lives at `backend/vector/chroma_master_catalog/`, which matches neither pattern.
`README.md:82` claims "Local legacy/cache dirs under `data/vectors/` and
`backend/data/chroma/` are ignored" — true, and irrelevant to the directory that actually
got committed.

---

## 5. Recommendation

**Retire ChromaDB. Keep Postgres/pgvector.** This is not a close call — pgvector is the
only store with a client, and the retirement is already 95% done in code. What remains is
deleting a binary artifact and renaming three symbols.

Do **not** treat this as a search-behavior change. Nothing in the removal touches a live
query path, and no reindex is required.

### Removal steps, in dependency order

1. **Delete the artifact and stop it coming back.**
   `git rm -r --cached backend/vector/chroma_master_catalog/` and delete the directory;
   add `backend/vector/chroma_master_catalog/` (or better, a broader `**/chroma*/` rule)
   to `.gitignore` next to the existing block at lines 24-26. Nothing imports it, so this
   is safe to do first and independently of everything below.
   *Note: history rewrite is a separate decision — see risks.*

2. **Rename `build_inventory_chroma_document` → e.g. `build_compact_listing_document`.**
   Two sites: definition `backend/utils/field_clean.py:693`, call
   `backend/vector/listings_semantic.py:185-187`. No tests reference it
   (`grep -rl chroma backend/tests/` → empty).

3. **Rename `row_chroma_metadata` → e.g. `row_catalog_metadata`.**
   Two sites, both in `backend/vector/ingest_master_specs.py` (definition 257, call 350).

4. **Delete `get_persist_dir()`** (`pgvector_service.py:54-56`) and remove it from
   `backend/vector/__init__.py:3-5`. Grep first — it is an exported name, so an out-of-repo
   or notebook caller is conceivable.

5. **Delete `master_catalog_persist_dir()`** (`catalog_service.py:18-21`) and inline the
   log message at `ingest_master_specs.py:322,359`, or drop the path from the log entirely.
   This also removes the pointless `./data/vectors/` mkdir side effect.

6. **Reconsider the blanket CVE ignore.** `scripts/security_check.sh:57-62` passes
   `--ignore-vuln CVE-2026-45829` unconditionally. Once step 1 lands there is nothing
   chroma-shaped in the repo at all; the ignore then only masks a stale venv install, and
   will silently mask a *real* chromadb dependency if one is ever added. Consider dropping
   it, or gating it on an explicit "chromadb is not in requirements.txt" assertion.

7. **Update the docs** (`README.md:82,139`, `docs/SECURITY_MASTER_TODO.md:165-166`,
   `docs/LISTINGS_PGVECTOR_SEARCH.md:20`) to stop describing Chroma as a thing that exists.
   Last, so the docs describe the finished state.

Steps 2-5 are pure renames/deletions with no behavior change and can land as one commit.

---

## 6. Risks and unknowns

**Risks**

- *Deleting the artifact loses the only copy of that pre-migration index.* Rebuilding it
  is not possible with the current code (no Chroma client), and even a pgvector rebuild
  from EPA data would produce a **different** corpus: today's `ingest_master_specs.py`
  targets model years 2024-2026 (`TARGET_YEARS`, line 49) and dedupes to one row per
  make/model/year, whereas the frozen index holds 3,000 docs whose EPA ids cluster in the
  ~46,800-50,000 range. Whether those id sets overlap is **not determined** — I did not
  download EPA data to check. If the old index is ever wanted as a reference, extract what
  is needed *before* deleting.
- *`get_persist_dir` is publicly exported*, so removing it is the one step with a real
  chance of breaking an unseen caller (scripts outside the repo, notebooks, ops runbooks).
- *History rewrite is the expensive option.* `git rm --cached` stops the bleeding but
  leaves 32 MB in every clone forever. Rewriting history to purge it invalidates every
  outstanding branch and PR. Recommend `--cached` now, purge only if repo size becomes a
  real problem.

**Unknowns I could not resolve**

- **Whether `master_spec_embeddings` is currently populated in production.** A scanner job
  was running and I was instructed not to touch Postgres, so I did not run
  `master_spec_catalog_nonempty()`. If it is empty, post-scan enrichment has been silently
  skipping (`pipeline.py:574-580`) and step 1 is unrelated to that — but it would mean the
  EPA catalog has *no* live copy in either store, which changes the urgency of preserving
  the Chroma snapshot.
- **The exact date and commit that last wrote the Chroma index**, and the commit that
  removed the chromadb dependency. Both are in git history, which I was not permitted to
  query. Filesystem mtimes are uniformly the checkout time and carry no information.
- **The contents of `chroma.sqlite3`** (collection names, metadata, document text). I
  deliberately did not open it — the operating constraint for this session was to touch no
  `*.sqlite3` file. All conclusions about the index come from the HNSW sidecar files
  (`index_metadata.pickle`, `length.bin`, byte sizes), which are not sqlite. The four
  segment ids in the directory listing are almost certainly four Chroma collections, but I
  did not confirm their names.
- **Embedding dimension of the frozen index** is inferred, not read: `length.bin` is
  12,000 bytes (4 bytes × 3,000 slots) and `data_level0.bin` is 5,028,000 bytes → 1,676
  bytes per record ≈ 384 floats × 4 bytes + ~140 bytes overhead, consistent with
  `all-MiniLM-L6-v2`. Consistent, not proven.

**Correction to the audit finding as written**

The finding says "two embedding stores (ChromaDB and Postgres/pgvector) doing the same
job." There is only one store doing a job. The correct finding is: *a 32 MB obsolete
Chroma index is committed to git because it sits outside the `.gitignore` chroma patterns,
and three symbols plus two deprecated helpers still carry Chroma-era names.* The measured
"Chroma in 3 files" count is accurate as a grep result but every hit is a string in an
identifier, not a code path.
