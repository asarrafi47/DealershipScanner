# N33 Browser path retirement (17B) - RESUME

Branch phase/N33, merged into integ/draft at 0de0388a7 (head d739c2211). Draft only.

| Unit | Head | Status |
|---|---|---|
| P17B.1, P17B.7 | f3116bb5b | DONE |
| P17B.2 | 3e60586fc | DONE |
| P17B.3 | 0474e6dbb | DONE, scan check deferred |
| P17B.4 | fb1909d9c | DONE |
| P17B.5, P17B.8a | ee3fb7168 | DONE (script only) |
| P17B.6 | 180293b20 | DONE, needs N18/N32 merged |
| P17B.9, P17B.10 | dc821e089 | DONE |
| P17B.11 | d739c2211 | DONE (docs) |
| P17B.8 | - | apply against real DB not run |
| P17B.12 | - | open follow-up, not in unit table |

## Deferred checks
- P17B.3: 5-dealer browser-free scan within +-2% of rows
- P17B.8: dry run then apply of clear_dead_winning_strategies on a local copy
- Next nightly within +-3% of rows; scanner-nightly Docker smoke then deploy
- P17B.6 merge ordering: N18 (P12A.4) and N32 first
- Delete stale __pycache__-only dirs backend/intelligence/llm and agents in any checkout after merge
- README Node section, docs/SCANNER_NODE.md, master-todo C1 (P18B.2)

## Owner asks
- Confirm deleting (not rebuilding) html_next_data and jsonld_listing_html recovery; 10 local dealers had html_next_data cached
- P17B.12: slim discovery's scrape_inventory_path (autowall/shopperexpress browser code kept for discovery)
- Approve Docker smoke and release (minor)
