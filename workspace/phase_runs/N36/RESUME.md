# N36 Docs close-out (18B) - RESUME

Branch phase/N36, merged into integ/draft at 409fa55b9 (head 54bc96b66). Draft only; nothing released, pushed or deployed.

| Unit | Head | Status |
|---|---|---|
| P18B.1, P18B.4, P18B.6 | 64929e2ee | DONE |
| P18B.2, P18B.3 | 1436ace1d | DONE |
| P18B.7a | ea79db097 | DONE (CLAUDE.md not edited) |
| P18B.5 | 54bc96b66 | DONE (duplicate SEC IDs not renumbered) |
| P18B.7b | - | blocked: owner approves CLAUDE.md wording |
| N36.8a | - | blocked: needs every earlier N-unit merged |
| N36.8b | - | blocked: owner-run read-only prod measurement |

## Deferred checks
- Migration registry table rebuild from migrations/ (V026-V038 exist)
- D2 "moot after SEC-014" in CLOUD_RESTRUCTURE_PLAN unverified
- pipeline.py module docstring still omits OSM
- Full offline suite not run (4 doc test files: 103 passed)

## Owner asks
- Approve the CLAUDE.md location-logging wording (P18B.7b)
- Run the read-only prod measurement for N36.8b
