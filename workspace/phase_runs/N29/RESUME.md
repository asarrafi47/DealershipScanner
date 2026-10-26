# N29 Dependencies and images - RESUME

Branch phase/N29, merged into integ/draft at acef236ca (no conflicts). Draft only; no Docker builds, no CI.

| Unit | Head | Status |
|---|---|---|
| P15B.2 | 86723b2e2 | DONE (static) |
| P15B.3 | 40c8b47a8 | PARTIAL (revision hash empty) |
| P15B.4 | 77928917f | DONE (static) |
| P15B.5 | 77928917f | DONE (static) |
| P15B.7 | 235e50c0d | DONE (static) |
| P15B.8 | - | not drafted; only if D-SEC7 = (a) |

## Deferred checks
- P15B.2v: each image builds. P15B.4m: web image size drop. P15B.5s: discovery smoke. Web/discovery sweep profile lists are guesses until run in an image.
- P15B.7r: pip-audit fails on a known-vulnerable floor (needs CI run).
- Confirm torch 2.12.1 CPU wheel exists on the pytorch index for linux.

## Owner asks
- Docker OK for builds.
- Op P15B.3o: fill DEFAULT_MODEL_REVISION in backend/vector/embedding_model.py (HF cache or model_info sha); Dockerfile snapshot_download is unpinned until then.
- D-SEC7 answer (decides P15B.8); required-check setting for pip-audit job.
