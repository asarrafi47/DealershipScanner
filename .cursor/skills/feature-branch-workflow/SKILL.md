---
name: feature-branch-workflow
description: >-
  Sarrafi Collection git workflow: create a new feature branch before every code
  change, never commit to main, bump repo-root VERSION (SemVer) with the delivery,
  and commit on the feature branch. Use when implementing features, fixes, refactors,
  docs, schema changes, or when the user asks to commit, branch, or release.
---

# Feature branch + version workflow

Mandatory for **every** code or schema change in DealershipScanner.

## Before you edit

1. **Do not work on `main`.** If currently on `main`, create and switch to a feature branch first.
2. **Create a new feature branch** for this task (one branch per feature/fix/doc set):

   ```bash
   git fetch origin
   git checkout main
   git pull origin main
   git checkout -b feat/short-topic-name
   ```

   Branch prefixes: `feat/`, `fix/`, `docs/`, `chore/`, `refactor/`.

3. If the user already has a relevant feature branch checked out for the same task, stay on it. Otherwise **create a new branch** — do not pile unrelated work onto an old branch.

## While implementing

- Keep changes scoped to the branch topic.
- Match `.cursor/rules/dealership-scanner-standards.mdc` and security todo when applicable.

## Version bump (required with delivery)

Canonical app version: repo-root **`VERSION`** (read by `/health` and `backend/main.py`).

Use **Semantic Versioning** (`MAJOR.MINOR.PATCH`):

| Change type | Bump | Example |
|-------------|------|---------|
| Breaking API/DB/schema for consumers | MAJOR | `1.0.0` → `2.0.0` |
| New feature, inventory columns, scanner behavior | MINOR | `0.2.0` → `0.3.0` |
| Bug fix only, no new surface | PATCH | `0.3.0` → `0.3.1` |

Update **`VERSION`** in the **same commit series** as the code change (not a follow-up “later” commit unless the user asks to split).

Do not bump for typo-only doc fixes unless the user requests a release.

## Commit

- Commit on the **feature branch**, not `main`.
- Only commit when the user asks (see user commit rules).
- Commit message: complete sentences; summarize **why**, not just file list.

## After commit

- Do **not** push or merge to `main` unless the user asks.
- PRs target `main` from the feature branch when the user requests a PR.

## Quick checklist

```
- [ ] On a feature branch (not main)
- [ ] Branch name matches the task
- [ ] VERSION bumped per SemVer (if shipping functional change)
- [ ] Commits on feature branch only
```

## Reference

Human-readable doc: [docs/DEVELOPMENT_WORKFLOW.md](../../../docs/DEVELOPMENT_WORKFLOW.md)
