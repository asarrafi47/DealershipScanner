# Development workflow

**Agent skill:** [`.cursor/skills/feature-branch-workflow/SKILL.md`](../.cursor/skills/feature-branch-workflow/SKILL.md)  
**App version:** repo-root [`VERSION`](../VERSION) (SemVer, exposed at `GET /health`)

---

## Branching

Every change set gets its **own feature branch**. Do not commit directly to `main`.

```bash
git checkout main
git pull origin main
git checkout -b feat/my-topic
# ... edit, test ...
git add ...
git commit -m "..."
```

| Prefix | Use for |
|--------|---------|
| `feat/` | New behavior, columns, scanner paths, UI features |
| `fix/` | Bug fixes |
| `docs/` | Documentation only |
| `chore/` | Tooling, deps, CI |
| `refactor/` | Internal restructure, no behavior change |

One task → one branch. Open a PR from that branch when ready.

---

## Versioning

Single source of truth: **`VERSION`** at the repository root.

| Bump | When |
|------|------|
| **MAJOR** | Breaking changes for API, mobile clients, or stored data consumers |
| **MINOR** | New features (inventory fields, enrichment, admin tools, scanner capabilities) |
| **PATCH** | Bug fixes only |

Bump `VERSION` in the **same delivery** as the code (same PR / commit series).

Current version is returned by the web app:

```bash
curl -s http://localhost:8000/health | jq .version
```

---

## Related docs

- [Data quality rollout](./DATA_QUALITY_ROLLOUT.md) — inventory NULL hygiene, scanner paths
- [Security master todo](./SECURITY_MASTER_TODO.md) — required for auth/secrets/API work
- [Platform master plan](./PLATFORM_MASTER_PLAN.md) — product architecture
