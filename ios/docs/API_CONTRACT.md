# iOS ↔ backend API contract (reference)

The iOS app consumes these routes from `backend/main.py`. Website HTML routes are not used except inside `Web/` embeds (`/car/:id`, `/compare`, `/premium`).

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/auth/csrf` | CSRF token for POSTs |
| GET | `/api/auth/me` | Session user |
| POST | `/api/auth/login` | Login |
| POST | `/api/auth/register` | Create account (same `users.db` as `/register`) |
| POST | `/api/auth/logout` | Logout |
| GET | `/api/listings/cars` | Grid JSON for one area (v2: `zip`/`radius`, or the session area) |
| GET | `/api/listings/filter-options` | Facet metadata |
| GET | `/api/listings/geo-coords` | ZIP + dealer coordinates |
| POST | `/api/search/smart` | Smart search within one area (v2) |
| POST | `/api/session/listings-geo` | Persist ZIP/radius |
| GET | `/api/zip-coords?zip=` | Geocode ZIP |
| GET | `/api/dealer-locator` | Nearby dealers |
| GET | `/api/saved-cars` | Saved cars (login required) |
| POST | `/api/cars/:id/save` | Toggle save (CSRF + login) |
| GET | `/api/cars/:id` | Car detail JSON (native path) |

**Registry:** `backend/mobile/contract.py` (kept in sync with this table). **Tests:** `backend/tests/test_mobile_api_contract.py`.

**Area-scoped listings (contract v2, 2026-09-28).** `GET /api/listings/cars` and `POST /api/search/smart` never return the whole fleet. The area is the request's ZIP (`zip`/`zip_code`) + radius (`radius`/`radius_miles`), else the area stored by `POST /api/session/listings-geo`. With neither: HTTP 400 `{"ok": false, "error": "zip_required", "area_required": true, "api_version": 2, "cars": []}` (`results: []` for smart search). Success bodies carry `api_version: 2`; the grid body also has `zip`, `radius` (snapped up to 10/25/50/100/250), `count`, `missing_coords`, `partial` (true while stored cards are being rebuilt: refetch shortly). Machine-readable: `LISTINGS_AREA_CONTRACT` in `backend/mobile/contract.py`.

`GET /api/listings/geo-coords` is used by the website for lazy geo maps; iOS currently geocodes per-ZIP via `/api/zip-coords` but may adopt bulk coords later.

When adding endpoints for iOS only, implement in `backend/main.py`, add to `contract.py`, and list them here. Prefer not to change website behavior in the same change unless required.
