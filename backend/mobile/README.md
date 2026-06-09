# Backend — mobile (iOS) APIs

Native clients consume **JSON** routes implemented in `backend/main.py`. This folder is the **scope anchor** for iOS backend work; it does not replace `main.py` yet.

## Contract

| File | Purpose |
|------|---------|
| [`contract.py`](contract.py) | Machine-readable route list (endpoint names + auth mode) |
| [`../../ios/docs/API_CONTRACT.md`](../../ios/docs/API_CONTRACT.md) | Human-readable contract for iOS developers |
| [`../tests/test_mobile_api_contract.py`](../tests/test_mobile_api_contract.py) | Asserts every contract route is registered |

## Related tests

```bash
python -m pytest backend/tests/test_mobile_api_contract.py \
  backend/tests/test_mobile_auth_api.py \
  backend/tests/test_car_detail_api.py \
  backend/tests/test_saved_cars_api.py \
  backend/tests/test_listings_perf.py -q
```

## Security (master todo)

| SEC | Topic |
|-----|--------|
| SEC-011 | CSRF on POST mutations |
| SEC-013 | Auth on `/api/saved-cars`, save toggle |
| SEC-077, SEC-086 | `/api/auth/*` (incl. `POST /api/auth/register` → `users.db`) |
| SEC-040 | `/api/search/smart` rate limit |

## Adding an endpoint

1. Implement in `backend/main.py` (or extract to a blueprint under `backend/mobile/` later).
2. Register CSRF in `_csrf_mutating_requests` if POST.
3. Add row to `contract.py` and `ios/docs/API_CONTRACT.md`.
4. Add behavior test under `backend/tests/test_mobile_*.py`.
5. Update `docs/SECURITY_MASTER_TODO.md` if auth/CSRF/rate limits apply.

## Website overlap

Several routes are shared with the listings website (`/api/listings/cars`, `filter-options`, `geo-coords`). Change behavior only when both clients need it, or version the API if shapes diverge.
