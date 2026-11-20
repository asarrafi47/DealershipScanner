# Edge cache for the anonymous landing and /find-dealers (2026-11-20)

Code: `backend/web/edge_cache.py`, used by `home()` (`/`) and `find_dealers_page()`
(`/find-dealers`). Tests: `backend/tests/test_edge_cache.py`.

## What the origin sends

| Request | Response |
|---|---|
| `GET`/`HEAD` with no `session` cookie, no `sc_dev` cookie, no `Authorization` | `Cache-Control: public, max-age=0, s-maxage=300, stale-while-revalidate=600, stale-if-error=86400`, weak `ETag`, 304 on a matching `If-None-Match`, `Vary: Accept-Encoding, Cookie`, no `Set-Cookie`, CSP with `script-src 'self'` and no nonce |
| Same page with an app cookie (anonymous session, any signed-in user) | `Cache-Control: private, no-store`, no `ETag`, `Vary: Cookie` |
| Signed-in user on `/` | `302` to `/home`, `Cache-Control: private, no-store` |

Why the origin is safe even though Cloudflare ignores `Vary: Cookie`: the public response
contains nothing per visitor. The CSRF token is lazy (`backend/utils/csrf.py::LazyCsrfToken`,
minted only when a template renders it), neither page renders it for an anonymous visitor,
neither page has an inline script (the CSP is nonce-free), and any app cookie makes the
response private. `EdgeSafeSessionInterface` downgrades any public response that sets a
cookie to private as a last guard.

Values: `s-maxage=300` matches the in-process TTL of the inventory count and featured cars;
`stale-while-revalidate=600` lets the edge answer at once and refresh in the background;
`stale-if-error=86400` keeps the landing up through a deploy or a crashed worker;
`max-age=0` makes browsers revalidate (a 304) so a deploy shows on the next view.
`CDN-Cache-Control` / `Cloudflare-CDN-Cache-Control` are not sent: `s-maxage` already gives
Cloudflare the edge TTL and every other shared cache the same one.

## Owner step: the Cloudflare Cache Rule

Cloudflare does not cache HTML by default, so without this rule `cf-cache-status` stays
`DYNAMIC` whatever the origin sends. Dashboard: sarraficars.com, Caching, Cache Rules,
Create rule.

- Rule name: `Edge cache anonymous marketing pages`
- When incoming requests match (Edit expression):

  ```
  (http.host eq "sarraficars.com" and http.request.method in {"GET" "HEAD"} and http.request.uri.path in {"/" "/find-dealers"} and not http.cookie contains "session=" and not http.cookie contains "sc_dev=")
  ```

- Then: Cache eligibility: **Eligible for cache**.
  - Edge TTL: **Use cache-control header if present, bypass cache if not** ("respect origin").
  - Browser TTL: **Respect origin TTL**.
  - Leave cache key, serve stale and origin error page settings at their defaults.
- Place it above any rule that bypasses cache for the same paths. Deploy.

The cookie test in the expression matters: a visitor with a `session` cookie must never be
answered from the cache (a signed-in user must get the 302 to `/home`). The origin marks
those responses `private, no-store` too, so Cloudflare would not store them either way.

Check after deploying the release and the rule:

```
curl -sI https://sarraficars.com/ | grep -iE 'cf-cache-status|cache-control|set-cookie|etag'
curl -sI https://sarraficars.com/ | grep -i cf-cache-status   # second call: HIT
curl -sI -H 'Cookie: session=x' https://sarraficars.com/ | grep -iE 'cf-cache-status|cache-control'   # BYPASS or DYNAMIC, private
```

Expected: first `MISS`, then `HIT` (or `REVALIDATED`/`UPDATING` after 5 minutes), no
`Set-Cookie`; with the cookie, `private, no-store` and no `HIT`.

Rollback: disable the rule. A cached copy is purged with Caching, Configuration, Custom Purge,
URL `https://sarraficars.com/` (and `/find-dealers`). After a deploy the edge may serve the
old HTML for up to 5 minutes (plus up to 10 stale); its `?v=` asset URLs still resolve, but
to the new files (`?v=` only busts caches). Purge both URLs after a release that changes
the landing's markup together with its CSS or JS, or removes a static file it loads.
