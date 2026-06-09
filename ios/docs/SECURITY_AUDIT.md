# Sarrafi Cars iOS — security audit

**Date:** 2026-05-26  
**Scope:** `ios/SarrafiCars/` (SwiftUI client, networking, WebViews)  
**Status:** Remediation complete (SEC-078, SEC-079)

---

## Summary

| ID | Issue | Status |
|----|--------|--------|
| H1 | WebView open redirect | **Fixed** — allowlist all navigations + response URL check |
| H2 | URLSession vs WKWebView cookies | **Fixed** — `SessionCookieBridge` sync both ways; native register uses URLSession (**SEC-086**) |
| M1 | Broad `NSAllowsLocalNetworking` | **Fixed** — localhost-only ATS exceptions |
| M2 | Debug API base URL allowlist | **Fixed** — `isAllowedAPIBaseURL` |
| M3 | No TLS pinning | **Fixed** — `PinnedTrustEvaluator` (Release) |
| M4 | Arbitrary image URLs | **Fixed** — `resolveSafeImageURL` + server `normalize_listing_image_url` |
| M5 | Logout cookie jar | **Fixed** |
| M6 | JS in WebView | **Mitigated** by H1 + server CSP (SEC-032) |
| L3 | Back-forward gestures | **Fixed** — disabled in embed WebView |

---

## TLS pinning

See [TLS_PINNING.md](TLS_PINNING.md). Release builds pin certificate DER hashes for **sarraficars.com**. Update pins after cert rotation.

---

## Validation checklist

Server-side controls below are covered by automated pytest; device-only flows still need manual runs before release.

- [ ] Manual (device): login native → Dashboard WebView logged in
- [ ] Manual (device): native register → signed in on phone; same credentials on website show saved cars
- [ ] Manual (device): external link opens Safari
- [ ] Manual (device): redirect to evil.com blocked in WebView
- [x] Server auth + image URL policy: `pytest backend/tests/test_mobile_auth_api.py backend/tests/test_safe_listing_url.py -q`
- [ ] Release archive: HTTPS base URL only; TLS pins match production cert (see [TLS_PINNING.md](TLS_PINNING.md))
