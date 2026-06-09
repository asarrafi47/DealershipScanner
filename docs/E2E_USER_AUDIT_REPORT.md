# E2E user audit report

**Base URL:** `http://localhost:5099`  
**Generated:** 2026-05-24 00:14 UTC  

## Summary

| Result | Count |
|--------|------:|
| Passed checks | 15 |
| Bugs | 2 |
| Warnings | 5 |
| Improvements | 0 |

## Bugs (fix first)

- **auth:** Free user post-register should reach dashboard
  - http://localhost:5099/listings?q=&zip_code=
- **setup:** Could not grant premium: user not found: e2e_prem_730799b7@example.com

## Warnings

- **copy:** Landing missing Intelligent Listing Assistant feature title
- **nav:** Free user dashboard missing Premium upsell in nav
- **car-detail:** Save button missing for logged-in free user
- **car-chat:** Car chat 403 but error=login_required
- **auth:** Logout control not found on car page

## Passed

- Guest landing loads
- Guest listings page renders grid shell
- Dashboard requires login
- Guest car detail hides Packages section
- Guest car detail hides car chat section
- Free user dashboard hides dealer inventory nav
- Premium page uses intelligent wording (no AI label)
- Free user car detail hides Packages section
- Free user car detail hides car chat section
- Free user smart search returns results UI
- Ram car detail shows Engine row
- Free user car detail hides window sticker block
- Free user BMW detail hides window sticker block
- Free user car detail hides car chat section
- Free user car page has no premium upsell blocks

## Failed network requests

- `GET https://content.homenetiol.com/2000292/2143540/0x0/e1c99a4d860c41909fcadcdceb2a5dec.jpg — net::ERR_ABORTED`
- `GET https://content.homenetiol.com/2000292/2143540/0x0/c6b04fc7ff7642debda1dcd8511b7c01.jpg — net::ERR_ABORTED`
