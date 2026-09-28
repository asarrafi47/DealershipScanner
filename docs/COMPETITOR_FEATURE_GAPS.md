# Competitor feature gaps: account and profile features

Date: 2026-09-28. Branch: `feature/profile-preferences`.

## Summary

Eight consumer car-shopping sites (Cars.com, Autotrader, CarGurus, Edmunds, TrueCar, Carvana,
CarMax, Kelley Blue Book) were surveyed for what a logged-in shopper gets from an account. The
pattern is consistent: saved cars and saved searches are table stakes, and every one of the eight
turns them into a retention channel with price-drop and new-inventory alerts controlled from a
notification-preferences page. Most also offer a trade-in valuation or instant cash offer, a deal
rating on every result, dealer reviews, and some form of financing pre-qualification. We already
cover the search and vehicle-detail side well (faceted and natural-language search, compare, VDP
with deal badge, market intelligence, window sticker, recalls, dealer research page) and we have
the raw signals for several account features that competitors charge attention for (per-car price
history and days on lot from the nightly scan, `listing_removed_at` from reconcile, `deal_score`,
saved cars and saved searches tables). What is missing is the loop that turns those signals into
something the user sees in the profile: alerts and the preferences to control them, a saved-search
page with names and one-click re-run, price history on the VDP, sold/removed state on saved cars,
and a garage for the shopper's own car. The gap list below is ordered so the S-effort items that
reuse existing data ship first and the L-effort items that need partners (lenders, dealer
acquisition offers) are scoped rather than started.

## Competitor feature matrix

"Ours" is the state of the codebase on this branch: yes = shipped, partial = data or endpoint
exists but no complete user-facing feature, no = nothing.

| Feature | Cars.com | Autotrader | CarGurus | Edmunds | TrueCar | Carvana | CarMax | KBB | Ours |
|---|---|---|---|---|---|---|---|---|---|
| Saved cars (heart), synced across devices | yes | yes | yes | yes | yes | yes | yes | yes | yes (`/api/saved-cars`, Home section, heart on cards and VDP) |
| Saved searches | yes, named, unlimited | yes | yes, one per make/model | yes | yes | yes | yes | - | partial (create/list/delete API behind `FEATURE_SAVED_SEARCHES`; panel on listings; no names, no profile page) |
| Search history / recent searches | - | - | - | - | - | - | - | - | yes (shipped today; see section 4) |
| Hide / exclude dealerships | - | - | - | - | - | - | - | - | yes (shipped today; see section 4) |
| Price-drop alerts on saved cars | yes | yes (email/text) | yes | yes | yes (email/push) | yes | yes | yes | no (price history stored; no notification loop) |
| New-listing alerts on saved searches | yes | yes, daily/weekly | yes | yes | - | yes | yes | - | no |
| Notification preferences page | - | yes (cadence, channel) | yes (email settings) | yes (dashboard gear) | - | yes (Communication Preferences) | - | - | no |
| Deal rating badge on every result | yes (Great/Good/Fair/Well-Equipped) | KBB Price Advisor (Good/Great paused May 2026) | yes (Great Deal to Overpriced) | Suggested Price / TMV | yes (Excellent/Great/Fair/High) | - | - | Price Advisor (paused) | partial (deal badge on VDP and market stats; not on grid, no sort) |
| Price history on listing | - | desktop only | yes (chart, days on lot, drops) | - | Price Graph (what others paid) | recent price update | - | - | partial (`price_history()`, `days_on_lot()`, grid price-drop badge; no VDP timeline) |
| Price-drop filter / sort | - | - | - | - | yes (cut in last 30 days, best deal sort) | yes (recent price update) | - | - | no (`price_drop_days_ago` in serialize only) |
| Compare listings side by side | yes | - | yes | - | yes | - | yes (with payment) | yes | yes (`/compare`, compare tray, AI compare assistant) |
| Share a car or saved list | - | yes (email/text) | no (requested) | yes (share research) | - | - | - | - | partial (copy-link on a single VDP only) |
| Dealer reviews on listings / dealer pages | yes (read + submit) | yes | yes (invited after contact, dealer reply) | - | - | - | - | - | partial (`dealer_reviews.py` scaffold, Google rating on dealership page; no aggregate on cards, no dealer reply, no invitation) |
| Monthly payment / affordability calculator | yes | - | - | - | - | payments while searching | personalized payment | - | partial (estimated-payment calculator on VDP, user-entered APR) |
| Financing pre-qualification on listings | - | - | yes (Capital One, Westlake, GLS, Chase) | - | yes | yes (no credit impact) | yes (multi-lender) | - | no |
| Trade-in valuation / instant cash offer | yes | KBB ICO | yes | yes (appraisal + ICO) | yes (True Cash Offer) | yes (under 3 min) | yes (appraisal appointments) | yes (My Car's Value, ICO) | no |
| Garage: own vehicle with value, recalls | - | - | - | yes (appraisal) | - | yes (recalls on owned car) | - | yes (My Car's Value) | no (recall lookup and VIN decode exist, not tied to the user) |
| Personalized recommendations | - | - | - | yes (Insider) | yes (rankings) | - | - | yes (from saved research) | partial (Home "Recommended for you" from views/compares + ZIP; not from saved searches; hidden dealers now excluded) |
| Sold / removed state on saved cars | yes | - | yes | yes (X per card) | - | - | - | - | no (`listing_removed_at` exists; saved cars vanish) |
| Vehicle history report on listing | - | yes (most listings) | yes (accidents, owners) | - | - | yes (free CARFAX) | - | - | partial (NHTSA recalls by VIN, `FEATURE_VEHICLE_HISTORY`; no title/accident report) |
| Fee-transparency badge / filter | - | all-in pricing badge (2026) | yes (price includes fees, filter) | - | - | - | - | - | no (dealership page lists add-on fee notes only) |
| Search nearby dealerships | yes (by brand) | - | - | - | - | - | - | - | yes (`find_dealers.html`, nearby-dealers picker, radius filter) |
| Social login | - | - | - | yes (Facebook/Google/Apple) | - | - | - | - | yes (Google, Apple) |
| Purchase / payments in account | - | - | - | - | - | yes (resume purchase, payments) | yes (auto finance payments) | - | n/a (marketplace, not a retailer) |

## Prioritized gap list

Ordered by value per unit of effort. "Hooks" names the code that would carry the change.

### 1. Saved-search page with naming and one-click re-run (S)

- Who has it: Cars.com, Autotrader, CarGurus, Edmunds, TrueCar, CarMax.
- Why it matters: we have create/list/delete endpoints but no page to manage or re-run saved
  searches and no user-assigned names. Competitors treat named searches with a one-click return
  as table stakes for the account area. Prerequisite for new-listing alerts (item 3).
- Hooks: add `name` to `saved_searches` (`migrations/V018__saved_searches.sql`, DDL mirror in
  `backend/db/inventory_pg.py`), extend `/api/saved-searches` in `backend/routes/listings_api.py`,
  add a card to `frontend/templates/account_profile.html` matching the existing Profile / Password
  / Hidden dealerships / Recent searches cards. The "Run" link rebuilds `/listings` query params
  from the stored filters exactly as the Recent searches card does today
  (`backend/utils/search_history_format.py`, `frontend/static/account_profile.js`).

### 2. Price-drop alerts on saved cars (M)

- Who has it: all eight.
- Why it matters: the universal reason shoppers create an account. Per-car price history already
  lives in `cars.price_provenance_json` (`backend/intelligence/inventory_signals.py`:
  `price_history`, `recent_price_drop`) and `saved_cars` exists, so the signal is there; only the
  notification loop is missing. Turns saved cars into a retention channel.
- Hooks: a post-scan job that diffs saved-car prices against the last notified price, a
  `user_notification_prefs` table (item 4), and an outbound email provider. Today only
  `backend/utils/mfa_delivery.py` sends mail; there is no SMTP/SendGrid/Resend integration, so
  that is the real cost. Push can follow through the iOS scaffold (`docs/IOS_APP.md`).

### 3. New-listing alerts on saved searches (M)

- Who has it: Cars.com, Autotrader, CarGurus, Edmunds, Carvana, CarMax.
- Why it matters: a saved search without alerts is a bookmark. The nightly scan already records
  `first_seen_at` per listing, so "new since last alert" is a cheap query per saved search.
- Hooks: same notification and email infrastructure as item 2; per-search alert toggle and
  daily/weekly cadence (Autotrader's model) on the saved-search card from item 1; query through
  `backend/db/repositories/listings_repo.py` with the stored filters. Respect the user's hidden
  dealerships (`main.hidden_dealer_ids_for_user`) when matching.

### 4. Notification preferences card in profile (S)

- Who has it: Carvana, Autotrader, Edmunds, CarGurus.
- Why it matters: every competitor with alerts exposes per-channel, per-type toggles. Required for
  CAN-SPAM-compliant alerts and it belongs in the profile section being expanded now.
- Hooks: `account_profile.html` gets a Notifications card (same markup and CSS classes as the
  existing cards, no new visual treatment); new `user_notification_prefs` table via a
  `migrations/V023__*.sql` plus the SQLite mirror in `inventory_pg.py` / `schema_repo.py`; the
  alert jobs from items 2 and 3 read it.

### 5. Price history timeline and days on lot on the VDP (S)

- Who has it: CarGurus (chart, days on lot, drops), Autotrader (desktop), Carvana (recent update).
- Why it matters: this is our data moat (nightly scan of the dealer's own site) made visible and
  strong negotiation leverage for the shopper. `price_history()` and `days_on_lot()` already run
  and the grid shows a price-drop badge, but `car.html` never shows the timeline.
- Hooks: render the `price_provenance_json` timeline and `first_seen_at` on
  `frontend/templates/car.html` next to the deal badge. Pure read-time; no new data collection.
  Chart styling should follow the existing VDP palette, not a library default.

### 6. Deal rating badge and "sort by deal" in the results grid (S)

- Who has it: Cars.com, CarGurus, TrueCar; Autotrader and KBB paused Good/Great badges in May
  2026 over FTC all-in-pricing enforcement.
- Why it matters: shoppers compare in the grid, not on the VDP, and we only badge the VDP. Because
  of the Cox pause, badge only when the price is known to be fee-inclusive or label it as the
  advertised price.
- Hooks: carry `deal_score` into the listings-grid serializer in `backend/routes/listings_api.py`,
  add the badge to the card template in `frontend/templates/listings.html` and a sort option; add a
  "price includes fees" caveat consistent with CarGurus fee-transparency badges.

### 7. Price-drop filter and sort in search results (S)

- Who has it: TrueCar (cut in last 30 days; best deal sort), Carvana (recent price update).
- Why it matters: cheap and differentiating for bargain hunters; the grid already carries
  `price_drop_days_ago`.
- Hooks: filter option in the `listings_api.py` filter-options payload and a WHERE clause in
  `backend/db/repositories/listings_repo.py`. That file already notes `price_drop_days_ago` is
  wall-clock dependent, so compute it at query time rather than caching it on the row.

### 8. Sold / removed state on saved cars (S)

- Who has it: CarGurus, Edmunds, Cars.com.
- Why it matters: saved cars that leave inventory currently vanish. Competitors mark them sold or
  let users clear them. A "no longer listed, removed on <date>, last price $X" state also feeds
  the sold-alert story.
- Hooks: use `listing_removed_at` from the reconcile pass (working since the 09-23 pipeline) in the
  `api_saved_cars` serialization in `listings_api.py` and in the saved-cars section on
  `frontend/templates/home.html`; later the profile saved-cars card.

### 9. Share saved cars / compare list (S)

- Who has it: Autotrader (email/text), Edmunds (share research); CarGurus users publicly ask for it.
- Why it matters: car buying is usually a two-person decision. We have a copy-link share button on
  a single VDP only.
- Hooks: `/compare` (`frontend/templates/compare.html`) can encode car ids in the URL; saved-cars
  list gets an opt-in public read-only token; add Web Share API / mailto on both.

### 10. Dealer reviews with aggregate rating on cards, dealer response (M)

- Who has it: Cars.com, Autotrader, CarGurus (invited after contact; dealer can respond).
- Why it matters: aggregate stars on listing cards influence which dealer gets the lead, and dealer
  replies are a dealer-portal feature we can charge for.
- Hooks: `backend/routes/dealer_reviews.py` (post/edit one review, flag, auto-hide at 3 flags) and
  the cached Google rating via `community_api` exist. Add avg rating and count to the dealer
  serialization and listing cards, a dealer-portal reply endpoint, and a post-contact review prompt
  once lead/contact events exist.

### 11. Garage: the shopper's own vehicle (M)

- Who has it: KBB (My Car's Value), Edmunds (appraisal), Carvana (recalls on owned car).
- Why it matters: gives non-shoppers a reason to return, and anchors trade-in math against
  listings. We already have NHTSA recall lookup, vPIC VIN decode, window stickers and build sheets.
- Hooks: new `user_vehicles` table (user_id, vin, nickname, mileage); reuse
  `frontend/templates/nhtsa_recalls.html` and the VIN decode path; value estimate from the
  market-stats comparables (`/api/listings/market-stats`). Profile card, same markup as the rest.

### 12. Personalized recommendations from saved searches and hidden dealers (M)

- Who has it: KBB, Edmunds, TrueCar.
- Why it matters: `home.html` already has "Recommended for you" from view/compare history and ZIP,
  and as of today it excludes hidden dealerships (`backend/routes/home_dashboard.py`). Feeding saved
  searches and recent searches into the same query is the obvious next step.
- Hooks: extend the recommendation query in `home_dashboard.py` with `saved_searches` filters and
  `search_history_repo` terms; optionally use the pgvector embeddings already present for
  natural-language search (`backend/utils/hybrid_search.py`).

### 13. Financing pre-qualification / personalized payment (L; S interim)

- Who has it: CarGurus, TrueCar, Carvana, CarMax; Cars.com has calculators.
- Why it matters: real APR and monthly payment per listing after a soft pull. Also a revenue line
  (lender referral fees) the GTM plan does not count yet.
- Hooks: full version needs a lender partner (Capital One Auto Navigator dealer program or an
  aggregator) and compliance review. Interim S version: an affordability calculator ("what can I
  afford at $X/mo") plus saving the user's down payment, APR and term to the profile so every VDP
  payment estimate on `car.html` is personalized.

### 14. Trade-in valuation / instant cash offer (L)

- Who has it: all eight.
- Why it matters: the second most common account feature after saved cars and a natural fit for the
  dealer product (dealers pay for acquisition leads). We have nothing.
- Hooks: our market-stats engine can estimate retail from scanned comparables by
  year/make/model/trim/mileage; trade-in is a discount off that. Start with a "value my car"
  estimate stored in the garage (item 11); defer instant offers until dealers can opt in through the
  dealer portal.

## Shipped today on feature/profile-preferences (2026-09-28)

Two profile features from this list's neighbourhood landed on this branch today and are counted as
"yes" in the matrix above. No surveyed competitor offers either.

- Hidden dealerships (commits `ac134fb4a`, `5244b745f`): per-user list of dealerships excluded
  from search results, smart search, nearby-dealer picker, client-side previews and Home
  recommendations. Table `user_hidden_dealers` (`migrations/V021__user_hidden_dealers.sql`),
  repository `backend/db/repositories/hidden_dealers_repo.py`, API `/api/profile/hidden-dealers`
  (GET/POST/DELETE) in `backend/routes/listings_api.py`, "Hide this dealership" control on
  `dealership.html`, and a Hidden dealerships card on `account_profile.html`. Tests:
  `backend/tests/test_hidden_dealers.py`.
- Recent searches (commit `542ec0687`): per-user search history recorded from faceted and smart
  searches, with run-again links, save-as-saved-search, per-entry delete and clear-all. Table
  `user_search_history` (`migrations/V022__user_search_history.sql`), repository
  `backend/db/repositories/search_history_repo.py`, formatter
  `backend/utils/search_history_format.py`, API `/api/profile/search-history` in
  `listings_api.py`, and a Recent searches card on `account_profile.html`. Tests:
  `backend/tests/test_search_history.py`.

Both cards reuse the existing profile page markup and CSS classes; new profile cards from the gap
list should do the same.

## Sources

Cars.com

- https://www.cars.com/articles/save-car-listings-and-searches-across-devices-with-new-cars-com-profiles-1420663002302/
- https://www.cars.com/articles/five-hacks-for-car-shopping-with-cars-coms-app-1420692717122/
- https://www.cars.com/profile/saved-cars/
- https://investor.cars.com/2017-10-12-Cars-com-Leverages-Algorithm-to-Redefine-Best-In-Class-Car-Search-and-Shopping-Experience
- https://www.cars.com/dealers/reviews/

Autotrader

- https://www.autotrader.com/help/my-autotrader
- https://www.autoremarketing.com/ar/technology/autotrader-unveils-new-alerts-feature-push-texts-emails-shoppers/
- https://www.prnewswire.com/news-releases/autotradercom-launches-new-alerts-feature-236049791.html
- https://www.cargurus.ca/Cars/Discussion-ds812131
- https://www.cbtnews.com/cox-automotive-pauses-good-great-price-badging/
- https://forums.redflagdeals.com/autotrader-price-history-cn-gone-2678123/
- https://www.nerdwallet.com/article/loans/auto-loans/autotrader-app-review
- https://www.kbb.com/instant-cash-offer/

CarGurus

- https://cargurus.helpscoutdocs.com/article/29-where-can-i-view-my-saved-listings
- https://www.cargurus.com/Cars/myAccount/saved-searches
- https://www.cargurus.com/Cars/myAccount/saved-listings
- https://www.amerifreight.net/blog/cargurus-review
- https://www.cargurus.com/about/dealer-reviews
- https://www.cargurus.com/research/compare
- https://www.autoremarketing.com/subprime/cargurus-unveils-pre-qualify-program-through-capital-one-westlake-gls/
- https://www.cargurus.com/about/press/fee-transparency-updates
- https://www.cargurus.com/Cars/Discussion-ds988005

Edmunds

- https://help.edmunds.com/hc/en-us/articles/115011994448-Your-Edmunds-Insider-account
- https://help.edmunds.com/hc/en-us/articles/360019544514-What-are-the-advantages-of-registering-as-an-Edmunds-Insider
- https://help.edmunds.com/hc/en-us/articles/360019714233-How-do-I-save-and-remove-cars-from-my-Edmunds-Insider-account
- https://www.montway.com/blog/guide-to-buying-a-car-on-edmunds-MCCFSZAGT73JAEBKMEOD5F27BHAY
- https://www.edmunds.com/appraisal/
- https://help.edmunds.com/hc/en-us/articles/360024831253-What-is-Edmunds-Suggested-Price

TrueCar

- https://www.truecar.com/blog/3-truecar-features-you-wish-you-knew-sooner/
- https://www.truecar.com/account/saved-searches/
- https://www.truecar.com/account/saved-vehicles/
- https://www.truecar.com/blog/truecar-price-curve/
- https://www.truecar.com/sell-your-car/
- https://www.truecar.com/trade/
- https://www.nerdwallet.com/article/loans/auto-loans/truecar-app-review

Carvana

- https://www.carvana.com/my-carvana/
- https://unsubscribeforgmail.com/guides/how-to-unsubscribe-from-carvana-emails
- https://www.carvana.com/account-ui/notifications
- https://play.google.com/store/apps/details?id=com.carvana.carvana&hl=en_US
- https://www.carvana.com/cars/recent-price-update

CarMax

- https://www.carmax.com/mycarmax/register
- https://www.carmax.com/mycarmax/saved-cars
- https://www.carmax.com/mycarmax/saved-searches
- https://www.carmax.com/watchlist
- https://investors.carmax.com/news-and-events/news/news-details/2023/CarMax-Launches-New-Online-Pre-Qualification-Capability-Where-Customers-Can-Shop-Cars-Nationwide-with-Personalized-Financing-Terms/default.aspx
- https://www.carmax.com/research/car-comparison
- https://apps.apple.com/us/app/carmax-used-cars-for-sale/id571044395

Kelley Blue Book

- https://apps.apple.com/us/app/kelley-blue-book-we-know-cars/id6670768496
- https://play.google.com/store/apps/details?id=com.kbb.mobile.kbb_app&hl=en_US
- https://www.kbb.com/whats-my-car-worth/
- https://www.kbb.com/instant-cash-offer/
- https://www.cbtnews.com/cox-automotive-pauses-good-great-price-badging/
- https://www.kbb.com/car-prices/
