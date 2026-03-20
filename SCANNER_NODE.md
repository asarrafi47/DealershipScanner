# Node.js / Puppeteer scanner

The inventory scraper can run as **Node.js + Puppeteer** (stealth) while the web app stays **Python + Flask**. Both use the same **`inventory.db`** file.

## Setup

Install **npm dependencies once** (required; otherwise `Cannot find module 'fs-extra'`):

```bash
cd /path/to/DealershipScanner
npm install
```

## Run

```bash
node scanner.js
# or
npm run scan
```

Web app (unchanged):

```bash
python run.py
```

Set a custom DB path (optional):

```bash
export INVENTORY_DB_PATH=/absolute/path/to/inventory.db
node scanner.js
```

Optional: cap inventory API page size (default **500**):

```bash
export INVENTORY_PAGE_SIZE=500
node scanner.js
```

## What it does

- Reads **`dealers.json`** (same manifest as the Python scanner).
- Skips dealers where `provider` is not `dealer_dot_com` (e.g. `dealer_on`).
- Launches **Puppeteer** in **`headless: "new"`** with **puppeteer-extra-plugin-stealth** and a **random** Chrome **User-Agent**.
- **Turbo mode (default):**
  - **Blocks** Puppeteer requests for **image**, **stylesheet**, **font**, and **media** (JSON/API traffic only).
  - Rewrites **`getInventory`** **GET** URLs to use a large **`pageSize`** (see `INVENTORY_PAGE_SIZE`, max 500).
  - Uses **`page.waitForResponse`** on the first **`getInventory`** JSON after each inventory navigation (no long fixed sleeps).
  - If the JSON includes **`totalCount`** (or common variants) and there are more vehicles than one page, fetches **additional pages** with **`fetch()` inside the page** (same cookies), instead of clicking “Next”.
  - Runs **all Dealer.com dealers in parallel** (`Promise.all`); writes to SQLite **once** at the end to avoid lock contention.
- **Slow fallback (per dealer):** If turbo yields no vehicles or throws (e.g. CAPTCHA / blocking), retries that dealer with **human-like** scrolling and **Next / Load more** clicks and longer waits.
- Intercepts JSON responses whose URL suggests **getInventory** (turbo) or **inventory** APIs (slow) and validates vehicle lists (same recursive `vin` heuristic as Python).
- Maps **Dealer.com** fields (title array, `trackingPricing.internetPrice`, odometer in `trackingAttributes`, `images[].uri`, `callout` / `highlightedAttributes`, Carfax URLs, etc.).
- **Self-correction**: looks up **`model_specs`** by `make` + `model` and fills missing **cylinders** or **transmission** (or derives transmission from **gears**).
- **Upserts** into **`cars`** with **`gallery`** and **`history_highlights`** stored as JSON strings (matches Python `upsert_vehicles`).

## `model_specs` table

Populated manually (or by a future admin tool). Example:

```sql
INSERT OR REPLACE INTO model_specs (make, model, cylinders, gears, transmission)
VALUES ('BMW', 'X5', 6, 8, '8-Speed Automatic');
```

If no row exists, self-correction is a no-op.

## Frontend

Vehicle detail **`car.html`** remains **3 columns**: gallery | metadata | history highlights + **View Full CARFAX Report** (new tab). No iframe.

## Column parity

The Node `INSERT ... ON CONFLICT` matches `backend/database.py` `upsert_vehicles`:

`vin`, `title`, `year`, `make`, `model`, `trim`, `price`, `mileage`, `image_url`, `dealer_name`, `dealer_url`, `dealer_id`, `scraped_at`, `zip_code`, `fuel_type`, `cylinders`, `transmission`, `drivetrain`, `exterior_color`, `interior_color`, `stock_number`, `gallery`, `carfax_url`, `history_highlights`, **`msrp`**.

**`msrp`** is stored when the listing hides the sale price but still exposes MSRP (used on the detail page for “MSRP: $XX,XXX” vs “Call for Price”).

## Knowledge Engine (EPA + trim decoder)

- **Python** module: `backend/knowledge_engine.py` — `decode_trim_logic()`, EPA lookup on **`epa_master`**, merged into **`verified_specs`** on the car detail page.
- **Manual EPA import** (one-time / periodic):

```bash
python scripts/import_epa_master.py
```

Downloads `vehicles.csv` from fueleconomy.gov and fills **`epa_master`** (same DB as `inventory.db`).

The existing **`model_specs`** table remains for manual overrides; the Node scanner still applies **self-correction** from `model_specs` when present.

## AI co-pilot (GPT-4o + EPA verification)

- **Env:** `OPENAI_API_KEY` (required for live answers). Optional: `OPENAI_CHAT_MODEL` (default `gpt-4o`).
- **Backend:** `backend/ai_agent.py` — `verify_car_data(vin)` compares listing vs `epa_master` + trim decoder; `POST /api/ai/chat` runs the assistant with that JSON in context.
- **Debug:** `GET /api/ai/verify/<vin>` returns verification JSON only (no OpenAI call).
- **Frontend:** `frontend/static/ai_widget.js` — floating **Co-Pilot** on listings & dashboard; car detail uses the gallery chat + same API. Amber highlight on `data-spec-field` rows when `discrepancy_flags` is returned.
- **Legacy:** `POST /api/chat` (Ollama) is unchanged if you still use local LLM elsewhere.

```bash
pip install openai
export OPENAI_API_KEY=sk-...
```
