# Sarrafi Cars — iOS app (native)

Standalone **SwiftUI** app under `ios/`. It is not the website and does not share frontend templates. See [ARCHITECTURE.md](ARCHITECTURE.md) for module boundaries and agent rules.

## User flow

1. **Home** — ZIP + radius + search → results grid → car detail (WebView).
2. **Saved** (signed in) — Grid → compare 2–4 (WebView).
3. **Dealers** — Location or ZIP → map + list.
4. **Premium** — Billing WebView when signed in without paid access.

Auth: **Sign in** / **Get started** or **Profile** in the navy header (`Features/Auth`).

## Project layout

```
SarrafiCars/SarrafiCars/
├── App/
├── Core/Theme/ + Core/Components/
├── Networking/
├── Features/Auth|Home|Saved|Dealers|Premium/
├── Web/
└── Resources/
```

`Legacy/` holds old tab prototypes — **not** in the Xcode target.

## Run

```bash
open ios/SarrafiCars/SarrafiCars.xcodeproj
```

Set your Team → **⌘R**.

| Build | API base |
|-------|----------|
| **Debug** | `http://127.0.0.1:5001` (run `./start.sh` at repo root) |
| **Release** | `https://sarraficars.com` |

Physical device (Debug): set `SARRAFI_API_BASE_URL` in `Resources/Info.plist` to `http://<your-mac-lan-ip>:5001`. LAN HTTP may require an ATS exception for that IP, or use an HTTPS tunnel to production.

## APIs

Documented in [docs/API_CONTRACT.md](docs/API_CONTRACT.md). Session cookies are shared with `Web/` for embedded pages.
