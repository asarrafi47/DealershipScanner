# Sarrafi Cars iOS — architecture

The iOS app is a **separate product** from the Flask website. It lives entirely under `ios/` and talks to the backend only through documented JSON HTTP APIs and a few embedded WebViews (car detail, compare, premium billing).

## Boundaries

| Layer | Location | Do not mix with |
|-------|----------|-----------------|
| **iOS app** | `ios/` | `frontend/`, `backend/` (except shared API contract) |
| **Website** | `frontend/`, `backend/listings/` | `ios/` |
| **Shared API** | `backend/main.py` mobile routes | Document changes in `ios/docs/API_CONTRACT.md` |

Native UI (SwiftUI) is the default. WebViews are intentional islands for features that still match the website (chat, stickers, compare layout, Stripe).

## Module layout

```
ios/
├── README.md                 # Run / debug
├── ARCHITECTURE.md             # This file
├── docs/
│   └── API_CONTRACT.md         # Endpoints the app uses (reference)
└── SarrafiCars/
    ├── SarrafiCars.xcodeproj
    └── SarrafiCars/
        ├── App/                # Entry, tabs, global config & state
        ├── Core/
        │   ├── Theme/          # Colors, shells, shared chrome
        │   └── Components/     # Reusable UI (cards, grid, images)
        ├── Networking/         # APIClient + DTOs
        ├── Features/
        │   ├── Auth/
        │   ├── Home/           # Search + listings
        │   ├── Saved/
        │   ├── Dealers/
        │   └── Premium/
        ├── Web/                # WKWebView wrappers only
        ├── Resources/          # Info.plist, Assets
        └── Legacy/             # Old tabs — not in Xcode target
```

## Feature rules

1. **New screens** go under `Features/<Name>/`, not the repo root.
2. **Shared UI** used by 2+ features → `Core/Components/` or `Core/Theme/`.
3. **API types and HTTP** → `Networking/` only (`APIClient.swift`, `Models.swift`).
4. **No website templates or JS** in the iOS tree.
5. **Backend changes** for mobile: only when the app needs a new/changed JSON endpoint; update `ios/docs/API_CONTRACT.md` in the same PR.

## Build

Open `ios/SarrafiCars/SarrafiCars.xcodeproj`. Debug → `http://127.0.0.1:5001`; Release → `https://sarraficars.com` (`App/AppConfig.swift`).
