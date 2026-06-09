# Sarrafi Cars — iOS app release guide

This document covers signing, TestFlight, and App Store submission for the native app in `ios/SarrafiCars/`. For day-to-day development and module layout, see [ios/README.md](../ios/README.md) and [ios/ARCHITECTURE.md](../ios/ARCHITECTURE.md).

## Prerequisites

1. **Apple Developer Program** membership (individual or organization).
2. **Xcode 15+** on macOS.
3. App record in [App Store Connect](https://appstoreconnect.apple.com/) with bundle ID **`com.sarraficars.app`**.
4. Production API reachable at **https://sarraficars.com** (Release build uses this via `App/AppConfig.swift`).

## One-time setup

### App Store Connect

1. **My Apps → + → New App**.
2. Platform: **iOS**. Name: **Sarrafi Cars** (or your marketing name).
3. Bundle ID: select **`com.sarraficars.app`** (create under [Certificates, Identifiers & Profiles](https://developer.apple.com/account/resources/identifiers/list) if needed).
4. SKU: e.g. `sarraficars-ios-001`.
5. Fill **Privacy Policy URL** and **Category** (e.g. Shopping or Lifestyle).

### Xcode signing

1. Open `ios/SarrafiCars/SarrafiCars.xcodeproj`.
2. Target **SarrafiCars** → **Signing & Capabilities**.
3. Enable **Automatically manage signing**.
4. Choose your **Team**.
5. Verify **Bundle Identifier**: `com.sarraficars.app`.
6. Add a **1024×1024** App Store icon in `Assets.xcassets` → **AppIcon** (required for upload).

## Archive and upload (TestFlight)

1. Select destination **Any iOS Device (arm64)** (not a simulator).
2. **Product → Archive**.
3. When the Organizer opens, select the archive → **Distribute App**.
4. Choose **App Store Connect** → **Upload**.
5. Follow prompts (include bitcode/symbols as Xcode recommends).
6. In App Store Connect → **TestFlight**, wait for processing (often 5–30 minutes).
7. Add **Internal** testers (team) or **External** testers (requires Beta App Review for first external build).

### Common upload failures

| Issue | Fix |
|-------|-----|
| Missing App Icon | Add all required sizes in AppIcon asset |
| Invalid bundle ID | Match App Store Connect and Xcode exactly |
| Signing certificate | Xcode → Settings → Accounts → Download Manual Profiles |
| ITMS-90717 (missing launch screen) | `Info.plist` includes `UILaunchScreen`; rebuild |

## TestFlight testing checklist

- [ ] App launches and loads https://sarraficars.com
- [ ] Back button works after in-app navigation
- [ ] Refresh reloads current page
- [ ] External links (e.g. dealer sites) open in Safari
- [ ] Login / logout on the website persists in the web view session
- [ ] Portrait and landscape on iPhone and iPad

## App Store release

1. App Store Connect → your app → **App Store** tab → **+ Version**.
2. Upload screenshots (6.7", 6.5", 5.5" iPhone; iPad if supporting tablet).
3. Description, keywords, support URL, privacy policy.
4. Select the **TestFlight build** for the version.
5. Submit for **App Review**.

Review notes for Apple:

> Sarrafi Cars is a companion app that displays our automotive inventory website in a secure in-app browser. Users browse listings, save cars, and sign in with the same account as the website. No separate account is required for the app itself.

## Version bumps

Update in Xcode target **General**:

- **Version** (`CFBundleShortVersionString`) — user-visible, e.g. `1.0.1`
- **Build** (`CFBundleVersion`) — increment every upload, e.g. `2`

Also update `Info.plist` if you maintain versions there manually.

## Always-on backend

The iOS app expects the Flask site to be available. For a home server or VPS that stays up, see [deploy/always-on/README.md](../deploy/always-on/README.md).

## Security

Mobile JSON auth (`/api/auth/*`) is documented in `docs/SECURITY_MASTER_TODO.md` (**SEC-077**). The WebView release relies on standard HTTPS and Flask session cookies; treat the site as the trust boundary.
