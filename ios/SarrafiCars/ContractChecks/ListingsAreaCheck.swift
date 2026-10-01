// Regression check for audit B7 (2026-10-01): the app must scope every
// `/api/listings/cars` request to a ZIP + radius, and must turn the server's
// 400 `zip_required` into a ZIP prompt rather than a generic error.
//
// The app has no XCTest target, so this is a standalone macOS executable built
// against the real networking sources (no simulator, no xcodebuild):
//
//   cd ios/SarrafiCars
//   xcrun swiftc -parse-as-library SarrafiCars/Networking/*.swift \
//       SarrafiCars/App/AppConfig.swift ContractChecks/ListingsAreaCheck.swift \
//       -o /tmp/listings_area_check && /tmp/listings_area_check

import Foundation

/// Lives in Features/Dealers/DealersView.swift (SwiftUI); stubbed so the
/// networking layer compiles on its own.
struct DealerLocatorResponse: Decodable {}

@main
enum ListingsAreaCheck {
    static var failures = 0

    static func check(_ ok: Bool, _ what: String) {
        print("\(ok ? "PASS" : "FAIL")  \(what)")
        if !ok { failures += 1 }
    }

    static func main() {
        check(
            APIClient.listingsCarsPath(zip: "92694", radiusMiles: 25) == "/api/listings/cars?zip=92694&radius=25",
            "valid ZIP sends zip + radius explicitly"
        )
        check(
            APIClient.listingsCarsPath(zip: " 92694 ", radiusMiles: 100) == "/api/listings/cars?zip=92694&radius=100",
            "ZIP is trimmed"
        )
        check(APIClient.listingsCarsPath(zip: "", radiusMiles: 25) == nil, "empty ZIP has no path")
        check(APIClient.listingsCarsPath(zip: "9269", radiusMiles: 25) == nil, "short ZIP has no path")
        check(APIClient.listingsCarsPath(zip: "92a94", radiusMiles: 25) == nil, "non-digit ZIP has no path")
        check(APIClient.listingsCarsPath(zip: "٩٢٦٩٤", radiusMiles: 25) == nil, "non-ASCII digits rejected")

        if case APIError.zipRequired = APIClient.mapListingsError(APIError.httpStatus(400, "zip_required")) {
            check(true, "400 zip_required maps to .zipRequired")
        } else {
            check(false, "400 zip_required maps to .zipRequired")
        }
        if case APIError.httpStatus(400, "zip_not_found"?) = APIClient.mapListingsError(APIError.httpStatus(400, "zip_not_found")) {
            check(true, "other 400s pass through")
        } else {
            check(false, "other 400s pass through")
        }
        if case APIError.httpStatus(500, nil) = APIClient.mapListingsError(APIError.httpStatus(500, nil)) {
            check(true, "5xx passes through")
        } else {
            check(false, "5xx passes through")
        }
        check(
            APIError.zipRequired.errorDescription?.contains("ZIP") == true,
            ".zipRequired reads as a ZIP prompt"
        )

        print(failures == 0 ? "OK" : "\(failures) failed")
        exit(failures == 0 ? 0 : 1)
    }
}
