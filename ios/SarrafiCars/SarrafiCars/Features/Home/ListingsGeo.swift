import Foundation

enum ListingsGeo {
    static let earthRadiusMiles = 3958.8

    static func haversineMiles(lat1: Double, lon1: Double, lat2: Double, lon2: Double) -> Double {
        let r = earthRadiusMiles
        let dLat = (lat2 - lat1) * .pi / 180
        let dLon = (lon2 - lon1) * .pi / 180
        let a = sin(dLat / 2) * sin(dLat / 2)
            + cos(lat1 * .pi / 180) * cos(lat2 * .pi / 180) * sin(dLon / 2) * sin(dLon / 2)
        return 2 * r * asin(min(1, sqrt(a)))
    }

    /// Website-aligned radius filter: dealer geopoint first, then car ZIP map, then include if unknown.
    static func filterByRadius(
        cars: [ListingCar],
        origin: (lat: Double, lon: Double),
        radiusMi: Double,
        zipCoords: [String: (lat: Double, lon: Double)],
        dealerCoords: [String: (lat: Double, lon: Double)]
    ) -> [ListingCar] {
        cars.filter { car in
            let coords: (lat: Double, lon: Double)? = {
                if let url = car.dealerUrl?.trimmingCharacters(in: .whitespacesAndNewlines),
                   !url.isEmpty,
                   let pair = dealerCoords[url] {
                    return pair
                }
                if let z = car.zipCode?.trimmingCharacters(in: .whitespacesAndNewlines),
                   z.count >= 5 {
                    let key = String(z.prefix(5))
                    if let pair = zipCoords[key] { return pair }
                }
                return nil
            }()
            guard let coords else { return true }
            return haversineMiles(
                lat1: origin.lat, lon1: origin.lon,
                lat2: coords.lat, lon2: coords.lon
            ) <= radiusMi
        }
    }
}

/// Caches ZIP → coordinates from `/api/zip-coords`.
actor ZipCoordCache {
    static let shared = ZipCoordCache()

    private var cache: [String: (lat: Double, lon: Double)] = [:]

    func coords(for zip: String, api: APIClient = .shared) async throws -> (lat: Double, lon: Double)? {
        let key = zip.trimmingCharacters(in: .whitespacesAndNewlines)
        guard key.count == 5, key.allSatisfy(\.isNumber) else { return nil }
        if let hit = cache[key] { return hit }
        let pair = try await api.fetchZipCoords(zip: key)
        cache[key] = pair
        return pair
    }

    func clear() {
        cache.removeAll()
    }
}

/// Bulk ZIP + dealer coordinate maps from `GET /api/listings/geo-coords`.
actor ListingsGeoMapsCache {
    static let shared = ListingsGeoMapsCache()

    private var zipCoords: [String: (lat: Double, lon: Double)] = [:]
    private var dealerCoords: [String: (lat: Double, lon: Double)] = [:]
    private var loaded = false

    func maps() async throws -> (
        zip: [String: (lat: Double, lon: Double)],
        dealer: [String: (lat: Double, lon: Double)]
    ) {
        if !loaded {
            try await reload()
        }
        return (zipCoords, dealerCoords)
    }

    func reload() async throws {
        let resp = try await APIClient.shared.fetchListingsGeoCoords()
        zipCoords = Self.parseCoordMap(resp.zipCoords ?? [:])
        dealerCoords = Self.parseCoordMap(resp.dealerCoords ?? [:])
        loaded = true
    }

    func clear() {
        zipCoords.removeAll()
        dealerCoords.removeAll()
        loaded = false
    }

    private static func parseCoordMap(_ raw: [String: [Double]]) -> [String: (lat: Double, lon: Double)] {
        var out: [String: (lat: Double, lon: Double)] = [:]
        for (key, arr) in raw where arr.count >= 2 {
            out[key] = (arr[0], arr[1])
        }
        return out
    }
}
