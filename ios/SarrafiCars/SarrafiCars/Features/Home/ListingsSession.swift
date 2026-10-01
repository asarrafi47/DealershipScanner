import Foundation

/// Website-aligned search radius options (miles).
enum ListingsRadius: Int, CaseIterable, Identifiable {
    case mi10 = 10
    case mi25 = 25
    case mi50 = 50
    case mi100 = 100
    case mi200 = 200
    case mi500 = 500

    var id: Int { rawValue }
    var label: String { "\(rawValue) mi" }
}

struct ListingsFilters: Equatable {
    var maxPrice: Int?
    var maxMileage: Int?
    var makes: Set<String> = []
    var models: Set<String> = []
    var trims: Set<String> = []
    var drivetrains: Set<String> = []
    var bodyStyles: Set<String> = []
    var fuelTypes: Set<String> = []
    var transmissions: Set<String> = []
    var cylinders: Set<Int> = []
    var exteriorColors: Set<String> = []
    var interiorColors: Set<String> = []
    var packageNames: Set<String> = []

    // Premium-only: filter by specific dealerships nearby.
    var dealershipRegistryIds: Set<Int> = []

    static let priceOptions: [Int?] = [nil, 25_000, 35_000, 50_000, 75_000, 100_000, 150_000]
    static let mileageOptions: [Int?] = [nil, 5_000, 10_000, 15_000, 20_000, 25_000, 30_000, 40_000, 50_000, 75_000, 100_000]

    var activeCount: Int {
        var n = 0
        if maxPrice != nil { n += 1 }
        if maxMileage != nil { n += 1 }
        n += makes.count + models.count + trims.count + drivetrains.count + bodyStyles.count + fuelTypes.count + transmissions.count
        n += cylinders.count + exteriorColors.count + interiorColors.count + packageNames.count
        n += dealershipRegistryIds.count
        return n
    }
}

enum ListingsFilterEngine {
    static func apply(_ filters: ListingsFilters, to cars: [ListingCar]) -> [ListingCar] {
        cars.filter { car in
            if let maxP = filters.maxPrice, let price = car.price, price > Double(maxP) { return false }
            if let maxM = filters.maxMileage, let miles = car.mileage, miles > maxM { return false }
            if !filters.makes.isEmpty, !containsCI(filters.makes, car.make) { return false }
            if !filters.models.isEmpty, !matchesModel(filters.models, car.model) { return false }
            if !filters.trims.isEmpty, !containsCI(filters.trims, car.trim) { return false }
            if !filters.drivetrains.isEmpty, !containsCI(filters.drivetrains, car.drivetrain) { return false }
            if !filters.bodyStyles.isEmpty, !containsCI(filters.bodyStyles, car.bodyStyle) { return false }
            if !filters.fuelTypes.isEmpty, !containsCI(filters.fuelTypes, car.fuelType) { return false }
            if !filters.transmissions.isEmpty, !containsCI(filters.transmissions, car.transmission) { return false }

            if !filters.cylinders.isEmpty {
                guard let cyl = car.cylinders else { return false }
                if !filters.cylinders.contains(cyl) { return false }
            }

            if !filters.exteriorColors.isEmpty, !containsCI(filters.exteriorColors, car.exteriorColor) { return false }
            if !filters.interiorColors.isEmpty, !containsCI(filters.interiorColors, car.interiorColor) { return false }

            if !filters.packageNames.isEmpty {
                let selected = filters.packageNames.map { $0.lowercased() }
                let carPackages = (car.packageNames ?? []).map { $0.lowercased() }
                if !carPackages.contains(where: { selected.contains($0) }) { return false }
            }

            if !filters.dealershipRegistryIds.isEmpty {
                guard let regId = car.dealershipRegistryId else { return false }
                if !filters.dealershipRegistryIds.contains(regId) { return false }
            }
            return true
        }
    }

    static func facetValues(in cars: [ListingCar]) -> (
        makes: [String],
        models: [String],
        trims: [String],
        drivetrains: [String],
        bodyStyles: [String],
        fuelTypes: [String],
        transmissions: [String],
        cylinders: [Int],
        exteriorColors: [String],
        interiorColors: [String],
        packageNames: [String]
    ) {
        func uniq(_ keyPath: KeyPath<ListingCar, String?>) -> [String] {
            Array(Set(cars.compactMap { $0[keyPath: keyPath] }.filter { !$0.isEmpty && $0 != "—" })).sorted()
        }

        func uniqInts(_ keyPath: KeyPath<ListingCar, Int?>) -> [Int] {
            Array(Set(cars.compactMap { $0[keyPath: keyPath] })).sorted()
        }

        let packages: [String] = cars
            .flatMap { $0.packageNames ?? [] }
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty && $0 != "—" }

        return (
            makes: uniq(\.make),
            models: uniq(\.model),
            trims: uniq(\.trim),
            drivetrains: uniq(\.drivetrain),
            bodyStyles: uniq(\.bodyStyle),
            fuelTypes: uniq(\.fuelType),
            transmissions: uniq(\.transmission),
            cylinders: uniqInts(\.cylinders),
            exteriorColors: uniq(\.exteriorColor),
            interiorColors: uniq(\.interiorColor),
            packageNames: Array(Set(packages)).sorted()
        )
    }

    private static func containsCI(_ set: Set<String>, _ value: String?) -> Bool {
        guard let value, !value.isEmpty, value != "—" else { return false }
        let low = value.lowercased()
        return set.contains { $0.lowercased() == low }
    }

    /// Website allows model checkbox values to prefix-match (e.g. filter ``2`` → ``2 Series``).
    private static func matchesModel(_ set: Set<String>, _ value: String?) -> Bool {
        guard let value, !value.isEmpty, value != "—" else { return false }
        let low = value.lowercased()
        return set.contains { sel in
            let s = sel.lowercased()
            if s == low { return true }
            return s.count >= 2 && low.hasPrefix(s)
        }
    }
}

/// Website-aligned facet catalog for Make / Model / Trim / etc. (not limited to current result set).
@MainActor
final class ListingsFacetCatalog: ObservableObject {
    @Published private(set) var makes: [String] = []
    @Published private(set) var modelRows: [(make: String, model: String)] = []
    @Published private(set) var trimRows: [(make: String, model: String, trim: String)] = []
    @Published private(set) var fuelTypes: [String] = []
    @Published private(set) var cylinders: [Int] = []
    @Published private(set) var transmissions: [String] = []
    @Published private(set) var drivetrains: [String] = []
    @Published private(set) var bodyStyles: [String] = []
    @Published private(set) var exteriorColors: [String] = []
    @Published private(set) var interiorColors: [String] = []
    @Published private(set) var packageNames: [String] = []
    @Published private(set) var isLoading = false

    private var didLoad = false

    /// Loads the server facet catalog once. When that fails or comes back empty, falls
    /// back to the cars of one area: `/api/listings/cars` is scoped to a ZIP + radius and
    /// has no whole-fleet answer, so without a valid `zip` there is no fallback.
    func loadIfNeeded(zip: String? = nil, radiusMiles: Int = ListingsRadius.mi50.rawValue) async {
        guard !didLoad else { return }
        isLoading = true
        defer {
            isLoading = false
            didLoad = true
        }
        do {
            let resp = try await APIClient.shared.fetchListingsFilterOptions()
            apply(response: resp)
            if makes.isEmpty {
                await applyFromArea(zip: zip, radiusMiles: radiusMiles)
            }
        } catch {
            await applyFromArea(zip: zip, radiusMiles: radiusMiles)
        }
    }

    private func applyFromArea(zip: String?, radiusMiles: Int) async {
        guard let zip, APIClient.listingsCarsPath(zip: zip, radiusMiles: radiusMiles) != nil else { return }
        // Result-set facets still apply after search, so a failure here is not fatal.
        if let cars = try? await APIClient.shared.fetchListings(zip: zip, radiusMiles: radiusMiles) {
            applyFromCars(cars)
        }
    }

    func applyFromCars(_ cars: [ListingCar]) {
        let f = ListingsFilterEngine.facetValues(in: cars)
        if makes.isEmpty { makes = FilterOptionDedupe.strings(f.makes) }
        if fuelTypes.isEmpty { fuelTypes = f.fuelTypes }
        if cylinders.isEmpty { cylinders = f.cylinders }
        if transmissions.isEmpty { transmissions = f.transmissions }
        if drivetrains.isEmpty { drivetrains = f.drivetrains }
        if bodyStyles.isEmpty { bodyStyles = f.bodyStyles }
        if exteriorColors.isEmpty { exteriorColors = f.exteriorColors }
        if interiorColors.isEmpty { interiorColors = f.interiorColors }
        if packageNames.isEmpty { packageNames = f.packageNames }
        if modelRows.isEmpty {
            var seen = Set<String>()
            var rows: [(String, String)] = []
            for car in cars {
                guard let make = car.make, let model = car.model,
                      !make.isEmpty, make != "—", !model.isEmpty, model != "—" else { continue }
                let key = "\(make.lowercased())|\(model.lowercased())"
                if seen.insert(key).inserted { rows.append((make, model)) }
            }
            modelRows = rows.sorted { $0.0.localizedCaseInsensitiveCompare($1.0) == .orderedAscending }
        }
        if trimRows.isEmpty {
            var seen = Set<String>()
            var rows: [(String, String, String)] = []
            for car in cars {
                guard let make = car.make, let model = car.model, let trim = car.trim,
                      !make.isEmpty, !model.isEmpty, !trim.isEmpty, trim != "—" else { continue }
                let key = "\(make.lowercased())|\(model.lowercased())|\(trim.lowercased())"
                if seen.insert(key).inserted { rows.append((make, model, trim)) }
            }
            trimRows = rows.sorted { $0.0.localizedCaseInsensitiveCompare($1.0) == .orderedAscending }
        }
    }

    private func apply(response: ListingsFilterOptionsResponse) {
        makes = FilterOptionDedupe.strings(response.makes ?? [])
        modelRows = (response.modelRows ?? []).compactMap { row -> (String, String)? in
            if row.count >= 2 { return (row[0], row[1]) }
            return nil
        }
        trimRows = (response.trimRows ?? []).compactMap { row -> (String, String, String)? in
            if row.count >= 3 { return (row[0], row[1], row[2]) }
            return nil
        }
        fuelTypes = response.fuelTypes ?? []
        cylinders = response.cylinders ?? []
        transmissions = response.transmissions ?? []
        drivetrains = response.drivetrains ?? []
        bodyStyles = response.bodyStyles ?? []
        exteriorColors = response.exteriorColors ?? []
        interiorColors = response.interiorColors ?? []
        if let pkgs = response.allPackageNames, !pkgs.isEmpty {
            packageNames = pkgs
        }
    }

    func models(for selectedMakes: Set<String>) -> [String] {
        let rows = modelRows.filter { selectedMakes.isEmpty || selectedMakes.contains($0.make) }
        return Array(Set(rows.map(\.model))).sorted()
    }

    func trims(for selectedMakes: Set<String>, models selectedModels: Set<String>) -> [String] {
        let rows = trimRows.filter { row in
            (selectedMakes.isEmpty || selectedMakes.contains(row.make))
                && (selectedModels.isEmpty || selectedModels.contains(row.model))
        }
        return Array(Set(rows.map(\.trim).filter { !$0.isEmpty && $0 != "—" })).sorted()
    }
}

/// The ZIP + radius one listings request is scoped to.
struct ListingsArea: Equatable {
    let zip: String
    let radiusMiles: Int
}

@MainActor
final class ListingsSession: ObservableObject {
    @Published var zipCode: String = ""
    @Published var radiusMiles: Int = UserDefaults.standard.object(forKey: "listings_radius") as? Int ?? ListingsRadius.mi25.rawValue {
        didSet { UserDefaults.standard.set(radiusMiles, forKey: "listings_radius") }
    }
    @Published var searchQuery: String = ""
    @Published var filters = ListingsFilters() {
        didSet { applyLocalFilters() }
    }

    @Published private(set) var displayedCars: [ListingCar] = []
    @Published private(set) var facetPool: [ListingCar] = []
    @Published private(set) var isLoading = false
    @Published private(set) var geoHint: String?
    @Published private(set) var emptyMessage: String?
    @Published private(set) var errorMessage: String?
    /// True when the server answered `zip_required`: the UI asks for a ZIP instead of
    /// showing a generic error.
    @Published private(set) var needsZip = false
    /// Set when remembering the ZIP + radius in the server session failed. Listings still
    /// load (every request carries the area explicitly), but dashboard recommendations
    /// that read the remembered area will not match, so the user is told.
    @Published private(set) var sessionSaveError: String?
    @Published var visibleCount = AppConfig.pageSize

    let facetCatalog = ListingsFacetCatalog()

    private var allCars: [ListingCar] = []
    /// The ZIP + radius `allCars` was fetched for; the server scopes cars to that area.
    private var allCarsArea: ListingsArea?
    private var smartSearchActive = false

    var isZipValid: Bool {
        let z = zipCode.trimmingCharacters(in: .whitespacesAndNewlines)
        return z.count == 5 && z.allSatisfy(\.isNumber)
    }

    /// - Parameter forceReloadInventory: When true, refetches `/api/listings/cars` (pull-to-refresh).
    func refresh(forceReloadInventory: Bool = false) async {
        guard isZipValid else {
            promptForZip()
            return
        }
        isLoading = true
        errorMessage = nil
        needsZip = false
        geoHint = nil
        emptyMessage = nil
        defer { isLoading = false }

        let zip = zipCode.trimmingCharacters(in: .whitespacesAndNewlines)
        let area = ListingsArea(zip: zip, radiusMiles: radiusMiles)
        do {
            guard let origin = try await ZipCoordCache.shared.coords(for: zip) else {
                geoHint = "ZIP code not found — check and try again."
                displayedCars = []
                return
            }
            await rememberArea(area)

            if forceReloadInventory || allCarsArea != area {
                allCars = try await APIClient.shared.fetchListings(zip: zip, radiusMiles: radiusMiles)
                allCarsArea = area
                // The coord maps are fleet-wide (not area-scoped): `maps()` loads them once;
                // only pull-to-refresh refetches, not every ZIP or radius change.
                if forceReloadInventory {
                    try await ListingsGeoMapsCache.shared.reload()
                }
                await facetCatalog.loadIfNeeded(zip: zip, radiusMiles: radiusMiles)
                if facetCatalog.makes.isEmpty {
                    facetCatalog.applyFromCars(allCars)
                }
            }

            let geoMaps = try await ListingsGeoMapsCache.shared.maps()
            let q = searchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
            var pool: [ListingCar]
            if q.isEmpty {
                smartSearchActive = false
                pool = ListingsGeo.filterByRadius(
                    cars: allCars,
                    origin: origin,
                    radiusMi: Double(radiusMiles),
                    zipCoords: geoMaps.zip,
                    dealerCoords: geoMaps.dealer
                )
            } else {
                smartSearchActive = true
                let resp = try await APIClient.shared.smartSearch(
                    query: q,
                    zip: zip,
                    radius: Double(radiusMiles)
                )
                pool = resp.results
                emptyMessage = resp.emptyMessage
            }

            facetPool = pool
            applyLocalFilters()
            visibleCount = AppConfig.pageSize

            if displayedCars.isEmpty {
                if let emptyMessage, !emptyMessage.isEmpty {
                    // keep smart-search message
                } else if pool.isEmpty {
                    geoHint = "No listings within \(radiusMiles) mi of \(zip)."
                } else {
                    geoHint = "No matches with current filters."
                }
            }
        } catch APIError.zipRequired {
            promptForZip()
        } catch {
            errorMessage = error.localizedDescription
            displayedCars = []
        }
    }

    /// No usable ZIP (none typed, or the server answered `zip_required`): ask for one.
    private func promptForZip() {
        needsZip = true
        errorMessage = nil
        geoHint = APIError.zipRequired.errorDescription
        allCars = []
        allCarsArea = nil
        displayedCars = []
        facetPool = []
    }

    /// Remembers the area in the server session (dashboard recommendations read it).
    /// A failure is surfaced in `sessionSaveError`, not swallowed; it does not block the
    /// listings fetch, which carries the area explicitly.
    private func rememberArea(_ area: ListingsArea) async {
        do {
            try await APIClient.shared.persistListingsGeo(zip: area.zip, radiusMiles: area.radiusMiles)
            sessionSaveError = nil
        } catch {
            sessionSaveError = "Could not save your ZIP for recommendations: \(error.localizedDescription)"
        }
    }

    func applyLocalFilters() {
        displayedCars = ListingsFilterEngine.apply(filters, to: facetPool)
        if displayedCars.isEmpty, !facetPool.isEmpty {
            geoHint = "No matches with current filters."
        } else if displayedCars.isEmpty {
            geoHint = geoHint ?? "No listings in this area."
        } else {
            geoHint = nil
        }
    }

    func clearFilters() {
        filters = ListingsFilters()
        applyLocalFilters()
    }
}
