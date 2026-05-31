import Foundation

struct ListingCar: Identifiable, Decodable, Hashable {
    let id: Int
    let title: String?
    let year: Int?
    let make: String?
    let model: String?
    let trim: String?
    let cylinders: Int?
    let price: Double?
    let mileage: Int?
    let imageUrl: String?
    let gallery: [String]?
    let dealerName: String?
    let exteriorColor: String?
    let interiorColor: String?
    let drivetrain: String?
    let transmission: String?
    let bodyStyle: String?
    let fuelType: String?
    let engineL: Double?
    let packageNames: [String]?
    let dealershipRegistryId: Int?
    let zipCode: String?
    let dealerUrl: String?

    private enum CodingKeys: String, CodingKey {
        case id, title, year, make, model, trim, cylinders, price, mileage, gallery
        case imageUrl = "image_url"
        case dealerUrl = "dealer_url"
        case dealerName = "dealer_name"
        case exteriorColor = "exterior_color"
        case interiorColor = "interior_color"
        case drivetrain, transmission
        case bodyStyle = "body_style"
        case fuelType = "fuel_type"
        case engineL = "engine_l"
        case packageNames = "package_names"
        case dealershipRegistryId = "dealership_registry_id"
        case zipCode = "zip_code"
    }

    init(
        id: Int,
        title: String? = nil,
        year: Int? = nil,
        make: String? = nil,
        model: String? = nil,
        trim: String? = nil,
        cylinders: Int? = nil,
        price: Double? = nil,
        mileage: Int? = nil,
        imageUrl: String? = nil,
        gallery: [String]? = nil,
        dealerName: String? = nil,
        exteriorColor: String? = nil,
        interiorColor: String? = nil,
        drivetrain: String? = nil,
        transmission: String? = nil,
        bodyStyle: String? = nil,
        fuelType: String? = nil,
        engineL: Double? = nil,
        packageNames: [String]? = nil,
        dealershipRegistryId: Int? = nil,
        zipCode: String? = nil,
        dealerUrl: String? = nil
    ) {
        self.id = id
        self.title = title
        self.year = year
        self.make = make
        self.model = model
        self.trim = trim
        self.cylinders = cylinders
        self.price = price
        self.mileage = mileage
        self.imageUrl = imageUrl
        self.gallery = gallery
        self.dealerName = dealerName
        self.exteriorColor = exteriorColor
        self.interiorColor = interiorColor
        self.drivetrain = drivetrain
        self.transmission = transmission
        self.bodyStyle = bodyStyle
        self.fuelType = fuelType
        self.engineL = engineL
        self.packageNames = packageNames
        self.dealershipRegistryId = dealershipRegistryId
        self.zipCode = zipCode
        self.dealerUrl = dealerUrl
    }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        id = try c.decode(Int.self, forKey: .id)
        title = try c.decodeIfPresent(String.self, forKey: .title)
        year = try c.decodeIfPresent(Int.self, forKey: .year)
        make = try c.decodeIfPresent(String.self, forKey: .make)
        model = try c.decodeIfPresent(String.self, forKey: .model)
        trim = try c.decodeIfPresent(String.self, forKey: .trim)
        cylinders = try c.decodeIfPresent(Int.self, forKey: .cylinders)
        price = try c.decodeIfPresent(Double.self, forKey: .price)
        mileage = try c.decodeIfPresent(Int.self, forKey: .mileage)
        imageUrl = try c.decodeIfPresent(String.self, forKey: .imageUrl)
        gallery = try c.decodeIfPresent([String].self, forKey: .gallery)
        dealerName = try c.decodeIfPresent(String.self, forKey: .dealerName)
        exteriorColor = try c.decodeIfPresent(String.self, forKey: .exteriorColor)
        interiorColor = try c.decodeIfPresent(String.self, forKey: .interiorColor)
        drivetrain = try c.decodeIfPresent(String.self, forKey: .drivetrain)
        transmission = try c.decodeIfPresent(String.self, forKey: .transmission)
        bodyStyle = try c.decodeIfPresent(String.self, forKey: .bodyStyle)
        fuelType = try c.decodeIfPresent(String.self, forKey: .fuelType)
        engineL = Self.decodeFlexibleDouble(c, forKey: .engineL)
        packageNames = try c.decodeIfPresent([String].self, forKey: .packageNames)
        dealershipRegistryId = try c.decodeIfPresent(Int.self, forKey: .dealershipRegistryId)
        zipCode = try c.decodeIfPresent(String.self, forKey: .zipCode)
        dealerUrl = try c.decodeIfPresent(String.self, forKey: .dealerUrl)
    }

    /// API may send ``engine_l`` as a string (e.g. ``"6.2"``) or a number.
    private static func decodeFlexibleDouble(
        _ c: KeyedDecodingContainer<CodingKeys>,
        forKey key: CodingKeys
    ) -> Double? {
        guard c.contains(key) else { return nil }
        if let n = try? c.decode(Double.self, forKey: key) {
            return n
        }
        guard let s = try? c.decode(String.self, forKey: key), !s.isEmpty else {
            return nil
        }
        return Double(s)
    }

    var headline: String {
        if let title, !title.isEmpty, title != "—" { return title }
        let parts = [year.map(String.init), make, model, trim]
            .compactMap { $0 }
            .filter { !$0.isEmpty && $0 != "—" }
        return parts.isEmpty ? "Vehicle" : parts.joined(separator: " ")
    }

    var primaryImagePath: String? {
        if let gallery, let first = gallery.first, !first.isEmpty { return first }
        return imageUrl
    }
}

struct AuthUser: Codable {
    let id: Int
    let username: String?
    let email: String?
    let isPremium: Bool?
    let hasPaidAccess: Bool?
}

struct CarDetailPayload: Codable {
    let car: DetailCar
    let galleryImages: [String]?
    let carIsSaved: Bool?
    let loggedIn: Bool?
    let marketIntel: MarketIntel?
    let dealerInfo: DealerInfo?
}

struct DetailCar: Codable {
    let id: Int?
    let title: String?
    let year: Int?
    let make: String?
    let model: String?
    let trim: String?
    let price: Double?
    let mileage: Int?
    let vin: String?
    let exteriorColor: String?
    let interiorColor: String?
    let drivetrain: String?
    let transmission: String?
    let fuelType: String?
    let bodyStyle: String?
    let engineDescription: String?
    let dealerName: String?

    var headline: String {
        if let title, !title.isEmpty, title != "—" { return title }
        let parts = [year.map(String.init), make, model, trim]
            .compactMap { $0 }
            .filter { !$0.isEmpty && $0 != "—" }
        return parts.isEmpty ? "Vehicle" : parts.joined(separator: " ")
    }
}

struct MarketIntel: Codable {
    let avgPrice: Double?
    let sampleCount: Int?
    let vsMarketLabel: String?
}

struct DealerInfo: Codable {
    let name: String?
    let city: String?
    let state: String?
}

struct CSRFResponse: Decodable {
    let ok: Bool
    let csrfToken: String
}

struct OKResponse: Decodable {
    let ok: Bool
    let error: String?
}

struct AuthMeResponse: Decodable {
    let ok: Bool
    let user: AuthUser?
    let error: String?
}

struct AuthLoginResponse: Decodable {
    let ok: Bool
    let user: AuthUser?
    let error: String?
    let message: String?
}

struct ListingsResponse: Decodable {
    let ok: Bool
    let cars: [ListingCar]
}

/// Server-side facet catalog (`GET /api/listings/filter-options`).
struct ListingsGeoCoordsResponse: Decodable {
    let ok: Bool
    let zipCoords: [String: [Double]]?
    let dealerCoords: [String: [Double]]?
}

struct ListingsFilterOptionsResponse: Decodable {
    let ok: Bool
    let makes: [String]?
    let modelRows: [[String]]?
    let trimRows: [[String]]?
    let fuelTypes: [String]?
    let cylinders: [Int]?
    let transmissions: [String]?
    let drivetrains: [String]?
    let bodyStyles: [String]?
    let exteriorColors: [String]?
    let interiorColors: [String]?
    let packageRows: [[String]]?
    let allPackageNames: [String]?
}

struct CarDetailResponse: Decodable {
    let ok: Bool
    let car: DetailCar?
    let galleryImages: [String]?
    let carIsSaved: Bool?
    let loggedIn: Bool?
    let marketIntel: MarketIntel?
    let dealerInfo: DealerInfo?
    let error: String?
}

struct SmartSearchResponse: Decodable {
    let results: [ListingCar]
    let emptyMessage: String?
}

struct SaveToggleResponse: Decodable {
    let ok: Bool
    let saved: Bool?
    let error: String?
}
