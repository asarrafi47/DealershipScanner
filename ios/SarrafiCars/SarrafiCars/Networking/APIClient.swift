import Foundation

enum APIError: LocalizedError {
    case invalidURL
    case httpStatus(Int, String?)
    case server(String)
    case decoding(Error)
    case unreachable(baseURL: URL)

    var errorDescription: String? {
        switch self {
        case .invalidURL:
            return "Invalid URL"
        case .httpStatus(let code, let msg):
            return msg ?? "Request failed (\(code))"
        case .server(let msg):
            return msg
        case .unreachable(let baseURL):
            #if DEBUG
            return """
            Cannot reach the API at \(baseURL.absoluteString).
            From the repo root run: ./start.sh
            Simulator: use http://127.0.0.1:5001. On a physical iPhone set SARRAFI_API_BASE_URL in Info.plist to http://<your-mac-ip>:5001.
            """
            #else
            return "Could not connect to the server."
            #endif
        case .decoding(let underlying):
            #if DEBUG
            return "Could not read server response (\(underlying.localizedDescription))"
            #else
            return "Could not read server response"
            #endif
        }
    }
}

final class APIClient {
    static let shared = APIClient()

    private let baseURL: URL
    private let session: URLSession
    private let sessionDelegate = PinningURLSessionDelegate()
    private(set) var csrfToken: String?

    private var decoder: JSONDecoder {
        let d = JSONDecoder()
        d.keyDecodingStrategy = .convertFromSnakeCase
        return d
    }

    init(baseURL: URL = AppConfig.baseURL) {
        self.baseURL = baseURL
        let config = URLSessionConfiguration.default
        config.httpCookieStorage = HTTPCookieStorage.shared
        config.httpShouldSetCookies = true
        config.httpCookieAcceptPolicy = .always
        self.session = URLSession(
            configuration: config,
            delegate: sessionDelegate,
            delegateQueue: nil
        )
    }

    func refreshCSRF() async throws {
        let resp: CSRFResponse = try await get("/api/auth/csrf")
        guard resp.ok else { throw APIError.server("csrf_failed") }
        csrfToken = resp.csrfToken
    }

    func fetchMe() async throws -> AuthUser? {
        do {
            let resp: AuthMeResponse = try await get("/api/auth/me")
            return resp.user
        } catch APIError.httpStatus(401, _) {
            return nil
        }
    }

    func login(login: String, password: String) async throws -> AuthUser {
        try await refreshCSRF()
        let body = ["login": login, "password": password]
        let resp: AuthLoginResponse = try await postJSON("/api/auth/login", body: body, csrf: true)
        guard resp.ok, let user = resp.user else {
            throw APIError.server(resp.error ?? "login_failed")
        }
        await SessionCookieBridge.syncToWebView()
        return user
    }

    func register(username: String, email: String, password: String, plan: String = "free") async throws -> AuthUser {
        try await refreshCSRF()
        let body: [String: Any] = [
            "username": username,
            "email": email,
            "password": password,
            "plan": plan,
        ]
        let resp: AuthLoginResponse = try await postJSON("/api/auth/register", body: body, csrf: true)
        guard resp.ok, let user = resp.user else {
            let code = resp.message ?? resp.error ?? "register_failed"
            throw APIError.server(code)
        }
        await SessionCookieBridge.syncToWebView()
        return user
    }

    func logout() async throws {
        try await refreshCSRF()
        let _: OKResponse = try await postJSON("/api/auth/logout", body: [String: String](), csrf: true)
        csrfToken = nil
        await SessionCookieBridge.clearSessionCookies()
    }

    func fetchListings() async throws -> [ListingCar] {
        let resp: ListingsResponse = try await get("/api/listings/cars")
        guard resp.ok else { throw APIError.server("listings_failed") }
        return resp.cars
    }

    func fetchListingsFilterOptions() async throws -> ListingsFilterOptionsResponse {
        let resp: ListingsFilterOptionsResponse = try await get("/api/listings/filter-options")
        guard resp.ok else { throw APIError.server("filter_options_failed") }
        return resp
    }

    func fetchListingsGeoCoords() async throws -> ListingsGeoCoordsResponse {
        let resp: ListingsGeoCoordsResponse = try await get("/api/listings/geo-coords")
        guard resp.ok else { throw APIError.server("geo_coords_failed") }
        return resp
    }

    func fetchZipCoords(zip: String) async throws -> (lat: Double, lon: Double) {
        struct ZipResp: Decodable {
            let lat: Double
            let lon: Double
        }
        let z = zip.trimmingCharacters(in: .whitespacesAndNewlines)
        let resp: ZipResp = try await get("/api/zip-coords?zip=\(z)")
        return (resp.lat, resp.lon)
    }

    func persistListingsGeo(zip: String, radiusMiles: Int) async throws {
        try await refreshCSRF()
        let body: [String: Any] = ["zip_code": zip, "radius": radiusMiles]
        let _: OKResponse = try await postJSON("/api/session/listings-geo", body: body, csrf: true)
    }

    func smartSearch(query: String, zip: String, radius: Double) async throws -> SmartSearchResponse {
        let body: [String: Any] = [
            "query": query,
            "q": query,
            "zip_code": zip,
            "radius": radius,
        ]
        do {
            try await refreshCSRF()
            return try await postJSON("/api/search/smart", body: body, csrf: true)
        } catch APIError.httpStatus(403, _) {
            try await refreshCSRF()
            return try await postJSON("/api/search/smart", body: body, csrf: true)
        }
    }

    func fetchCarDetail(id: Int) async throws -> CarDetailResponse {
        let resp: CarDetailResponse = try await get("/api/cars/\(id)")
        guard resp.ok else { throw APIError.server(resp.error ?? "not_found") }
        return resp
    }

    func fetchSavedCars() async throws -> [ListingCar] {
        let resp: ListingsResponse = try await get("/api/saved-cars")
        guard resp.ok else { throw APIError.server("saved_failed") }
        return resp.cars
    }

    func toggleSave(carId: Int) async throws -> Bool {
        try await refreshCSRF()
        let resp: SaveToggleResponse = try await postJSON(
            "/api/cars/\(carId)/save",
            body: [String: String](),
            csrf: true
        )
        guard resp.ok, let saved = resp.saved else {
            throw APIError.server(resp.error ?? "save_failed")
        }
        return saved
    }

    func fetchNearbyDealers(
        lat: Double? = nil,
        lon: Double? = nil,
        zip: String? = nil,
        radius: Double
    ) async throws -> DealerLocatorResponse {
        var parts: [String] = ["radius=\(radius)"]
        if let lat, let lon {
            parts.append("lat=\(lat)")
            parts.append("lon=\(lon)")
        }
        if let zip, !zip.isEmpty {
            parts.append("zip=\(zip.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? zip)")
        }
        let path = "/api/dealer-locator?" + parts.joined(separator: "&")
        return try await get(path)
    }

    // MARK: - HTTP

    private func get<T: Decodable>(_ path: String) async throws -> T {
        guard let url = URL(string: path, relativeTo: baseURL) else {
            throw APIError.invalidURL
        }
        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.setValue("application/json", forHTTPHeaderField: "Accept")
        let data = try await data(for: req)
        return try decode(data)
    }

    private func postJSON<T: Decodable>(
        _ path: String,
        body: [String: Any],
        csrf: Bool
    ) async throws -> T {
        guard let url = URL(string: path, relativeTo: baseURL) else {
            throw APIError.invalidURL
        }
        var req = URLRequest(url: url)
        req.httpMethod = "POST"
        req.setValue("application/json", forHTTPHeaderField: "Content-Type")
        req.setValue("application/json", forHTTPHeaderField: "Accept")
        if csrf {
            guard let csrfToken else {
                throw APIError.server("csrf_required")
            }
            req.setValue(csrfToken, forHTTPHeaderField: "X-CSRF-Token")
        }
        req.httpBody = try JSONSerialization.data(withJSONObject: body)
        let data = try await data(for: req)
        return try decode(data)
    }

    private func data(for request: URLRequest) async throws -> Data {
        let data: Data
        let response: URLResponse
        do {
            (data, response) = try await session.data(for: request)
        } catch {
            throw mapTransportError(error)
        }
        guard let http = response as? HTTPURLResponse else {
            throw APIError.server("bad_response")
        }
        if http.statusCode == 304 {
            return data
        }
        if !(200 ... 299).contains(http.statusCode) {
            let msg = (try? decoder.decode(OKResponse.self, from: data))?.error
            throw APIError.httpStatus(http.statusCode, msg)
        }
        return data
    }

    private func mapTransportError(_ error: Error) -> Error {
        guard let urlError = error as? URLError else { return error }
        switch urlError.code {
        case .cannotConnectToHost, .cannotFindHost, .networkConnectionLost,
             .notConnectedToInternet, .timedOut, .dnsLookupFailed:
            return APIError.unreachable(baseURL: baseURL)
        default:
            return error
        }
    }

    private func decode<T: Decodable>(_ data: Data) throws -> T {
        do {
            return try decoder.decode(T.self, from: data)
        } catch {
            throw APIError.decoding(error)
        }
    }
}
