import Foundation

@MainActor
final class AppState: ObservableObject {
    @Published var user: AuthUser?
    @Published var isLoadingSession = true
    @Published var authError: String?
    @Published private(set) var savedCarIds: Set<Int> = []

    private let api = APIClient.shared

    func bootstrap() async {
        isLoadingSession = true
        defer { isLoadingSession = false }
        do {
            _ = try await api.refreshCSRF()
            user = try await api.fetchMe()
            SessionCookieBridge.syncToWebView()
            await refreshSavedCarIds()
        } catch {
            user = nil
            savedCarIds = []
        }
    }

    func login(login: String, password: String) async -> Bool {
        authError = nil
        do {
            user = try await api.login(login: login, password: password)
            await refreshSavedCarIds()
            return true
        } catch {
            authError = error.localizedDescription
            return false
        }
    }

    func register(username: String, email: String, password: String, plan: String = "free") async -> Bool {
        authError = nil
        do {
            user = try await api.register(username: username, email: email, password: password, plan: plan)
            await refreshSavedCarIds()
            return true
        } catch {
            authError = friendlyAuthError(error)
            return false
        }
    }

    /// After embedded web auth, pull session cookies into URLSession before ``bootstrap()``.
    func adoptWebSession() async {
        await SessionCookieBridge.syncFromWebView()
        await bootstrap()
    }

    private func friendlyAuthError(_ error: Error) -> String {
        if case APIError.server(let code) = error {
            switch code {
            case "duplicate_user":
                return "That username or email is already registered."
            case "validation_error":
                return "Check your username, email, and password."
            case "rate_limited":
                return "Too many attempts. Try again in a minute."
            default:
                return code.replacingOccurrences(of: "_", with: " ").capitalized
            }
        }
        return error.localizedDescription
    }

    func logout() async {
        authError = nil
        do {
            try await api.logout()
        } catch {
            authError = error.localizedDescription
        }
        user = nil
        savedCarIds = []
    }

    func refreshSavedCarIds() async {
        guard isLoggedIn else {
            savedCarIds = []
            return
        }
        do {
            _ = try await fetchAndSyncSavedCars()
        } catch {
            // Keep prior set on transient errors.
        }
    }

    /// Fetches saved listings and updates ``savedCarIds`` (private setter).
    func fetchAndSyncSavedCars() async throws -> [ListingCar] {
        let cars = try await api.fetchSavedCars()
        savedCarIds = Set(cars.map(\.id))
        return cars
    }

    func toggleSaved(carId: Int) async throws -> Bool {
        let saved = try await api.toggleSave(carId: carId)
        if saved {
            savedCarIds.insert(carId)
        } else {
            savedCarIds.remove(carId)
        }
        return saved
    }

    func isSaved(_ carId: Int) -> Bool {
        savedCarIds.contains(carId)
    }

    var isLoggedIn: Bool { user != nil }

    var hasPremiumAccess: Bool {
        guard let user else { return false }
        return user.hasPaidAccess == true || user.isPremium == true
    }
}
