import SwiftUI

struct SavedTabView: View {
    @EnvironmentObject private var appState: AppState

    @State private var cars: [ListingCar] = []
    @State private var visibleCount = AppConfig.pageSize
    @State private var isLoading = false
    @State private var errorMessage: String?

    var body: some View {
        NavigationStack {
            Group {
                if !appState.isLoggedIn {
                    EmptyStateView(
                        title: "Sign in required",
                        systemImage: "heart",
                        message: "Save cars from a listing to see them here."
                    )
                } else if isLoading && cars.isEmpty {
                    ProgressView("Loading saved cars…")
                } else if let errorMessage, cars.isEmpty {
                    EmptyStateView(
                        title: "Could not load",
                        systemImage: "exclamationmark.triangle",
                        message: errorMessage
                    )
                } else if cars.isEmpty {
                    EmptyStateView(
                        title: "No saved cars",
                        systemImage: "heart",
                        message: "Tap Save on a vehicle to add it here."
                    )
                } else {
                    ScrollView {
                        ListingsGridView(cars: cars, visibleCount: $visibleCount)
                            .padding()
                    }
                }
            }
            .navigationTitle("Saved")
            .navigationDestination(for: Int.self) { carId in
                CarDetailView(carId: carId)
            }
            .refreshable { await load() }
            .task(id: appState.isLoggedIn) { await load() }
        }
    }

    private func load() async {
        guard appState.isLoggedIn else {
            cars = []
            return
        }
        isLoading = true
        errorMessage = nil
        defer { isLoading = false }
        do {
            cars = try await APIClient.shared.fetchSavedCars()
            visibleCount = AppConfig.pageSize
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}
