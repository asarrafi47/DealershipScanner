import SwiftUI

struct SavedCarsView: View {
    @EnvironmentObject private var appState: AppState
    @State private var cars: [ListingCar] = []
    @State private var selectedIds: Set<Int> = []
    @State private var visibleCount = AppConfig.pageSize
    @State private var isLoading = false
    @State private var errorMessage: String?
    @State private var showCompare = false
    @State private var compareLimitMessage: String?

    private let maxCompare = 4

    var body: some View {
        NavigationStack {
            AppScreenShell(
                title: "Saved cars",
                subtitle: "Select 2–4 vehicles to compare side by side."
            ) {
                Group {
                    if !appState.isLoggedIn {
                        EmptyStateView(
                            title: "Sign in required",
                            systemImage: "heart",
                            message: "Save cars from search results, then compare them here."
                        )
                        .padding(.top, 24)
                    } else if isLoading && cars.isEmpty {
                        ProgressView("Loading saved cars…")
                            .padding(.top, 32)
                    } else if let errorMessage, cars.isEmpty {
                        EmptyStateView(title: "Error", systemImage: "exclamationmark.triangle", message: errorMessage)
                            .padding(.top, 24)
                    } else if cars.isEmpty {
                        EmptyStateView(
                            title: "No saved cars",
                            systemImage: "heart",
                            message: "Tap Save on a vehicle to add it here."
                        )
                        .padding(.top, 24)
                    } else {
                        VStack(spacing: 0) {
                            compareBar
                            ScrollView(showsIndicators: AppTheme.showsScrollIndicators) {
                                LazyVGrid(
                                    columns: [GridItem(.flexible()), GridItem(.flexible())],
                                    spacing: 12
                                ) {
                                    ForEach(Array(cars.prefix(visibleCount))) { car in
                                        ZStack(alignment: .topTrailing) {
                                            NavigationLink(value: car.id) {
                                                ListingCardView(
                                                    car: car,
                                                    isSaved: true,
                                                    showSaveControl: true,
                                                    onSaveTap: { Task { await unsave(car.id) } }
                                                )
                                            }
                                            .buttonStyle(.plain)
                                            .opacity(selectedIds.contains(car.id) ? 1 : 0.92)

                                            compareToggle(car.id)
                                        }
                                        .onAppear {
                                            if car.id == cars.prefix(visibleCount).last?.id {
                                                visibleCount = min(visibleCount + AppConfig.pageSize, cars.count)
                                            }
                                        }
                                    }
                                }
                                .padding(16)
                            }
                            .frame(maxHeight: .infinity)
                        }
                    }
                }
            }
            .navigationBarHidden(true)
            .navigationDestination(for: Int.self) { carId in
                CarWebDetailView(carId: carId)
            }
            .navigationDestination(isPresented: $showCompare) {
                CompareWebView(carIds: Array(selectedIds).sorted())
            }
            .refreshable { await load() }
            .task(id: appState.isLoggedIn) { await load() }
            .alert("Compare limit", isPresented: Binding(
                get: { compareLimitMessage != nil },
                set: { if !$0 { compareLimitMessage = nil } }
            )) {
                Button("OK", role: .cancel) { compareLimitMessage = nil }
            } message: {
                Text(compareLimitMessage ?? "")
            }
        }
    }

    private var compareBar: some View {
        HStack {
            Text("Select up to \(maxCompare) to compare")
                .font(.caption)
                .foregroundStyle(AppTheme.textMuted)
            Spacer()
            Button("Compare (\(selectedIds.count))") {
                showCompare = true
            }
            .font(.subheadline.weight(.semibold))
            .foregroundStyle(AppTheme.navy)
            .disabled(selectedIds.count < 2)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 10)
        .background(AppTheme.cardBackground)
    }

    @ViewBuilder
    private func compareToggle(_ id: Int) -> some View {
        Button {
            if selectedIds.contains(id) {
                selectedIds.remove(id)
            } else if selectedIds.count < maxCompare {
                selectedIds.insert(id)
            } else {
                compareLimitMessage = "You can compare up to \(maxCompare) vehicles. Deselect one to add another."
            }
        } label: {
            Image(systemName: selectedIds.contains(id) ? "checkmark.circle.fill" : "circle")
                .font(.title2)
                .symbolRenderingMode(.palette)
                .foregroundStyle(.white, selectedIds.contains(id) ? AppTheme.navy : Color.gray.opacity(0.8))
                .padding(8)
        }
    }

    private func unsave(_ carId: Int) async {
        do {
            _ = try await appState.toggleSaved(carId: carId)
            await load()
        } catch {
            errorMessage = error.localizedDescription
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
            cars = try await appState.fetchAndSyncSavedCars()
            visibleCount = AppConfig.pageSize
            selectedIds = selectedIds.filter { id in cars.contains(where: { $0.id == id }) }
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}

struct CompareWebView: View {
    let carIds: [Int]

    private var compareURL: URL {
        var components = URLComponents(url: AppConfig.baseURL.appending(path: "compare"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "ids", value: carIds.map(String.init).joined(separator: ","))]
        return components.url ?? AppConfig.baseURL.appending(path: "compare")
    }

    var body: some View {
        EmbeddedWebView(url: compareURL)
            .navigationTitle("Compare")
            .navigationBarTitleDisplayMode(.inline)
    }
}
