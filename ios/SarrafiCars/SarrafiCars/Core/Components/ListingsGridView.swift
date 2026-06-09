import SwiftUI

struct ListingsGridView: View {
    @EnvironmentObject private var appState: AppState

    let cars: [ListingCar]
    @Binding var visibleCount: Int
    var onSaveChanged: (() -> Void)?

    @State private var saveError: String?

    private let columns = [
        GridItem(.flexible(), spacing: 12),
        GridItem(.flexible(), spacing: 12),
    ]

    var body: some View {
        LazyVGrid(columns: columns, spacing: 12) {
            ForEach(Array(cars.prefix(visibleCount).enumerated()), id: \.element.id) { index, car in
                NavigationLink(value: car.id) {
                    ListingCardView(
                        car: car,
                        isSaved: appState.isSaved(car.id),
                        showSaveControl: appState.isLoggedIn,
                        onSaveTap: { Task { await toggleSave(car.id) } }
                    )
                }
                .buttonStyle(.plain)
                .onAppear {
                    if index >= visibleCount - 8 {
                        visibleCount = min(visibleCount + AppConfig.pageSize, cars.count)
                    }
                }
            }
        }
        .alert("Could not update saved cars", isPresented: Binding(
            get: { saveError != nil },
            set: { if !$0 { saveError = nil } }
        )) {
            Button("OK", role: .cancel) { saveError = nil }
        } message: {
            Text(saveError ?? "")
        }
    }

    private func toggleSave(_ carId: Int) async {
        do {
            _ = try await appState.toggleSaved(carId: carId)
            onSaveChanged?()
        } catch {
            saveError = error.localizedDescription
        }
    }
}
