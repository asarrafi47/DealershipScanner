import SwiftUI

struct CarDetailView: View {
    let carId: Int
    @EnvironmentObject private var appState: AppState

    @State private var detail: CarDetailResponse?
    @State private var isLoading = true
    @State private var errorMessage: String?
    @State private var isSaved = false
    @State private var isSaving = false

    private let api = APIClient.shared
    private let columns = [GridItem(.flexible()), GridItem(.flexible())]

    var body: some View {
        Group {
            if isLoading {
                ProgressView("Loading…")
            } else if let errorMessage {
                EmptyStateView(
                    title: "Could not load",
                    systemImage: "exclamationmark.triangle",
                    message: errorMessage ?? "Try again."
                )
            } else if let detail, let car = detail.car {
                ScrollView {
                    VStack(alignment: .leading, spacing: 16) {
                        photoCarousel(urls: galleryURLs(detail))

                        VStack(alignment: .leading, spacing: 8) {
                            Text(car.headline)
                                .font(.title2.bold())
                            Text(AppConfig.formatPrice(car.price))
                                .font(.title.weight(.semibold))
                                .foregroundStyle(Color.accentColor)
                            Text(AppConfig.formatMileage(car.mileage))
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                        }
                        .padding(.horizontal)

                        if let intel = detail.marketIntel, intel.avgPrice != nil {
                            marketCard(intel)
                        }

                        if appState.isLoggedIn {
                            saveButton
                        }

                        specSection(title: "Overview", rows: overviewRows(car))
                        if let dealer = detail.dealerInfo {
                            specSection(title: "Dealer", rows: dealerRows(dealer))
                        }
                    }
                    .padding(.bottom, 24)
                }
            }
        }
        .navigationBarTitleDisplayMode(.inline)
        .task { await load() }
        .refreshable { await load() }
    }

    private var saveButton: some View {
        Button {
            Task { await toggleSave() }
        } label: {
            Label(isSaved ? "Saved" : "Save car", systemImage: isSaved ? "heart.fill" : "heart")
                .frame(maxWidth: .infinity)
        }
        .buttonStyle(.borderedProminent)
        .tint(isSaved ? .pink : .accentColor)
        .disabled(isSaving)
        .padding(.horizontal)
    }

    @ViewBuilder
    private func photoCarousel(urls: [URL]) -> some View {
        if urls.isEmpty {
            RemoteImage(url: nil)
                .frame(height: 240)
        } else {
            TabView {
                ForEach(Array(urls.enumerated()), id: \.offset) { _, url in
                    RemoteImage(url: url)
                        .frame(height: 240)
                }
            }
            .frame(height: 240)
            .tabViewStyle(.page(indexDisplayMode: urls.count > 1 ? .automatic : .never))
        }
    }

    private func marketCard(_ intel: MarketIntel) -> some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text("Market insight")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                if let avg = intel.avgPrice {
                    Text("Similar listings avg. \(AppConfig.formatPrice(avg))")
                        .font(.subheadline)
                }
                if let label = intel.vsMarketLabel, !label.isEmpty {
                    Text(label)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                if let n = intel.sampleCount {
                    Text("\(n) comparable listings")
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
            Spacer()
        }
        .padding()
        .background(Color(.secondarySystemGroupedBackground))
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .padding(.horizontal)
    }

    private func specSection(title: String, rows: [(String, String)]) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .font(.headline)
                .padding(.horizontal)
            LazyVGrid(columns: columns, alignment: .leading, spacing: 8) {
                ForEach(Array(rows.enumerated()), id: \.offset) { _, row in
                    VStack(alignment: .leading, spacing: 2) {
                        Text(row.0)
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(row.1)
                            .font(.subheadline)
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(10)
                    .background(Color(.secondarySystemGroupedBackground))
                    .clipShape(RoundedRectangle(cornerRadius: 8))
                }
            }
            .padding(.horizontal)
        }
    }

    private func galleryURLs(_ detail: CarDetailResponse) -> [URL] {
        let paths = detail.galleryImages ?? []
        return paths.compactMap { AppConfig.resolveURL($0) }
    }

    private func overviewRows(_ car: DetailCar) -> [(String, String)] {
        var rows: [(String, String)] = []
        func add(_ label: String, _ value: String?) {
            guard let value, !value.isEmpty, value != "—" else { return }
            rows.append((label, value))
        }
        add("VIN", car.vin)
        add("Exterior", car.exteriorColor)
        add("Interior", car.interiorColor)
        add("Drivetrain", car.drivetrain)
        add("Transmission", car.transmission)
        add("Fuel", car.fuelType)
        add("Body", car.bodyStyle)
        add("Engine", car.engineDescription)
        add("Dealer", car.dealerName)
        return rows
    }

    private func dealerRows(_ dealer: DealerInfo) -> [(String, String)] {
        var rows: [(String, String)] = []
        if let name = dealer.name, !name.isEmpty { rows.append(("Name", name)) }
        let loc = [dealer.city, dealer.state].compactMap { $0 }.filter { !$0.isEmpty }.joined(separator: ", ")
        if !loc.isEmpty { rows.append(("Location", loc)) }
        return rows
    }

    private func load() async {
        isLoading = true
        errorMessage = nil
        defer { isLoading = false }
        do {
            let resp = try await api.fetchCarDetail(id: carId)
            detail = resp
            isSaved = resp.carIsSaved ?? false
        } catch {
            errorMessage = error.localizedDescription
        }
    }

    private func toggleSave() async {
        isSaving = true
        defer { isSaving = false }
        do {
            isSaved = try await api.toggleSave(carId: carId)
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}
