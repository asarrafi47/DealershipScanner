import SwiftUI

/// Primary inventory tab — mirrors website listings flow (ZIP + radius → smart search → refine → grid).
struct InventoryTabView: View {
    @StateObject private var session = ListingsSession()
    @State private var showFilters = false

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 16) {
                    headerSection
                    geoSection
                    searchSection
                    if let hint = session.geoHint {
                        Text(hint)
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }
                    if session.isLoading {
                        HStack {
                            Spacer()
                            ProgressView("Loading inventory…")
                            Spacer()
                        }
                        .padding(.vertical, 32)
                    } else if let err = session.errorMessage {
                        EmptyStateView(title: "Could not load", systemImage: "wifi.exclamationmark", message: err)
                            .frame(minHeight: 200)
                    } else if !session.isZipValid {
                        EmptyStateView(
                            title: "Set your location",
                            systemImage: "location.circle",
                            message: "Enter a 5-digit ZIP and radius to see nearby dealer inventory."
                        )
                        .frame(minHeight: 200)
                    } else if session.displayedCars.isEmpty {
                        EmptyStateView(
                            title: "No matches",
                            systemImage: "car",
                            message: session.emptyMessage ?? session.geoHint ?? "Try adjusting filters or search terms."
                        )
                        .frame(minHeight: 200)
                    } else {
                        ListingsGridView(cars: session.displayedCars, visibleCount: $session.visibleCount)
                    }
                }
                .padding()
            }
            .navigationTitle("Find your next car")
            .navigationBarTitleDisplayMode(.large)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    if session.isZipValid && !session.displayedCars.isEmpty {
                        Text("\(session.displayedCars.count.formatted()) cars")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
                ToolbarItem(placement: .topBarLeading) {
                    Button {
                        showFilters = true
                    } label: {
                        HStack(spacing: 4) {
                            Image(systemName: "line.3.horizontal.decrease.circle")
                            if session.filters.activeCount > 0 {
                                Text("\(session.filters.activeCount)")
                                    .font(.caption2.bold())
                                    .padding(.horizontal, 6)
                                    .padding(.vertical, 2)
                                    .background(Color.accentColor)
                                    .foregroundStyle(.white)
                                    .clipShape(Capsule())
                            }
                        }
                    }
                    .disabled(!session.isZipValid || session.facetPool.isEmpty)
                    .accessibilityLabel("Refine results")
                }
            }
            .sheet(isPresented: $showFilters) {
                ListingsFilterSheet(session: session)
            }
            .navigationDestination(for: Int.self) { carId in
                CarDetailView(carId: carId)
            }
            .refreshable { await session.refresh() }
        }
    }

    private var headerSection: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text("Live dealer inventory")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .textCase(.uppercase)
            Text("Search in plain English, set your radius, then refine by make, price, and more.")
                .font(.subheadline)
                .foregroundStyle(.secondary)
        }
    }

    private var geoSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Near you")
                .font(.subheadline.weight(.semibold))
            HStack(spacing: 12) {
                VStack(alignment: .leading, spacing: 4) {
                    Text("ZIP")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    TextField("00000", text: $session.zipCode)
                        .keyboardType(.numberPad)
                        .textFieldStyle(.roundedBorder)
                        .onChange(of: session.zipCode) { new in
                            if new.count > 5 {
                                session.zipCode = String(new.prefix(5))
                            }
                        }
                }
                VStack(alignment: .leading, spacing: 4) {
                    Text("Radius")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Picker("Radius", selection: $session.radiusMiles) {
                        ForEach(ListingsRadius.allCases) { r in
                            Text(r.label).tag(r.rawValue)
                        }
                    }
                    .pickerStyle(.menu)
                }
            }
        }
        .padding()
        .background(Color(.secondarySystemGroupedBackground))
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }

    private var searchSection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Try: Accord with heated seats under $30k · Premium Package · paste a VIN")
                .font(.caption)
                .foregroundStyle(.secondary)

            TextField("Accord with sunroof under $30k…", text: $session.searchQuery)
                .textFieldStyle(.roundedBorder)
                .submitLabel(.search)
                .onSubmit { Task { await session.refresh() } }

            Button {
                Task { await session.refresh() }
            } label: {
                Text("Search inventory")
                    .frame(maxWidth: .infinity)
            }
            .buttonStyle(.borderedProminent)
            .disabled(!session.isZipValid || session.isLoading)
        }
        .padding()
        .background(Color(.secondarySystemGroupedBackground))
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }
}
