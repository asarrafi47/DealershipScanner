import SwiftUI

struct ListingsFilterSheet: View {
    @EnvironmentObject private var appState: AppState
    @ObservedObject var session: ListingsSession
    @Environment(\.dismiss) private var dismiss

    private var facets: (
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
        ListingsFilterEngine.facetValues(in: session.facetPool)
    }

    var body: some View {
        NavigationStack {
            Form {
                Section("Price & mileage") {
                    Picker("Max price", selection: $session.filters.maxPrice) {
                        Text("Any").tag(Int?.none)
                        ForEach(ListingsFilters.priceOptions.compactMap { $0 }, id: \.self) { p in
                            Text("Under \(AppConfig.formatPrice(Double(p)))").tag(Optional(p))
                        }
                    }
                    Picker("Max mileage", selection: $session.filters.maxMileage) {
                        Text("Any").tag(Int?.none)
                        ForEach(ListingsFilters.mileageOptions.compactMap { $0 }, id: \.self) { m in
                            Text("Under \(AppConfig.formatMileage(m))").tag(Optional(m))
                        }
                    }
                }

                if !facets.makes.isEmpty {
                    Section("Make") { facetList(options: facets.makes, selected: $session.filters.makes) }
                }
                if !facets.models.isEmpty {
                    Section("Model") { facetList(options: facets.models, selected: $session.filters.models) }
                }
                if !facets.trims.isEmpty {
                    Section("Trim") { facetList(options: facets.trims, selected: $session.filters.trims) }
                }

                if !facets.cylinders.isEmpty {
                    Section("Cylinders") { intFacetList(options: facets.cylinders, selected: $session.filters.cylinders) }
                }

                if !facets.transmissions.isEmpty {
                    Section("Transmission") { facetList(options: facets.transmissions, selected: $session.filters.transmissions) }
                }
                if !facets.drivetrains.isEmpty {
                    Section("Drivetrain") { facetList(options: facets.drivetrains, selected: $session.filters.drivetrains) }
                }
                if !facets.bodyStyles.isEmpty {
                    Section("Body style") { facetList(options: facets.bodyStyles, selected: $session.filters.bodyStyles) }
                }
                if !facets.fuelTypes.isEmpty {
                    Section("Fuel") { facetList(options: facets.fuelTypes, selected: $session.filters.fuelTypes) }
                }

                if !facets.exteriorColors.isEmpty {
                    Section("Ext. Color") { facetList(options: facets.exteriorColors, selected: $session.filters.exteriorColors) }
                }
                if !facets.interiorColors.isEmpty {
                    Section("Int. Color") { facetList(options: facets.interiorColors, selected: $session.filters.interiorColors) }
                }
                if !facets.packageNames.isEmpty {
                    Section("Package") { facetList(options: facets.packageNames, selected: $session.filters.packageNames) }
                }

                if appState.isLoggedIn, appState.user?.hasPaidAccess == true {
                    premiumDealershipSection
                }
            }
            .navigationTitle("Refine results")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Clear") { session.clearFilters() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Apply") {
                        session.applyLocalFilters()
                        dismiss()
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func facetList(options: [String], selected: Binding<Set<String>>) -> some View {
        ForEach(options, id: \.self) { option in
            Button {
                if selected.wrappedValue.contains(option) {
                    selected.wrappedValue.remove(option)
                } else {
                    selected.wrappedValue.insert(option)
                }
            } label: {
                HStack {
                    Text(option)
                    Spacer()
                    if selected.wrappedValue.contains(option) {
                        Image(systemName: "checkmark").foregroundStyle(Color.accentColor)
                    }
                }
            }
            .foregroundStyle(.primary)
        }
    }

    @ViewBuilder
    private func intFacetList(options: [Int], selected: Binding<Set<Int>>) -> some View {
        ForEach(options.sorted(), id: \.self) { option in
            Button {
                if selected.wrappedValue.contains(option) {
                    selected.wrappedValue.remove(option)
                } else {
                    selected.wrappedValue.insert(option)
                }
            } label: {
                HStack {
                    Text(option == 0 ? "Electric" : "\(option)-cyl")
                    Spacer()
                    if selected.wrappedValue.contains(option) {
                        Image(systemName: "checkmark").foregroundStyle(Color.accentColor)
                    }
                }
            }
            .foregroundStyle(.primary)
        }
    }

    private var premiumDealershipSection: some View {
        let dealerships: [Int: String] = Dictionary(
            uniqueKeysWithValues: session.facetPool.compactMap { car in
                guard let id = car.dealershipRegistryId else { return nil }
                let name = car.dealerName?.trimmingCharacters(in: .whitespacesAndNewlines)
                return (id, name?.isEmpty == false ? name! : "Dealer")
            }
        )

        let sortedIds = dealerships.keys.sorted()

        return Section("Dealerships (Premium)") {
            if sortedIds.isEmpty {
                Text("Run a search to load nearby dealers.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(sortedIds, id: \.self) { id in
                    Button {
                        if session.filters.dealershipRegistryIds.contains(id) {
                            session.filters.dealershipRegistryIds.remove(id)
                        } else {
                            session.filters.dealershipRegistryIds.insert(id)
                        }
                    } label: {
                        HStack {
                            Text(dealerships[id] ?? "Dealer")
                            Spacer()
                            if session.filters.dealershipRegistryIds.contains(id) {
                                Image(systemName: "checkmark").foregroundStyle(Color.accentColor)
                            }
                        }
                    }
                }
            }
        }
    }
}
