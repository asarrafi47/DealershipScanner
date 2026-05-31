import SwiftUI

/// Case-insensitive dedupe; prefers Title Case over ALL CAPS (e.g. Cadillac not CADILLAC + Cadillac).
enum FilterOptionDedupe {
    static func strings(_ options: [String]) -> [String] {
        var byKey: [String: String] = [:]
        for raw in options {
            let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, trimmed != "—" else { continue }
            let key = trimmed.lowercased()
            if let existing = byKey[key] {
                if existing == existing.uppercased(), trimmed != trimmed.uppercased() {
                    byKey[key] = trimmed
                }
            } else {
                byKey[key] = trimmed
            }
        }
        return byKey.values.sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }
}

/// Compact refine filters — same options as the website, less vertical scroll.

struct ListingsInlineFilters: View {

    @EnvironmentObject private var appState: AppState

    @ObservedObject var session: ListingsSession

    var showHeading: Bool = true



    private var resultFacets: (

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



    private var catalog: ListingsFacetCatalog { session.facetCatalog }



    private var makeOptions: [String] {

        catalog.makes.isEmpty ? resultFacets.makes : catalog.makes

    }



    private var modelOptions: [String] {

        let fromCatalog = catalog.models(for: session.filters.makes)

        return fromCatalog.isEmpty ? resultFacets.models : fromCatalog

    }



    private var trimOptions: [String] {

        let fromCatalog = catalog.trims(for: session.filters.makes, models: session.filters.models)

        return fromCatalog.isEmpty ? resultFacets.trims : fromCatalog

    }



    private var bodyStyleOptions: [String] {

        catalog.bodyStyles.isEmpty ? resultFacets.bodyStyles : catalog.bodyStyles

    }



    private var cylinderOptions: [Int] {

        catalog.cylinders.isEmpty ? resultFacets.cylinders : catalog.cylinders

    }



    private var transmissionOptions: [String] {

        catalog.transmissions.isEmpty ? resultFacets.transmissions : catalog.transmissions

    }



    private var drivetrainOptions: [String] {

        catalog.drivetrains.isEmpty ? resultFacets.drivetrains : catalog.drivetrains

    }



    private var fuelOptions: [String] {

        catalog.fuelTypes.isEmpty ? resultFacets.fuelTypes : catalog.fuelTypes

    }



    private var exteriorOptions: [String] {

        catalog.exteriorColors.isEmpty ? resultFacets.exteriorColors : catalog.exteriorColors

    }



    private var interiorOptions: [String] {

        catalog.interiorColors.isEmpty ? resultFacets.interiorColors : catalog.interiorColors

    }



    private var packageOptions: [String] {

        catalog.packageNames.isEmpty ? resultFacets.packageNames : catalog.packageNames

    }



    private let filterColumns = [

        GridItem(.flexible(), spacing: 8),

        GridItem(.flexible(), spacing: 8),

        GridItem(.flexible(), spacing: 8),

    ]



    var body: some View {

        VStack(alignment: .leading, spacing: 10) {

            if showHeading {

                HStack {

                    Text("Refine results")

                        .font(.subheadline.weight(.semibold))

                        .foregroundStyle(AppTheme.textPrimary)

                    Spacer()

                    if session.filters.activeCount > 0 {

                        Button("Clear") { session.clearFilters() }

                            .font(.caption.weight(.semibold))

                            .foregroundStyle(AppTheme.navy)

                    }

                }

            }



            LazyVGrid(columns: filterColumns, spacing: 8) {

                MultiSelectFilterMenu(title: "Make", options: makeOptions, selected: $session.filters.makes, onUpdate: applyFilters)

                MultiSelectFilterMenu(title: "Model", options: modelOptions, selected: $session.filters.models, onUpdate: applyFilters)

                MultiSelectFilterMenu(title: "Trim", options: trimOptions, selected: $session.filters.trims, onUpdate: applyFilters)

            }



            LazyVGrid(columns: filterColumns, spacing: 8) {

                MultiSelectFilterMenu(title: "Body", options: bodyStyleOptions, selected: $session.filters.bodyStyles, onUpdate: applyFilters)

                IntMultiSelectFilterMenu(title: "Cyl.", options: cylinderOptions, selected: $session.filters.cylinders, onUpdate: applyFilters)

                MultiSelectFilterMenu(title: "Trans.", options: transmissionOptions, selected: $session.filters.transmissions, onUpdate: applyFilters)

            }



            HStack(spacing: 8) {
                CompactValuePicker(
                    title: "Max price",
                    selection: $session.filters.maxPrice,
                    options: ListingsFilters.priceOptions,
                    label: priceLabel,
                    onUpdate: applyFilters
                )
                CompactValuePicker(
                    title: "Max mi.",
                    selection: $session.filters.maxMileage,
                    options: ListingsFilters.mileageOptions,
                    label: mileageLabel,
                    onUpdate: applyFilters
                )
            }

            LazyVGrid(columns: filterColumns, spacing: 8) {
                MultiSelectFilterMenu(title: "Drive", options: drivetrainOptions, selected: $session.filters.drivetrains, onUpdate: applyFilters)
                MultiSelectFilterMenu(title: "Fuel", options: fuelOptions, selected: $session.filters.fuelTypes, onUpdate: applyFilters)
                MultiSelectFilterMenu(title: "Ext.", options: exteriorOptions, selected: $session.filters.exteriorColors, onUpdate: applyFilters)
            }

            LazyVGrid(columns: filterColumns, spacing: 8) {
                MultiSelectFilterMenu(title: "Int.", options: interiorOptions, selected: $session.filters.interiorColors, onUpdate: applyFilters)
                MultiSelectFilterMenu(title: "Pkg", options: packageOptions, selected: $session.filters.packageNames, onUpdate: applyFilters)
            }

            if appState.isLoggedIn, appState.hasPremiumAccess {
                premiumDealershipSection
            }
        }

    }



    private func priceLabel(_ value: Int?) -> String {

        guard let value else { return "Any" }

        return "≤\(AppConfig.formatPrice(Double(value)))"

    }



    private func mileageLabel(_ value: Int?) -> String {

        guard let value else { return "Any" }

        return "≤\(AppConfig.formatMileage(value))"

    }



    private func applyFilters() {

        session.applyLocalFilters()

    }



    @ViewBuilder

    private var premiumDealershipSection: some View {

        let dealerships: [(id: Int, name: String)] = {

            var seen = Set<Int>()

            return session.facetPool.compactMap { car -> (Int, String)? in

                guard let id = car.dealershipRegistryId, !seen.contains(id) else { return nil }

                seen.insert(id)

                let name = car.dealerName?.trimmingCharacters(in: .whitespacesAndNewlines)

                return (id, name?.isEmpty == false ? name! : "Dealer")

            }

        }()



        if !dealerships.isEmpty {

            MultiSelectFilterMenu(

                title: "Dealer",

                options: dealerships.map(\.name),

                selected: Binding(

                    get: {

                        Set(dealerships.filter { session.filters.dealershipRegistryIds.contains($0.id) }.map(\.name))

                    },

                    set: { names in

                        session.filters.dealershipRegistryIds = Set(

                            dealerships.filter { names.contains($0.name) }.map(\.id)

                        )

                    }

                ),

                onUpdate: applyFilters

            )

        }

    }

}



// MARK: - Compact filter controls



struct FilterMenuLabel: View {

    let title: String

    let summary: String



    var body: some View {

        VStack(alignment: .leading, spacing: 2) {

            Text(title)

                .font(.system(size: 10, weight: .semibold))

                .foregroundStyle(AppTheme.textMuted)

                .lineLimit(1)

            Text(summary)

                .font(.caption.weight(.medium))

                .foregroundStyle(AppTheme.textPrimary)

                .lineLimit(1)

                .minimumScaleFactor(0.8)

        }

        .frame(maxWidth: .infinity, alignment: .leading)

        .padding(.horizontal, 8)

        .padding(.vertical, 7)

        .background(Color(.systemGray6))

        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))

        .overlay(

            RoundedRectangle(cornerRadius: 8, style: .continuous)

                .stroke(AppTheme.navy.opacity(0.12), lineWidth: 1)

        )

    }

}



struct MultiSelectFilterMenu: View {
    let title: String
    let options: [String]
    @Binding var selected: Set<String>
    var onUpdate: () -> Void = {}

    @State private var isOpen = false

    private var usableOptions: [String] {
        FilterOptionDedupe.strings(options)
    }

    private var summary: String {
        if selected.isEmpty { return "Any" }
        if selected.count == 1 { return selected.first ?? "Any" }
        return "\(selected.count)"
    }

    var body: some View {
        Button {
            isOpen = true
        } label: {
            FilterMenuLabel(title: title, summary: summary)
        }
        .buttonStyle(.plain)
        .disabled(usableOptions.isEmpty)
        .sheet(isPresented: $isOpen) {
            FilterChecklistSheet(
                title: title,
                options: usableOptions,
                selected: $selected,
                onUpdate: onUpdate,
                onDismiss: { isOpen = false }
            )
        }
    }
}

struct IntMultiSelectFilterMenu: View {
    let title: String
    let options: [Int]
    @Binding var selected: Set<Int>
    var onUpdate: () -> Void = {}

    @State private var isOpen = false

    private var sortedOptions: [Int] {
        options.sorted()
    }

    private var summary: String {
        if selected.isEmpty { return "Any" }
        if selected.count == 1, let v = selected.first {
            return v == 0 ? "EV" : "\(v)"
        }
        return "\(selected.count)"
    }

    var body: some View {
        Button {
            isOpen = true
        } label: {
            FilterMenuLabel(title: title, summary: summary)
        }
        .buttonStyle(.plain)
        .disabled(sortedOptions.isEmpty)
        .sheet(isPresented: $isOpen) {
            IntFilterChecklistSheet(
                title: title,
                options: sortedOptions,
                selected: $selected,
                onUpdate: onUpdate,
                onDismiss: { isOpen = false }
            )
        }
    }
}

/// Native iOS checklist sheet (inline nav bar, no empty header gap).
private struct FilterChecklistSheet: View {
    let title: String
    let options: [String]
    @Binding var selected: Set<String>
    var onUpdate: () -> Void
    var onDismiss: () -> Void

    var body: some View {
        NavigationStack {
            List {
                if !selected.isEmpty {
                    Button("Clear \(title)", role: .destructive) {
                        selected.removeAll()
                        onUpdate()
                    }
                }
                ForEach(options, id: \.self) { option in
                    Button {
                        if selected.contains(option) {
                            selected.remove(option)
                        } else {
                            selected.insert(option)
                        }
                        onUpdate()
                    } label: {
                        HStack {
                            Text(option)
                                .foregroundStyle(AppTheme.textPrimary)
                            Spacer()
                            if selected.contains(option) {
                                Image(systemName: "checkmark")
                                    .font(.body.weight(.semibold))
                                    .foregroundStyle(AppTheme.navy)
                            }
                        }
                    }
                }
            }
            .listStyle(.insetGrouped)
            .navigationTitle(title)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done", action: onDismiss)
                }
            }
        }
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
    }
}

private struct IntFilterChecklistSheet: View {
    let title: String
    let options: [Int]
    @Binding var selected: Set<Int>
    var onUpdate: () -> Void
    var onDismiss: () -> Void

    var body: some View {
        NavigationStack {
            List {
                if !selected.isEmpty {
                    Button("Clear \(title)", role: .destructive) {
                        selected.removeAll()
                        onUpdate()
                    }
                }
                ForEach(options, id: \.self) { option in
                    let label = option == 0 ? "Electric" : "\(option)-cyl"
                    Button {
                        if selected.contains(option) {
                            selected.remove(option)
                        } else {
                            selected.insert(option)
                        }
                        onUpdate()
                    } label: {
                        HStack {
                            Text(label)
                                .foregroundStyle(AppTheme.textPrimary)
                            Spacer()
                            if selected.contains(option) {
                                Image(systemName: "checkmark")
                                    .font(.body.weight(.semibold))
                                    .foregroundStyle(AppTheme.navy)
                            }
                        }
                    }
                }
            }
            .listStyle(.insetGrouped)
            .navigationTitle(title)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .confirmationAction) {
                    Button("Done", action: onDismiss)
                }
            }
        }
        .presentationDetents([.medium, .large])
        .presentationDragIndicator(.visible)
    }
}



struct CompactValuePicker: View {
    let title: String
    @Binding var selection: Int?
    let options: [Int?]
    let label: (Int?) -> String
    var onUpdate: () -> Void = {}

    var body: some View {
        Menu {
            ForEach(options, id: \.self) { value in
                Button(label(value)) {
                    selection = value
                    onUpdate()
                }
            }
        } label: {
            FilterMenuLabel(title: title, summary: label(selection))
        }
    }
}


