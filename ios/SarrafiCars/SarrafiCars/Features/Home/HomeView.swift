import SwiftUI



struct FloatingFiltersCard: View {

    @EnvironmentObject private var appState: AppState

    @ObservedObject var session: ListingsSession

    var onSearch: () -> Void



    var body: some View {

        FloatingCard {

            VStack(alignment: .leading, spacing: 10) {

                TextField("Accord under $30k · paste a VIN…", text: $session.searchQuery)

                    .textFieldStyle(.roundedBorder)



                HStack(spacing: 10) {

                    TextField("ZIP", text: $session.zipCode)

                        .keyboardType(.numberPad)

                        .textFieldStyle(.roundedBorder)

                        .onChange(of: session.zipCode) { new in

                            if new.count > 5 { session.zipCode = String(new.prefix(5)) }

                        }

                    Picker("Radius", selection: $session.radiusMiles) {

                        ForEach(ListingsRadius.allCases) { r in

                            Text(r.label).tag(r.rawValue)

                        }

                    }

                    .pickerStyle(.menu)

                    .tint(AppTheme.navy)

                }



                Button("Search inventory") {

                    onSearch()

                }

                .buttonStyle(PrimaryNavyButton())

                .disabled(!session.isZipValid)



                Divider()



                ListingsInlineFilters(session: session)

                    .environmentObject(appState)

            }

        }

    }

}



struct HomeView: View {

    @StateObject private var session = ListingsSession()

    @State private var hasSearched = false



    var body: some View {

        NavigationStack {

            AppScreenShell {

                ScrollView(showsIndicators: AppTheme.showsScrollIndicators) {

                    VStack(spacing: 12) {

                        FloatingFiltersCard(session: session) {

                            hasSearched = true

                            Task { await session.refresh(forceReloadInventory: true) }

                        }

                        .padding(.horizontal, 16)

                        .padding(.top, 10)



                        if hasSearched {

                            ListingsResultsView()

                                .environmentObject(session)

                        } else {

                            EmptyStateView(

                                title: "Search inventory",

                                systemImage: "magnifyingglass",

                                message: "Enter ZIP + radius, then tap Search inventory."

                            )

                            .padding(.horizontal, 16)

                        }

                    }

                    .padding(.bottom, 24)

                }

                .refreshable {
                    guard hasSearched else { return }
                    await session.refresh(forceReloadInventory: true)
                }

            }

            .navigationBarHidden(true)

            .navigationDestination(for: Int.self) { carId in

                CarWebDetailView(carId: carId)

            }

        }

        .environmentObject(session)

        .task(id: session.isZipValid) {
            guard session.isZipValid else { return }
            await session.facetCatalog.loadIfNeeded()
        }

    }

}



struct ListingsResultsView: View {

    @EnvironmentObject private var session: ListingsSession



    var body: some View {

        Group {

            if session.isLoading {

                ProgressView("Loading…")

                    .frame(maxWidth: .infinity)

                    .padding(.vertical, 32)

            } else if let err = session.errorMessage {

                EmptyStateView(title: "Error", systemImage: "exclamationmark.triangle", message: err)

            } else if session.displayedCars.isEmpty {

                EmptyStateView(

                    title: "No matches",

                    systemImage: "car",

                    message: session.emptyMessage ?? session.geoHint ?? "Adjust filters and try again."

                )

            } else {

                VStack(alignment: .leading, spacing: 8) {

                    Text("\(session.displayedCars.count.formatted()) vehicles")

                        .font(.subheadline)

                        .foregroundStyle(AppTheme.textMuted)

                        .padding(.horizontal, 16)



                    ListingsGridView(cars: session.displayedCars, visibleCount: $session.visibleCount)

                        .padding(.horizontal, 16)

                }

            }

        }

    }

}


