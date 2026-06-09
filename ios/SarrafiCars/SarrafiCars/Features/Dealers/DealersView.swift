import SwiftUI

import CoreLocation

import MapKit



struct DealerRow: Identifiable, Decodable {

    let key: String?

    let registryId: Int?

    let name: String?

    let city: String?

    let state: String?

    let distanceMiles: Double?

    let inDatabase: Bool?

    let listingCount: Int?

    let websiteUrl: String?

    let latitude: Double?

    let longitude: Double?



    var id: String { key ?? "\(name ?? "")-\(registryId ?? 0)" }



    var locationLine: String {

        [city, state].compactMap { $0 }.filter { !$0.isEmpty }.joined(separator: ", ")

    }

}



struct DealerLocatorCenter: Decodable {

    let lat: Double

    let lon: Double



    var coordinate: CLLocationCoordinate2D {

        CLLocationCoordinate2D(latitude: lat, longitude: lon)

    }

}



struct DealerLocatorResponse: Decodable {

    let ok: Bool

    let dealers: [DealerRow]?

    let center: DealerLocatorCenter?

    let error: String?

}



final class LocationManager: NSObject, ObservableObject, CLLocationManagerDelegate {

    @Published var authorization: CLAuthorizationStatus = .notDetermined

    @Published var lastLocation: CLLocation?



    private let manager = CLLocationManager()



    override init() {

        super.init()

        manager.delegate = self

        manager.desiredAccuracy = kCLLocationAccuracyKilometer

    }



    func request() {

        manager.requestWhenInUseAuthorization()

    }



    func refreshLocation() {

        manager.requestLocation()

    }



    func locationManagerDidChangeAuthorization(_ manager: CLLocationManager) {

        authorization = manager.authorizationStatus

        if authorization == .authorizedWhenInUse || authorization == .authorizedAlways {

            manager.requestLocation()

        }

    }



    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {

        lastLocation = locations.last

    }



    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {}

}



struct DealersView: View {

    @StateObject private var location = LocationManager()

    @State private var zipCode = ""

    @State private var radiusMiles = 25

    @State private var dealers: [DealerRow] = []

    @State private var mapCenter: CLLocationCoordinate2D?

    @State private var selectedDealerId: String?

    @State private var status = "Allow location or enter ZIP, then search."

    @State private var isLoading = false

    @State private var didAutoSearch = false



    var body: some View {

        AppScreenShell(

            title: "Find dealers",

            subtitle: "Tap a dealer to highlight it. Tap the map or button for Apple Maps directions."

        ) {

            ScrollView(showsIndicators: AppTheme.showsScrollIndicators) {

                VStack(alignment: .leading, spacing: 12) {

                    compactForm



                    if !dealers.isEmpty || mapCenter != nil {

                        legend

                        DealersMapView(

                            dealers: dealers,

                            center: mapCenter,

                            selectedDealerId: $selectedDealerId,

                            onOpenInMaps: { DealerMapsLauncher.openDirections(to: $0) }

                        )

                        .frame(height: 220)

                        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))

                        .padding(.horizontal, 16)

                    }



                    if isLoading {

                        ProgressView("Finding dealers…")

                            .frame(maxWidth: .infinity)

                            .padding(.vertical, 20)

                    } else if dealers.isEmpty {

                        Text(status)

                            .font(.subheadline)

                            .foregroundStyle(AppTheme.textMuted)

                            .multilineTextAlignment(.center)

                            .frame(maxWidth: .infinity)

                            .padding(.vertical, 20)

                            .padding(.horizontal, 24)

                    } else {

                        VStack(alignment: .leading, spacing: 0) {

                            Text("Dealers (\(dealers.count))")

                                .font(.subheadline.weight(.semibold))

                                .foregroundStyle(AppTheme.textPrimary)

                                .padding(.horizontal, 16)

                                .padding(.bottom, 6)



                            ForEach(dealers) { dealer in

                                Button {

                                    selectedDealerId = dealer.id

                                } label: {

                                    dealerRow(dealer)

                                }

                                .buttonStyle(.plain)

                                Divider().padding(.leading, 16)

                            }

                        }

                    }

                }

                .padding(.bottom, 16)

            }

        }

        .onAppear { location.request() }

        .onChange(of: location.authorization) { auth in

            if auth == .denied || auth == .restricted {

                status = "Location denied — enter ZIP below."

            }

        }

        .onChange(of: location.lastLocation) { loc in

            guard let loc, !didAutoSearch else { return }

            didAutoSearch = true

            Task { await search(lat: loc.coordinate.latitude, lon: loc.coordinate.longitude) }

        }

    }



    private var legend: some View {

        HStack(spacing: 16) {

            HStack(spacing: 6) {

                Circle().fill(AppTheme.dealGreen).frame(width: 8, height: 8)

                Text("In our database").font(.caption).foregroundStyle(AppTheme.textMuted)

            }

            HStack(spacing: 6) {

                Circle().fill(Color.gray).frame(width: 8, height: 8)

                Text("Google only").font(.caption).foregroundStyle(AppTheme.textMuted)

            }

        }

        .padding(.horizontal, 16)

    }



    @ViewBuilder

    private func dealerRow(_ dealer: DealerRow) -> some View {

        VStack(alignment: .leading, spacing: 4) {

            HStack {

                Text(dealer.name ?? "Dealer")

                    .font(.subheadline.weight(.semibold))

                    .foregroundStyle(AppTheme.textPrimary)

                Spacer()

                if dealer.inDatabase == true {

                    Text("In database")

                        .font(.caption2.weight(.semibold))

                        .foregroundStyle(AppTheme.dealGreen)

                }

            }

            if !dealer.locationLine.isEmpty {

                Text(dealer.locationLine)

                    .font(.caption)

                    .foregroundStyle(AppTheme.textMuted)

            }

            HStack {

                if let d = dealer.distanceMiles {

                    Text(String(format: "%.1f mi", d))

                        .font(.caption)

                        .foregroundStyle(AppTheme.textMuted)

                }

                if let n = dealer.listingCount, dealer.inDatabase == true {

                    Text("· \(n) listings")

                        .font(.caption)

                        .foregroundStyle(AppTheme.textMuted)

                }

                Spacer()

                if selectedDealerId == dealer.id {

                    Label("Selected", systemImage: "mappin.circle.fill")

                        .font(.caption.weight(.semibold))

                        .foregroundStyle(AppTheme.navy)

                }

            }

        }

        .padding(.horizontal, 16)

        .padding(.vertical, 10)

        .background(selectedDealerId == dealer.id ? AppTheme.navy.opacity(0.08) : AppTheme.cardBackground)

    }



    private var compactForm: some View {

        FloatingCard {

            VStack(alignment: .leading, spacing: 8) {

                Button {

                    location.refreshLocation()

                    didAutoSearch = false

                } label: {

                    Label("Use my location", systemImage: "location.fill")

                        .font(.subheadline.weight(.medium))

                        .frame(maxWidth: .infinity, alignment: .leading)

                }

                .foregroundStyle(AppTheme.navy)



                HStack(spacing: 8) {

                    TextField("ZIP", text: $zipCode)

                        .keyboardType(.numberPad)

                        .textFieldStyle(.roundedBorder)

                    Picker("", selection: $radiusMiles) {

                        Text("10 mi").tag(10)

                        Text("25 mi").tag(25)

                        Text("50 mi").tag(50)

                    }

                    .pickerStyle(.menu)

                    .tint(AppTheme.navy)

                    Button("Search") {

                        Task { await searchFromZip() }

                    }

                    .font(.subheadline.weight(.semibold))

                    .foregroundStyle(.white)

                    .padding(.horizontal, 12)

                    .padding(.vertical, 8)

                    .background(AppTheme.navy)

                    .clipShape(Capsule())

                }



                Text(status)

                    .font(.caption2)

                    .foregroundStyle(AppTheme.textMuted)

                    .lineLimit(2)

            }

        }

        .padding(.horizontal, 16)

        .padding(.top, 4)

    }



    private func searchFromZip() async {

        let z = zipCode.trimmingCharacters(in: .whitespacesAndNewlines)

        guard z.count == 5 else {

            status = "Enter a valid 5-digit ZIP."

            return

        }

        await search(zip: z)

    }



    private func search(lat: Double? = nil, lon: Double? = nil, zip: String? = nil) async {

        isLoading = true

        selectedDealerId = nil

        defer { isLoading = false }

        do {

            let resp = try await APIClient.shared.fetchNearbyDealers(

                lat: lat,

                lon: lon,

                zip: zip,

                radius: Double(radiusMiles)

            )

            if resp.ok, let rows = resp.dealers {

                dealers = rows

                mapCenter = resp.center?.coordinate

                status = "Found \(rows.count) dealers"

                if let first = rows.first(where: { $0.latitude != nil && $0.longitude != nil }) {

                    selectedDealerId = first.id

                }

            } else {

                dealers = []

                mapCenter = nil

                status = resp.error ?? "No dealers found"

            }

        } catch {

            dealers = []

            mapCenter = nil

            status = error.localizedDescription

        }

    }

}


