import SwiftUI
import MapKit

struct DealerMapPin: Identifiable {
    let id: String
    let name: String
    let coordinate: CLLocationCoordinate2D
    let inDatabase: Bool
}

struct DealersMapView: View {
    let dealers: [DealerRow]
    let center: CLLocationCoordinate2D?
    @Binding var selectedDealerId: String?
    var onOpenInMaps: (DealerRow) -> Void

    @State private var region = MKCoordinateRegion(
        center: CLLocationCoordinate2D(latitude: 39.8283, longitude: -98.5795),
        span: MKCoordinateSpan(latitudeDelta: 0.35, longitudeDelta: 0.35)
    )

    private var pins: [DealerMapPin] {
        dealers.compactMap { dealer in
            guard let lat = dealer.latitude, let lon = dealer.longitude else { return nil }
            return DealerMapPin(
                id: dealer.id,
                name: dealer.name ?? "Dealer",
                coordinate: CLLocationCoordinate2D(latitude: lat, longitude: lon),
                inDatabase: dealer.inDatabase == true
            )
        }
    }

    var body: some View {
        ZStack(alignment: .bottom) {
            Map(coordinateRegion: $region, annotationItems: pins) { pin in
                MapAnnotation(coordinate: pin.coordinate) {
                    Button {
                        selectedDealerId = pin.id
                        centerOn(pin.coordinate)
                    } label: {
                        Circle()
                            .fill(pinFill(for: pin))
                            .frame(width: pin.id == selectedDealerId ? 16 : 12, height: pin.id == selectedDealerId ? 16 : 12)
                            .overlay(Circle().stroke(Color.white, lineWidth: 2))
                            .shadow(color: .black.opacity(0.2), radius: 2, y: 1)
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel(pin.name)
                }
            }
            .contentShape(Rectangle())
            .onTapGesture {
                guard let dealer = selectedDealer else { return }
                onOpenInMaps(dealer)
            }

            if let dealer = selectedDealer {
                Button {
                    onOpenInMaps(dealer)
                } label: {
                    Label("Open in Apple Maps", systemImage: "map.fill")
                        .font(.subheadline.weight(.semibold))
                        .frame(maxWidth: .infinity)
                        .padding(.vertical, 10)
                        .background(AppTheme.navy)
                        .foregroundStyle(.white)
                        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
                }
                .padding(10)
            }
        }
        .onAppear { fitRegion() }
        .onChange(of: dealers.count) { _ in fitRegion() }
        .onChange(of: center?.latitude) { _ in fitRegion() }
        .onChange(of: selectedDealerId) { id in
            guard let id, let pin = pins.first(where: { $0.id == id }) else { return }
            centerOn(pin.coordinate)
        }
    }

    private var selectedDealer: DealerRow? {
        guard let selectedDealerId else { return nil }
        return dealers.first { $0.id == selectedDealerId }
    }

    private func pinFill(for pin: DealerMapPin) -> Color {
        if pin.id == selectedDealerId { return AppTheme.navy }
        return pin.inDatabase ? AppTheme.dealGreen : Color.gray
    }

    private func centerOn(_ coordinate: CLLocationCoordinate2D) {
        region = MKCoordinateRegion(
            center: coordinate,
            span: MKCoordinateSpan(latitudeDelta: 0.12, longitudeDelta: 0.12)
        )
    }

    private func fitRegion() {
        if let center {
            var minLat = center.latitude
            var maxLat = center.latitude
            var minLon = center.longitude
            var maxLon = center.longitude

            for pin in pins {
                minLat = min(minLat, pin.coordinate.latitude)
                maxLat = max(maxLat, pin.coordinate.latitude)
                minLon = min(minLon, pin.coordinate.longitude)
                maxLon = max(maxLon, pin.coordinate.longitude)
            }

            let latDelta = max((maxLat - minLat) * 1.35, 0.08)
            let lonDelta = max((maxLon - minLon) * 1.35, 0.08)
            region = MKCoordinateRegion(
                center: CLLocationCoordinate2D(
                    latitude: (minLat + maxLat) / 2,
                    longitude: (minLon + maxLon) / 2
                ),
                span: MKCoordinateSpan(latitudeDelta: latDelta, longitudeDelta: lonDelta)
            )
            return
        }

        guard let first = pins.first else { return }
        region = MKCoordinateRegion(
            center: first.coordinate,
            span: MKCoordinateSpan(latitudeDelta: 0.25, longitudeDelta: 0.25)
        )
    }
}

enum DealerMapsLauncher {
    static func openDirections(to dealer: DealerRow) {
        guard let lat = dealer.latitude, let lon = dealer.longitude else { return }
        let coordinate = CLLocationCoordinate2D(latitude: lat, longitude: lon)
        let placemark = MKPlacemark(coordinate: coordinate)
        let item = MKMapItem(placemark: placemark)
        item.name = dealer.name ?? "Dealership"
        item.openInMaps(launchOptions: [
            MKLaunchOptionsDirectionsModeKey: MKLaunchOptionsDirectionsModeDriving,
        ])
    }
}
