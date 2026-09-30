import SwiftUI

/// Editorial listing: square-cornered photo, text beneath, a hairline under each entry.
/// No card fill, stroke, radius, or shadow (house rule: no boxes).
struct ListingCardView: View {
    let car: ListingCar
    var isSaved: Bool = false
    var showSaveControl: Bool = false
    var onSaveTap: (() -> Void)?

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            ZStack(alignment: .topTrailing) {
                RemoteImage(url: AppConfig.resolveSafeImageURL(car.primaryImagePath))
                    .frame(height: 120)
                    .clipped()

                if showSaveControl, let onSaveTap {
                    Button(action: onSaveTap) {
                        Image(systemName: isSaved ? "heart.fill" : "heart")
                            .font(.body.weight(.semibold))
                            .foregroundStyle(isSaved ? AppTheme.navy : .white)
                            .padding(8)
                            .background(.black.opacity(0.35), in: RoundedRectangle(cornerRadius: 4, style: .continuous))
                    }
                    .buttonStyle(.plain)
                    .padding(6)
                    .accessibilityLabel(isSaved ? "Unsave vehicle" : "Save vehicle")
                }
            }

            VStack(alignment: .leading, spacing: 4) {
                Text(car.headline)
                    .font(.subheadline.weight(.semibold))
                    .lineLimit(2)
                    .foregroundStyle(AppTheme.textPrimary)

                Text(AppConfig.formatPrice(car.price))
                    .font(.headline)
                    .foregroundStyle(AppTheme.navy)

                HStack {
                    Text(AppConfig.formatMileage(car.mileage))
                        .font(.caption)
                        .foregroundStyle(AppTheme.textMuted)
                    Spacer()
                    if let dealer = car.dealerName, !dealer.isEmpty, dealer != "—" {
                        Text(dealer)
                            .font(.caption2)
                            .foregroundStyle(AppTheme.textMuted.opacity(0.85))
                            .lineLimit(1)
                    }
                }
            }
            .padding(.top, 8)
            .padding(.bottom, 10)

            Divider()
        }
    }
}

#Preview {
    ListingCardView(
        car: ListingCar(
            id: 1,
            title: "2021 Jeep Grand Cherokee Limited",
            year: 2021,
            make: "Jeep",
            model: "Grand Cherokee",
            trim: "Limited",
            cylinders: nil,
            price: 32999,
            mileage: 41000,
            imageUrl: nil,
            gallery: nil,
            dealerName: "Demo Dealer",
            exteriorColor: nil,
            interiorColor: nil,
            drivetrain: nil,
            transmission: nil,
            bodyStyle: nil,
            fuelType: nil,
            engineL: nil,
            packageNames: nil,
            dealershipRegistryId: nil,
            zipCode: nil,
            dealerUrl: "https://example.com"
        ),
        isSaved: true,
        showSaveControl: true,
        onSaveTap: {}
    )
    .padding()
}
