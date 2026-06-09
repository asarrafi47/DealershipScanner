import SwiftUI

/// Website design tokens from `frontend/static/style.css`.
enum AppTheme {
    static let navy = Color(red: 28 / 255, green: 53 / 255, blue: 82 / 255)
    static let navyDeep = Color(red: 20 / 255, green: 42 / 255, blue: 64 / 255)
    static let pageBackground = Color(red: 244 / 255, green: 242 / 255, blue: 236 / 255)
    static let cardBackground = Color.white
    static let textPrimary = Color(red: 26 / 255, green: 39 / 255, blue: 68 / 255)
    static let textMuted = Color(red: 92 / 255, green: 103 / 255, blue: 120 / 255)
    static let dealGreen = Color(red: 22 / 255, green: 101 / 255, blue: 52 / 255)
    static let cream = Color(red: 244 / 255, green: 242 / 255, blue: 236 / 255)
    static let woodTrim = Color(red: 139 / 255, green: 94 / 255, blue: 60 / 255)

    static var pageGradient: LinearGradient {
        LinearGradient(
            colors: [pageBackground, pageBackground.opacity(0.92)],
            startPoint: .top,
            endPoint: .bottom
        )
    }
}

/// Full-width navy header matching website `.topbar`.
struct SarrafiHeaderBar: View {
    var body: some View {
        VStack(spacing: 0) {
            VStack(spacing: 10) {
                Text("Sarrafi Cars")
                    .font(.system(size: 20, weight: .semibold))
                    .tracking(1.8)
                    .textCase(.uppercase)
                    .foregroundStyle(AppTheme.cream)
                    .frame(maxWidth: .infinity, alignment: .center)

                HStack {
                    Spacer()
                    AccountHeaderButtons(variant: .navyBar)
                }
            }
            .padding(.horizontal, 20)
            .padding(.top, 14)
            .padding(.bottom, 14)
            .background(
                LinearGradient(
                    colors: [AppTheme.navy, AppTheme.navyDeep],
                    startPoint: .top,
                    endPoint: .bottom
                )
            )

            Rectangle()
                .fill(AppTheme.woodTrim)
                .frame(height: 3)
        }
    }
}

struct PrimaryNavyButton: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.headline)
            .foregroundStyle(.white)
            .frame(maxWidth: .infinity)
            .padding(.vertical, 14)
            .background(AppTheme.navy.opacity(configuration.isPressed ? 0.85 : 1))
            .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
    }
}

struct FloatingCard<Content: View>: View {
    @ViewBuilder var content: Content

    var body: some View {
        content
            .padding(14)
            .background(AppTheme.cardBackground)
            .clipShape(RoundedRectangle(cornerRadius: 16, style: .continuous))
            .shadow(color: Color.black.opacity(0.08), radius: 16, y: 8)
    }
}

/// Standard page chrome: navy header + optional title strip (matches website app nav pages).
struct AppScreenShell<Content: View>: View {
    var title: String?
    var subtitle: String?
    @ViewBuilder var content: () -> Content

    var body: some View {
        VStack(spacing: 0) {
            SarrafiHeaderBar()
            if title != nil || subtitle != nil {
                VStack(alignment: .leading, spacing: 4) {
                    if let title {
                        Text(title)
                            .font(.title3.weight(.semibold))
                            .foregroundStyle(AppTheme.textPrimary)
                    }
                    if let subtitle {
                        Text(subtitle)
                            .font(.caption)
                            .foregroundStyle(AppTheme.textMuted)
                            .fixedSize(horizontal: false, vertical: true)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(.horizontal, 16)
                .padding(.top, 10)
                .padding(.bottom, 6)
            }
            content()
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        .background(AppTheme.pageGradient.ignoresSafeArea())
    }
}

extension AppTheme {
    /// Mouse / trackpad scrolling on Mac (iOS app on Mac) works better with visible scroll bars.
    static var showsScrollIndicators: Bool {
        ProcessInfo.processInfo.isiOSAppOnMac
    }
}

/// Collapsible filter block — keeps Home above-the-fold tight until the user expands refine.
struct CollapsibleRefineSection<Content: View>: View {
    @Binding var isExpanded: Bool
    var activeFilterCount: Int
    @ViewBuilder var content: () -> Content

    var body: some View {
        DisclosureGroup(isExpanded: $isExpanded) {
            content()
                .padding(.top, 8)
        } label: {
            HStack(spacing: 8) {
                Text("Refine results")
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(AppTheme.textPrimary)
                if activeFilterCount > 0 {
                    Text("\(activeFilterCount)")
                        .font(.caption2.weight(.bold))
                        .foregroundStyle(.white)
                        .padding(.horizontal, 7)
                        .padding(.vertical, 3)
                        .background(AppTheme.navy)
                        .clipShape(Capsule())
                }
                Spacer(minLength: 0)
            }
        }
        .tint(AppTheme.navy)
    }
}
