import SwiftUI

/// Premium checkout — native header only; page loads without site nav (`?embed=1`).
struct PremiumWebView: View {
    private var premiumURL: URL {
        var components = URLComponents(url: AppConfig.baseURL.appending(path: "premium"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "embed", value: "1")]
        return components.url ?? AppConfig.baseURL.appending(path: "premium")
    }

    var body: some View {
        VStack(spacing: 0) {
            SarrafiHeaderBar()
            EmbeddedWebView(url: premiumURL, stripSiteChrome: true)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .background(AppTheme.pageBackground)
    }
}
