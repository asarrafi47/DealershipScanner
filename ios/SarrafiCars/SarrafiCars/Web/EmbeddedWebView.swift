import SwiftUI
import WebKit

/// Simple web page embed without toolbar bindings.
struct EmbeddedWebView: View {
    let url: URL
    var stripSiteChrome: Bool = false
    var embedBodyClass: String = "premium-embed"

    @State private var canGoBack = false
    @State private var reloadToken = UUID()
    @State private var webViewRef: WKWebViewBox?

    var body: some View {
        WebView(
            url: url,
            stripSiteChrome: stripSiteChrome,
            embedBodyClass: embedBodyClass,
            canGoBack: $canGoBack,
            reloadToken: reloadToken,
            webViewRef: $webViewRef
        )
    }
}

struct CarWebDetailView: View {
    let carId: Int

    private var carURL: URL {
        var components = URLComponents(url: AppConfig.baseURL.appending(path: "car/\(carId)"), resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "embed", value: "1")]
        return components.url ?? AppConfig.baseURL.appending(path: "car/\(carId)")
    }

    var body: some View {
        EmbeddedWebView(url: carURL, stripSiteChrome: true, embedBodyClass: "car-embed")
            .navigationTitle("Vehicle")
            .navigationBarTitleDisplayMode(.inline)
    }
}
