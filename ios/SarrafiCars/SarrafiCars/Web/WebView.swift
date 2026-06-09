import SwiftUI
import WebKit

/// Holds a weak reference to the embedded web view for optional toolbar actions.
final class WKWebViewBox: ObservableObject {
    weak var webView: WKWebView?
}

struct WebView: UIViewRepresentable {
    let url: URL
    var stripSiteChrome: Bool = false
    /// Body class added when stripping chrome (`car-embed`, `premium-embed`, …).
    var embedBodyClass: String = "premium-embed"
    @Binding var canGoBack: Bool
    let reloadToken: UUID
    @Binding var webViewRef: WKWebViewBox?

    func makeCoordinator() -> Coordinator {
        Coordinator(parent: self)
    }

    func makeUIView(context: Context) -> WKWebView {
        let config = WKWebViewConfiguration()
        config.websiteDataStore = .default()
        config.defaultWebpagePreferences.allowsContentJavaScript = true
        let webView = WKWebView(frame: .zero, configuration: config)
        webView.navigationDelegate = context.coordinator
        webView.allowsBackForwardNavigationGestures = false
        webView.scrollView.contentInsetAdjustmentBehavior = .automatic

        let refresh = UIRefreshControl()
        refresh.addTarget(context.coordinator, action: #selector(Coordinator.handleRefresh), for: .valueChanged)
        webView.scrollView.refreshControl = refresh
        context.coordinator.refreshControl = refresh

        SessionCookieBridge.syncToWebView()
        webView.load(URLRequest(url: url))
        let box = WKWebViewBox()
        box.webView = webView
        DispatchQueue.main.async { webViewRef = box }
        context.coordinator.webView = webView
        return webView
    }

    func updateUIView(_ webView: WKWebView, context: Context) {
        if context.coordinator.lastReloadToken != reloadToken {
            context.coordinator.lastReloadToken = reloadToken
            webView.reload()
        }
    }

    final class Coordinator: NSObject, WKNavigationDelegate {
        var parent: WebView
        weak var webView: WKWebView?
        weak var refreshControl: UIRefreshControl?
        var lastReloadToken: UUID

        init(parent: WebView) {
            self.parent = parent
            self.lastReloadToken = parent.reloadToken
        }

        @objc func handleRefresh() {
            webView?.reload()
        }

        func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
            parent.canGoBack = webView.canGoBack
            refreshControl?.endRefreshing()
            if parent.stripSiteChrome {
                let bodyClass = parent.embedBodyClass
                    .replacingOccurrences(of: "\\", with: "\\\\")
                    .replacingOccurrences(of: "'", with: "\\'")
                let js = """
                (function () {
                  document.body.classList.add('\(bodyClass)');
                  document.querySelectorAll('.topbar, .guest-banner, .nav-search, .vdp-back').forEach(function (el) {
                    el.style.display = 'none';
                  });
                })();
                """
                webView.evaluateJavaScript(js, completionHandler: nil)
            }
        }

        func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) {
            refreshControl?.endRefreshing()
        }

        func webView(
            _ webView: WKWebView,
            decidePolicyFor navigationAction: WKNavigationAction,
            decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
        ) {
            guard let targetURL = navigationAction.request.url else {
                decisionHandler(.cancel)
                return
            }
            if AppConfig.isAllowedInAppHost(targetURL) {
                decisionHandler(.allow)
                return
            }
            if navigationAction.navigationType == .linkActivated {
                UIApplication.shared.open(targetURL)
            }
            decisionHandler(.cancel)
        }

        func webView(
            _ webView: WKWebView,
            decidePolicyFor navigationResponse: WKNavigationResponse,
            decisionHandler: @escaping (WKNavigationResponsePolicy) -> Void
        ) {
            if let http = navigationResponse.response as? HTTPURLResponse,
               let finalURL = http.url,
               !AppConfig.isAllowedInAppHost(finalURL) {
                decisionHandler(.cancel)
                return
            }
            decisionHandler(.allow)
        }

        func webView(
            _ webView: WKWebView,
            didReceive challenge: URLAuthenticationChallenge,
            completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
        ) {
            PinnedTrustEvaluator.evaluate(challenge, completion: completionHandler)
        }
    }
}
