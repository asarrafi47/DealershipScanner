import Foundation
import WebKit

/// Keeps Flask session cookies aligned between ``URLSession`` and ``WKWebView``.
@MainActor
enum SessionCookieBridge {
    static func syncToWebView() {
        guard let cookies = HTTPCookieStorage.shared.cookies else { return }
        let store = WKWebsiteDataStore.default().httpCookieStore
        for cookie in cookies {
            store.setCookie(cookie, completionHandler: nil)
        }
    }

    /// Copy Flask session cookies from WKWebView (e.g. after HTML ``/register``) into URLSession.
    static func syncFromWebView() async {
        let hosts = hostSuffixesForAPI()
        let cookies = await allWebViewCookies()
        let storage = HTTPCookieStorage.shared
        for cookie in cookies where cookieMatches(cookie, hosts: hosts) {
            storage.setCookie(cookie)
        }
    }

    static func clearSessionCookies() async {
        let hosts = hostSuffixesForAPI()
        if let cookies = HTTPCookieStorage.shared.cookies {
            for cookie in cookies where cookieMatches(cookie, hosts: hosts) {
                HTTPCookieStorage.shared.deleteCookie(cookie)
            }
        }
        let store = WKWebsiteDataStore.default().httpCookieStore
        for cookie in await allWebViewCookies() where cookieMatches(cookie, hosts: hosts) {
            await deleteWebViewCookie(cookie, from: store)
        }
    }

    /// `withCheckedContinuation` is nonisolated; WebKit cookie store is `@MainActor`.
    private static func allWebViewCookies() async -> [HTTPCookie] {
        await withCheckedContinuation { continuation in
            Task { @MainActor in
                WKWebsiteDataStore.default().httpCookieStore.getAllCookies { cookies in
                    continuation.resume(returning: cookies)
                }
            }
        }
    }

    private static func deleteWebViewCookie(_ cookie: HTTPCookie, from store: WKHTTPCookieStore) async {
        await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
            store.delete(cookie) { continuation.resume() }
        }
    }

    private static func hostSuffixesForAPI() -> [String] {
        var hosts: [String] = ["sarraficars.com", ".sarraficars.com", "localhost", "127.0.0.1"]
        if let apiHost = AppConfig.baseURL.host?.lowercased(), !apiHost.isEmpty {
            hosts.append(apiHost)
            if apiHost.hasPrefix("www.") {
                hosts.append(String(apiHost.dropFirst(4)))
            }
        }
        return hosts
    }

    private static func cookieMatches(_ cookie: HTTPCookie, hosts: [String]) -> Bool {
        let domain = cookie.domain.lowercased()
        return hosts.contains { suffix in
            let s = suffix.lowercased()
            if s.hasPrefix(".") {
                return domain.hasSuffix(s) || domain == String(s.dropFirst())
            }
            return domain == s || domain.hasSuffix(".\(s)")
        }
    }
}
