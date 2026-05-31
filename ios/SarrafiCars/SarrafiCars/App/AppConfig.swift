import Foundation
import Network

enum AppConfig {
    /// Debug builds talk to local Flask (`./start.sh` → :5001). Release uses production.
    static let baseURL: URL = {
        #if DEBUG
        if let raw = Bundle.main.object(forInfoDictionaryKey: "SARRAFI_API_BASE_URL") as? String,
           let url = URL(string: raw),
           isAllowedAPIBaseURL(url) {
            return url
        }
        return URL(string: "http://127.0.0.1:5001")!
        #else
        return URL(string: "https://sarraficars.com")!
        #endif
    }()

    static let pageSize = 40

    /// Hosts permitted inside ``WKWebView`` (must match API origin in dev).
    static func isAllowedInAppHost(_ url: URL) -> Bool {
        if url.scheme?.lowercased() == "about" { return true }
        guard let host = url.host?.lowercased(), !host.isEmpty else { return false }
        if host == "sarraficars.com" || host.hasSuffix(".sarraficars.com") { return true }
        if host == "localhost" || host == "127.0.0.1" { return true }
        if let apiHost = baseURL.host?.lowercased(), host == apiHost { return true }
        #if DEBUG
        if host.hasPrefix("192.168.") || host.hasPrefix("10.") { return true }
        #endif
        return false
    }

    /// Debug-only override must not point at arbitrary internet origins.
    static func isAllowedAPIBaseURL(_ url: URL) -> Bool {
        #if DEBUG
        return isAllowedInAppHost(url)
        #else
        return false
        #endif
    }

    /// Inventory images: HTTPS in Release; block script/data URLs and private hosts.
    static func resolveSafeImageURL(_ raw: String?) -> URL? {
        guard let raw, !raw.isEmpty else { return nil }
        let trimmed = raw.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty || trimmed == "—" { return nil }

        let lower = trimmed.lowercased()
        if lower.hasPrefix("javascript:") || lower.hasPrefix("data:")
            || lower.hasPrefix("file:") || lower.hasPrefix("vbscript:") {
            return nil
        }

        guard let url = resolveURL(trimmed) else { return nil }
        return isAllowedImageURL(url) ? url : nil
    }

    static func isAllowedImageURL(_ url: URL) -> Bool {
        if url.scheme?.lowercased() == "about" { return false }
        guard let scheme = url.scheme?.lowercased() else { return false }

        if trimmedPathOnly(url) {
            return scheme == "https" || (scheme == "http" && isAllowedInAppHost(url))
        }

        #if DEBUG
        if scheme == "http", isAllowedInAppHost(url) {
            return !isBlockedImageHost(url)
        }
        #endif

        guard scheme == "https" else { return false }
        return !isBlockedImageHost(url)
    }

    static func resolveURL(_ raw: String?) -> URL? {
        guard let raw, !raw.isEmpty else { return nil }
        if raw.hasPrefix("http://") || raw.hasPrefix("https://") {
            return URL(string: raw)
        }
        if raw.hasPrefix("/") {
            return URL(string: raw, relativeTo: baseURL)
        }
        return URL(string: raw, relativeTo: baseURL)
    }

    static func formatPrice(_ value: Double?) -> String {
        guard let value, value > 0 else { return "—" }
        let f = NumberFormatter()
        f.numberStyle = .currency
        f.maximumFractionDigits = 0
        f.locale = Locale(identifier: "en_US")
        return f.string(from: NSNumber(value: value)) ?? "—"
    }

    static func formatMileage(_ value: Int?) -> String {
        guard let value, value >= 0 else { return "—" }
        let f = NumberFormatter()
        f.numberStyle = .decimal
        f.maximumFractionDigits = 0
        return (f.string(from: NSNumber(value: value)) ?? "\(value)") + " mi"
    }

    private static func trimmedPathOnly(_ url: URL) -> Bool {
        url.host == nil && url.scheme != nil
    }

    private static func isBlockedImageHost(_ url: URL) -> Bool {
        guard let host = url.host?.lowercased(), !host.isEmpty else { return true }
        if host == "localhost" || host == "127.0.0.1" || host == "::1" {
            return false
        }
        if host == "sarraficars.com" || host.hasSuffix(".sarraficars.com") {
            return false
        }
        if let apiHost = baseURL.host?.lowercased(), host == apiHost {
            return false
        }
        #if DEBUG
        if host.hasPrefix("192.168.") || host.hasPrefix("10.") { return false }
        #endif
        if let v4 = IPv4Address(host) {
            return isPrivateIPv4(v4) || v4.isLoopback || v4.isMulticast || v4.isLinkLocal
        }
        if let v6 = IPv6Address(host) {
            return isPrivateIPv6(v6) || v6.isLoopback || v6.isMulticast || v6.isLinkLocal
        }
        return false
    }

    /// RFC 1918 — `IPv4Address.isPrivate` is not available on all deployment SDKs.
    private static func isPrivateIPv4(_ address: IPv4Address) -> Bool {
        let b = address.rawValue
        if b[0] == 10 { return true }
        if b[0] == 172 && (b[1] & 0xF0) == 16 { return true }
        if b[0] == 192 && b[1] == 168 { return true }
        return false
    }

    /// Unique local (fc00::/7) — same rationale as ``isPrivateIPv4``.
    private static func isPrivateIPv6(_ address: IPv6Address) -> Bool {
        (address.rawValue[0] & 0xFE) == 0xFC
    }
}
