import CryptoKit
import Foundation
import Security

/// TLS trust + optional public-key pinning for production API / WebView hosts.
enum PinnedTrustEvaluator {
    /// SHA-256 (base64) of certificate DER for sarraficars.com chain (leaf + intermediate).
    /// Update when rotating certs — see `ios/docs/TLS_PINNING.md`.
    private static let pinnedCertificateHashes: Set<String> = [
        "993tmD+h+fV08umQgRESAem2eSHtvfUNYfUJTuNctl8=",
        "rrH9dBDoO8lvXaPGp8LBu4NtH6XLhucIUViQ5Ciodws=",
    ]

    static var isPinningEnabled: Bool {
        #if DEBUG
        false
        #else
        true
        #endif
    }

    static func shouldPin(host: String) -> Bool {
        let h = host.lowercased()
        return h == "sarraficars.com" || h.hasSuffix(".sarraficars.com")
    }

    /// Validates server trust; pins production hosts in Release after system trust evaluation.
    static func evaluate(
        _ challenge: URLAuthenticationChallenge,
        completion: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        guard challenge.protectionSpace.authenticationMethod == NSURLAuthenticationMethodServerTrust,
              let trust = challenge.protectionSpace.serverTrust else {
            completion(.performDefaultHandling, nil)
            return
        }

        let host = challenge.protectionSpace.host

        if !isPinningEnabled || !shouldPin(host: host) {
            completion(.performDefaultHandling, nil)
            return
        }

        var error: CFError?
        guard SecTrustEvaluateWithError(trust, &error) else {
            completion(.cancelAuthenticationChallenge, nil)
            return
        }

        if chainContainsPinnedCertificate(trust) {
            completion(.useCredential, URLCredential(trust: trust))
        } else {
            completion(.cancelAuthenticationChallenge, nil)
        }
    }

    private static func chainContainsPinnedCertificate(_ trust: SecTrust) -> Bool {
        if #available(iOS 15.0, *) {
            guard let chain = SecTrustCopyCertificateChain(trust) as? [SecCertificate] else {
                return false
            }
            return chain.contains { cert in
                pinnedCertificateHashes.contains(certificateHash(cert))
            }
        }
        let count = SecTrustGetCertificateCount(trust)
        guard count > 0 else { return false }
        for index in 0 ..< count {
            guard let cert = SecTrustGetCertificateAtIndex(trust, index) else { continue }
            if pinnedCertificateHashes.contains(certificateHash(cert)) {
                return true
            }
        }
        return false
    }

    private static func certificateHash(_ certificate: SecCertificate) -> String {
        let der = SecCertificateCopyData(certificate) as Data
        let digest = SHA256.hash(data: der)
        return Data(digest).base64EncodedString()
    }
}

/// Retained delegate so ``URLSession`` performs pinning in Release.
final class PinningURLSessionDelegate: NSObject, URLSessionDelegate {
    func urlSession(
        _ session: URLSession,
        didReceive challenge: URLAuthenticationChallenge,
        completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void
    ) {
        PinnedTrustEvaluator.evaluate(challenge, completion: completionHandler)
    }
}
