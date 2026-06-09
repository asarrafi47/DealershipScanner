# TLS certificate pinning (iOS Release)

Release builds pin **sarraficars.com** (and subdomains) in `PinnedTrustEvaluator.swift` using SHA-256 hashes of certificate DER bytes from the live chain.

## When to update pins

After renewing or changing the TLS certificate (e.g. Cloudflare rotation), update `pinnedCertificateHashes` in:

`ios/SarrafiCars/SarrafiCars/Networking/PinnedTrustEvaluator.swift`

## How to generate new hashes

```bash
python3 << 'PY'
import subprocess, hashlib, base64
out = subprocess.run(
    ["openssl", "s_client", "-connect", "sarraficars.com:443", "-servername", "sarraficars.com", "-showcerts"],
    input=b"", capture_output=True, timeout=15,
)
text = out.stdout.decode(errors="replace")
buf, in_cert, certs = [], False, []
for line in text.splitlines():
    if "BEGIN CERTIFICATE" in line:
        in_cert, buf = True, [line]
    elif "END CERTIFICATE" in line:
        buf.append(line)
        pem = "\n".join(buf)
        der = subprocess.run(["openssl", "x509", "-outform", "DER"], input=pem.encode(), capture_output=True).stdout
        certs.append(base64.b64encode(hashlib.sha256(der).digest()).decode())
        in_cert = False
    elif in_cert:
        buf.append(line)
for i, h in enumerate(certs):
    print(f'        "{h}",  // cert[{i}]')
PY
```

Keep **at least two** hashes (leaf + intermediate) when possible. Test a Release archive against production before shipping.

Debug builds skip pinning (local HTTP / custom `SARRAFI_API_BASE_URL`).
