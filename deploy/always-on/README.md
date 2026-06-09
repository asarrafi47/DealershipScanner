# Always-on hosting

Run the Sarrafi Cars Flask app and optional Cloudflare tunnel so **https://sarraficars.com** stays available for the website and iOS WebView shell.

This complements `start.sh` (interactive dev) with **supervised** process examples for macOS (`launchd`) and Linux (`systemd`).

## Quick start (foreground)

From the repo root:

```bash
./scripts/start-always-on.sh
```

Loads `.env`, starts Cloudflare tunnel + Flask via **`run.py`** (`PUBLIC=1`, port **5001** by default). Ctrl-C stops both.

For **supervised production** hosts, use the `launchd` / `systemd` examples below — they run **gunicorn** (same as `Dockerfile.web`), not the Werkzeug dev server in `run.py`.

## Environment

| Variable | Purpose |
|----------|---------|
| `PORT` | Flask listen port (default `5001`) |
| `PUBLIC` | Set to `1` for `0.0.0.0` bind (set by start script) |
| `FLASK_ENV` | Use `production` on a real host; set `SECRET_KEY` |
| Cloudflare | Tunnel name/token in your `cloudflared` config (see `start.sh`) |

Copy production secrets from your existing deploy; never commit `.env`.

## macOS — launchd

Example plists live in `deploy/always-on/launchd/`.

1. Edit paths and your macOS username in each plist.
2. Install:

```bash
cp deploy/always-on/launchd/com.sarraficars.web.plist ~/Library/LaunchAgents/
cp deploy/always-on/launchd/com.sarraficars.tunnel.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.sarraficars.web.plist
launchctl load ~/Library/LaunchAgents/com.sarraficars.tunnel.plist
```

3. Logs: `/tmp/sarraficars-web.log`, `/tmp/sarraficars-tunnel.log` (configurable in plists).

Unload with `launchctl unload ...` before removing plists.

## Linux — systemd

Example units in `deploy/always-on/systemd/`.

1. Edit `WorkingDirectory`, `User`, and `EnvironmentFile` in each unit.
2. Install:

```bash
sudo cp deploy/always-on/systemd/sarraficars-web.service /etc/systemd/system/
sudo cp deploy/always-on/systemd/cloudflared-tunnel.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now sarraficars-web.service cloudflared-tunnel.service
```

3. Status: `systemctl status sarraficars-web cloudflared-tunnel`

## Health check

After start:

```bash
curl -sS -o /dev/null -w "%{http_code}\n" http://127.0.0.1:5001/login
```

Expect `200`. Public URL should match your tunnel/DNS config.

## iOS note

The iOS app loads the public HTTPS origin. If the tunnel or server is down, the WebView shows a network error until service is restored.
