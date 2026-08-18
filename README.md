# saltTV Stats API

Live viewer statistics API for the saltTV OvenMediaEngine (OME) LLHLS/ABR
streaming platform, serving real viewer counts with country and ISP breakdown.

## Architecture

```
Viewer ──► Bunny CDN ──► NPM (backup.salttelevision.com) ──► Varnish :8081 ──► OME Edge :3333 ──► OVT pull ──► Origins
                                                                                                     ├─ app/stream
                                                                                                     └─ app/stream2
```

Bunny forwards the real viewer IP in the `X-Forwarded-For` header. NPM's
default access log only records the last hop (the Bunny PoP IP), so this
project adds a **second NPM access log** that captures the true viewer IP, and
a small HTTP API that reads it to compute live/peak viewer stats.

## Components

| File | Purpose |
|------|---------|
| `viewers_api.py` | Self-contained stdlib-only Python HTTP API (no pip deps) |
| `viewers.sh` | CLI version of the same stats, for quick terminal use |
| `viewers_nginx.conf` | NPM custom config that enables the real-viewer access log |

## API Endpoints

Base URL: `https://stats.salttelevision.com`

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Liveness check |
| `GET /api/viewers?minutes=5&countries=1` | Live viewers in the last N minutes (default 5), with per-stream counts and optional country/ISP breakdown |
| `GET /api/viewers/peak?minutes=60` | Peak concurrent viewers (all-time by default, or within the last N minutes) |

### Example responses

`GET /api/viewers?minutes=5&countries=1`:

```json
{
  "viewers": 24,
  "window_minutes": 5,
  "streams": {
    "app/stream": 21,
    "app/stream2": 2
  },
  "countries": [
    { "code": "UG", "name": "Uganda", "viewers": 17 },
    { "code": "AE", "name": "United Arab Emirates", "viewers": 2 }
  ],
  "isps": [
    { "code": "UG", "isp": "Airtel Uganda", "viewers": 2 },
    { "code": "UG", "isp": "MTN Uganda", "viewers": 2 }
  ]
}
```

`GET /api/viewers/peak`:

```json
{
  "peak_viewers": 12,
  "peak_time": "2026-08-18T06:49:00+00:00",
  "window_minutes": null,
  "samples": 24,
  "current_viewers": 11
}
```

## Query parameters

| Param | Values | Applies to |
|-------|--------|------------|
| `minutes` | 1–60 (default 5) | `/api/viewers` |
| `countries` | `1`/`true`/`yes` | `/api/viewers` — include country + ISP breakdown |
| `minutes` | any positive int | `/api/viewers/peak` — limit peak to the last N minutes (omit = all history) |

CORS is open (`Access-Control-Allow-Origin: *`) so the API can be called
directly from a browser or the site's frontend:

```js
fetch("https://stats.salttelevision.com/api/viewers?minutes=5&countries=1")
```

## How it works

1. **Real-viewer log.** NPM's `server_proxy.conf` custom include adds
   `access_log /data/logs/proxy-host-4_viewers.log realviewer;` with a
   `log_format` that logs `client=$http_x_forwarded_for` (the first, real
   viewer IP forwarded by Bunny). IPv6 addresses and Bunny's own prefetches
   are excluded (geo provider is IPv4-only).

2. **API container.** `viewer-stats-api` (a `python:3.9-alpine` container on
   the edge) mounts the NPM log read-only and serves the endpoints. It
   maintains a rolling history of per-minute distinct-viewer counts in
   `/cache/history.jsonl` (backfilled from the log on start, sampled every
   60 s), which powers the peak endpoint.

3. **Geo lookup.** Country/ISP resolution uses the free
   [ip-api.com](https://ip-api.com) API (IPv4 only), cached to
   `/cache/geo.tsv` to avoid repeated lookups and respect rate limits.

## Deployment

On the edge server (`198.204.224.170`):

```bash
# NPM custom config (host-side at /data/compose/21/data/nginx/custom/)
cat > http_top.conf <<'EOF'
log_format realviewer '$time_local $status "$request_uri" client=$http_x_forwarded_for ua="$http_user_agent" ref="$http_referer"';
EOF
cat > server_proxy.conf <<'EOF'
access_log /data/logs/proxy-host-4_viewers.log realviewer;
EOF
docker exec npm-app-1 nginx -s reload

# API container
docker run -d --name viewer-stats-api --restart unless-stopped \
  -p 8099:8099 \
  -v /home/customer/vstats/viewers_api.py:/app/viewers_api.py:ro \
  -v /data/compose/21/data/logs:/data/logs:ro \
  -v /home/customer/vstats/cache:/cache \
  -e VIEWER_LOG=/data/logs/proxy-host-4_viewers.log \
  -e PORT=8099 \
  python:3.9-alpine python /app/viewers_api.py
```

### NPM proxy host

Add a proxy host in Nginx Proxy Manager:

- Domain: `stats.salttelevision.com`
- Scheme: `http`, Forward host `172.18.0.1`, Port `8099`
  (use the Docker bridge gateway — `127.0.0.1` inside NPM will not reach the host)
- SSL: Let's Encrypt certificate, force HTTPS

DNS for the domain must resolve to the edge server's public IP. Remove any
stale A records pointing elsewhere or Let's Encrypt validation will fail.

## CLI usage

```bash
./viewers.sh                # viewers in the last 1 minute
./viewers.sh 3              # viewers in the last 3 minutes
./viewers.sh 3 --countries  # + country + ISP breakdown
```

## Notes

- `viewers` counts **distinct viewer IPs** observed in the window, which is
  the closest truthful measure available behind a CDN.
- OME's internal `LLHLS(n)` log counters are cumulative session counters, not
  concurrent viewers — the NPM real-viewer log is the authoritative source.
- The NPM custom config files (`http_top.conf`, `server_proxy.conf`) survive
  container restarts but may be lost on NPM reinstall/recreate — keep copies
  here.