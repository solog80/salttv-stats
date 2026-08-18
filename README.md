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
| `gcs_shipper.py` | Ships the NPM log to GCS/BigQuery as gzipped NDJSON (geo/ASN enriched) |

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
   [ip-api.com](https://ip-api.com) API (IPv4 only) and
   [ipwho.is](https://ipwho.is) (IPv6), cached to `/cache/geo.tsv` to avoid
   repeated lookups and respect rate limits. Datacenter/hosting IPs are
   identified by ASN blocklist + org keywords and excluded from counts by
   default (`filter_dc=0` to include them).

## BigQuery pipeline (GCS + external table)

In addition to the HTTP API, every NPM log line is shipped to Google
BigQuery for long-term storage and SQL analytics — no Firebase involved.

```
Edge NPM viewer log ──► log-shipper container (parse + geo/ASN enrich, gzip)
   ──► gs://salt-media-app1-viewer-logs/viewer-logs/date=YYYYMMDD/hour=HH/*.jsonl.gz
   ──► BigQuery external table viewer_logs.viewer_requests (live, free ingestion)
   ──► scheduled MERGE ──► viewer_logs.viewer_requests_native (DAY-partitioned on ts)
```

- **Shipper**: `gcs_shipper.py` runs as the `log-shipper` container on the
  edge. It reads the NPM log by byte offset (idempotent across restarts),
  parses each line, enriches with `country_code/country/isp/asn/
  is_datacenter`, and uploads gzipped NDJSON every 30 s. IPv4 geo via
  ip-api.com, IPv6 via ipwho.is, cached to `/state/geo.tsv` (seeded from the
  stats API cache).
- **External table** (`viewer_logs.viewer_requests`): reads GCS live via
  hive partitioning on `date`/`hour`. Schema: `ts, status, uri, stream,
  file_type, session, client_ip, user_agent, referer, country_code, country,
  isp, asn, is_datacenter`.
- **Filtered view** (`viewer_logs.viewer_requests_real`): `WHERE NOT
  is_datacenter` — the BigQuery equivalent of `filter_dc=1`.
- **Native table** (`viewer_logs.viewer_requests_native`): a scheduled query
  runs every 10 min and `MERGE`s new rows (deduped on `ts+client_ip+uri`)
  into a DAY-partitioned table for faster queries.

GCP setup: bucket `salt-media-app1-viewer-logs` (EU nearline); the
`firebase-adminsdk-ruyjd` service account holds `bigquery.dataEditor`,
`bigquery.jobUser`, and `storage.objectAdmin`. Credentials are mounted into
the container at `/creds/creds.json`.

Example SQL:

```sql
-- Real (non-datacenter) viewers per stream, per minute
SELECT TIMESTAMP_TRUNC(ts, MINUTE) AS minute, stream,
       COUNT(DISTINCT client_ip) AS viewers
FROM `viewer_logs.viewer_requests_real`
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
GROUP BY minute, stream ORDER BY minute DESC;

-- Countries of real viewers, last 30 min
SELECT country, COUNT(DISTINCT client_ip) AS viewers
FROM `viewer_logs.viewer_requests_real`
WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
GROUP BY country ORDER BY viewers DESC;
```

## Adding viewer stats to your React app

Never query BigQuery directly from the browser — the service account key would
be exposed. Instead, add a small **Firebase Function** that runs the SQL
server-side and returns JSON, then call it from React.

### 1. Firebase Function

In your Firebase project (`salt-media-app1`), add a callable function:

```js
// functions/index.js (or getViewerStats.js)
const functions = require("firebase-functions");
const admin = require("firebase-admin");
const { BigQuery } = require("@google-cloud/bigquery");

admin.initializeApp();
const bq = new BigQuery({ projectId: "salt-media-app1" });

const REAL_VIEWERS_SQL = `
  SELECT TIMESTAMP_TRUNC(ts, MINUTE) AS minute, stream,
         COUNT(DISTINCT client_ip) AS viewers
  FROM \`viewer_logs.viewer_requests_real\`
  WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @minutes MINUTE)
  GROUP BY minute, stream
  ORDER BY minute DESC`;

exports.getViewerStats = functions.https.onCall(async (data) => {
  const minutes = Math.min(Math.max(parseInt(data.minutes, 10) || 30, 1), 60 * 24);
  const [rows] = await bq.query({ query: REAL_VIEWERS_SQL, params: { minutes } });
  return { viewers: rows };
});

exports.getViewerCountries = functions.https.onCall(async () => {
  const [rows] = await bq.query({
    query: `
      SELECT country, COUNT(DISTINCT client_ip) AS viewers
      FROM \`viewer_logs.viewer_requests_real\`
      WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
      GROUP BY country ORDER BY viewers DESC`,
  });
  return { countries: rows };
});
```

```bash
# functions/package.json — add the BigQuery client
npm install @google-cloud/bigquery
firebase deploy --only functions
```

The function's runtime service account needs BigQuery read access (run once):

```bash
gcloud projects add-iam-policy-binding salt-media-app1 \
  --member="serviceAccount:salt-media-app1@appspot.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"
gcloud projects add-iam-policy-binding salt-media-app1 \
  --member="serviceAccount:salt-media-app1@appspot.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"
```

> Use `viewer_logs.viewer_requests_real` (the filtered view) for real
> viewer counts, or `viewer_logs.viewer_requests_native` (faster) with
> `WHERE NOT is_datacenter`.

### 2. React hook

```js
// useViewerStats.js
import { getFunctions, httpsCallable } from "firebase/functions";

export function useViewerStats(minutes = 30) {
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    const fn = httpsCallable(getFunctions(), "getViewerStats");
    let cancelled = false;
    fn({ minutes }).then(({ data }) => {
      if (!cancelled) setStats(data.viewers);
    }).catch(setError);
    return () => { cancelled = true; };
  }, [minutes]);

  return { stats, error };
}
```

```jsx
// MyViewerCard.jsx
import { useViewerStats } from "./useViewerStats";

export default function MyViewerCard() {
  const { stats, error } = useViewerStats(30);
  if (error) return <div>Error: {error.message}</div>;
  if (!stats) return <div>Loading…</div>;

  const total = stats.reduce((n, s) => n + s.viewers, 0);
  return (
    <div>
      <h3>Live viewers: {total}</h3>
      <ul>
        {stats.map(s => (
          <li key={s.minute}>{s.minute} — {s.stream}: {s.viewers}</li>
        ))}
      </ul>
    </div>
  );
}
```

### 3. Live numbers without a backend

For the current 5-minute live viewer count you can call the edge API directly
from the browser (CORS is open):

```js
fetch("https://stats.salttelevision.com/api/viewers?minutes=5&countries=1")
  .then(r => r.json())
  .then(d => console.log(d.viewers, d.countries));
```

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