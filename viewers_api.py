#!/usr/bin/env python3
"""Viewer stats API for the LLHLS edge.
Reads the NPM real-viewer log (X-Forwarded-For), computes distinct viewers
over a window, and returns JSON with optional country/ISP breakdown.

IPv6 viewers are counted. Geo: IPv4 via ip-api.com, IPv6 via ipwho.is.
Datacenter/hosting IPs (by ASN or org keyword) can be excluded via filter_dc=1.

Endpoints:
  GET /health                          -> {"status": "ok"}
  GET /api/viewers?minutes=5           -> live viewers (+ streams, countries=1)
  GET /api/viewers?minutes=5&filter_dc=1 -> exclude datacenter IPs (default on)
  GET /api/viewers/peak?minutes=60     -> peak viewers within window (default: all history)
"""
import json
import os
import re
import time
import threading
import datetime
import calendar
import urllib.request
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from collections import Counter

LOG = os.environ.get("VIEWER_LOG", "/data/logs/proxy-host-4_viewers.log")
CACHE = os.environ.get("GEO_CACHE", "/cache/geo.tsv")
HISTORY = os.environ.get("HISTORY", "/cache/history.jsonl")
SAMPLE_INTERVAL = 60  # seconds between history samples
DEFAULT_MINUTES = 5
MAX_MINUTES = 60

_TS = re.compile(r"^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})")
_MONTHS = {m: i for i, m in enumerate(calendar.month_abbr) if m}
_CLIENT = re.compile(r"client=([^ ]+)")
_STREAM = re.compile(r"/(app/[^/]+)/")

# Known datacenter / hosting / cloud ASNs (inflate viewer counts).
DC_ASNS = {
    24940, 213239,  # Hetzner
    212238, 60068,  # Datacamp / CDN77
    15169,          # Google
    16509, 14618,   # Amazon AWS
    8075,           # Microsoft Azure
    14061,          # DigitalOcean
    16276,          # OVH
    20473,          # Vultr / Choopa
    63949,          # Linode / Akamai
    51167,          # Contabo
    13335,          # Cloudflare
    31898,          # Oracle Cloud
    45102,          # Alibaba Cloud
    54113,          # Fastly
    20940,          # Akamai
}
DC_KEYWORDS = (
    "hetzner", "datacamp", "digitalocean", "vultr", "linode", "contabo",
    "ovh", "scaleway", "upcloud", "choopa", "amazon", "amazonaws",
    "microsoft azure", "google cloud", "oracle cloud", "alibaba",
    "cloudflare", "fastly", "akamai", "incapsula", "imperva",
    "datacenter", "data center", "hosting", "dedicated server", "vps",
)


def is_datacenter(asn, org):
    if asn:
        try:
            if int(asn) in DC_ASNS:
                return True
        except (TypeError, ValueError):
            pass
    if org:
        o = org.lower()
        if any(k in o for k in DC_KEYWORDS):
            return True
    return False


_hist_lock = threading.Lock()


def parse_ts(s):
    m = _TS.match(s)
    if not m:
        return None
    try:
        dt = datetime.datetime(
            int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1)),
            int(m.group(4)), int(m.group(5)), int(m.group(6)),
            tzinfo=datetime.timezone.utc,
        ) - datetime.timedelta(hours=3)
        return dt
    except Exception:
        return None


def _bucket_ts(dt):
    return int(dt.replace(second=0, microsecond=0).timestamp())


def read_window(minutes):
    now = datetime.datetime.now(datetime.timezone.utc)
    cutoff = now - datetime.timedelta(minutes=minutes)
    rows = []
    try:
        size = os.path.getsize(LOG)
        with open(LOG, "rb") as f:
            if size > 5_000_000:
                f.seek(size - 5_000_000)
                f.readline()
            for line in f:
                line = line.decode("utf-8", "replace").rstrip("\n")
                ts = parse_ts(line)
                if ts is None:
                    continue
                if ts < cutoff:
                    continue
                rows.append(line)
    except FileNotFoundError:
        return rows
    return rows


def geo_lookup(ip):
    try:
        if ":" in ip:
            url = "https://ipwho.is/" + urllib.parse.quote(ip)
            with urllib.request.urlopen(url, timeout=4) as r:
                data = json.loads(r.read().decode())
            cc = data.get("country_code", "")
            cn = data.get("country", "")
            isp = data.get("connection", {}).get("isp", "")
            org = data.get("connection", {}).get("org", "") or isp
            asn = data.get("connection", {}).get("asn", "")
            return cc, cn, isp, asn, org
        url = "http://ip-api.com/json/" + urllib.parse.quote(ip) + "?fields=query,countryCode,country,isp,org,as,hosting,proxy"
        with urllib.request.urlopen(url, timeout=4) as r:
            data = json.loads(r.read().decode())
        cc = data.get("countryCode", "")
        cn = data.get("country", "")
        isp = data.get("isp", "")
        org = data.get("org", "") or isp
        asn = data.get("as", "")
        asn_num = None
        m = re.match(r"AS(\d+)", str(asn))
        if m:
            asn_num = int(m.group(1))
        return cc, cn, isp, asn_num, org
    except Exception:
        return "", "", "", "", ""


def cached_geo(ip):
    try:
        with open(CACHE, "r") as f:
            for ln in f:
                if ln.startswith(ip + "\t"):
                    parts = ln.rstrip("\n").split("\t")
                    if len(parts) >= 4:
                        asn = parts[4] if len(parts) > 4 else ""
                        try:
                            asn = int(asn)
                        except ValueError:
                            asn = ""
                        org = parts[5] if len(parts) > 5 else ""
                        return parts[1], parts[2], parts[3], asn, org
    except FileNotFoundError:
        pass
    return None


def store_geo(ip, cc, cn, isp, asn, org):
    try:
        with open(CACHE, "a") as f:
            f.write(f"{ip}\t{cc}\t{cn}\t{isp}\t{asn}\t{org}\n")
    except Exception:
        pass


def geo_for(ip):
    """Return geo tuple for ip, using cache or live lookup (and caching)."""
    g = cached_geo(ip)
    if g is None:
        g = geo_lookup(ip)
        store_geo(ip, g[0], g[1], g[2], g[3], g[4])
        time.sleep(0.2)
        g = cached_geo(ip) or g
    return g


def extract_ips(rows):
    ips = []
    for l in rows:
        cm = _CLIENT.search(l)
        if not cm:
            continue
        ip = cm.group(1)
        if ip == "-":
            continue
        ips.append(ip)
    return ips


def filter_dc_ips(ip_list):
    """Return (kept, excluded) split of ips by datacenter classification."""
    kept, excluded = [], []
    for ip in ip_list:
        g = geo_for(ip)
        if g and is_datacenter(g[3], g[4]):
            excluded.append(ip)
        else:
            kept.append(ip)
    return kept, excluded


def build_stats(minutes, with_countries, filter_dc):
    rows = read_window(minutes)
    ip_set = sorted(set(extract_ips(rows)))
    result = {
        "viewers": len(ip_set),
        "ipv4": sum(1 for i in ip_set if ":" not in i),
        "ipv6": sum(1 for i in ip_set if ":" in i),
        "window_minutes": minutes,
        "streams": {},
    }
    streams = {}
    for l in rows:
        cm = _CLIENT.search(l)
        sm = _STREAM.search(l)
        if not cm or not sm:
            continue
        ip = cm.group(1)
        if ip == "-":
            continue
        streams.setdefault(sm.group(1), set()).add(ip)
    result["streams"] = {k: len(v) for k, v in sorted(streams.items())}

    dc_ips = set()
    if filter_dc:
        ip_set, dc_ips = filter_dc_ips(ip_set)
        result["excluded_datacenters"] = sorted(dc_ips)
        result["excluded_datacenters_count"] = len(dc_ips)
        result["viewers"] = len(ip_set)
        result["ipv4"] = sum(1 for i in ip_set if ":" not in i)
        result["ipv6"] = sum(1 for i in ip_set if ":" in i)
        result["streams"] = {
            k: len(v - set(dc_ips)) for k, v in sorted(streams.items())
        }

    if with_countries and ip_set:
        countries = Counter()
        isps = Counter()
        for ip in ip_set:
            g = geo_for(ip)
            if g and g[0]:
                countries[(g[0], g[1])] += 1
                isps[(g[0], g[2])] += 1
        result["countries"] = [{"code": c, "name": n, "viewers": v} for (c, n), v in countries.most_common()]
        result["isps"] = [{"code": c, "isp": n, "viewers": v} for (c, n), v in isps.most_common()]
    return result


def backfill_history():
    """Scan the log once, bucket distinct (non-datacenter) viewers per minute."""
    buckets = {}
    try:
        size = os.path.getsize(LOG)
        with open(LOG, "rb") as f:
            if size > 50_000_000:
                f.seek(size - 50_000_000)
                f.readline()
            for line in f:
                line = line.decode("utf-8", "replace").rstrip("\n")
                ts = parse_ts(line)
                if ts is None:
                    continue
                cm = _CLIENT.search(line)
                if not cm:
                    continue
                ip = cm.group(1)
                if ip == "-":
                    continue
                buckets.setdefault(_bucket_ts(ts), set()).add(ip)
    except FileNotFoundError:
        return
    with _hist_lock:
        with open(HISTORY, "w") as f:
            for b in sorted(buckets):
                f.write(f"{b}\t{len(buckets[b])}\n")


def sample_loop():
    while True:
        time.sleep(SAMPLE_INTERVAL)
        try:
            rows = read_window(DEFAULT_MINUTES)
            ips = set(extract_ips(rows))
            ips, _ = filter_dc_ips(list(ips))
            b = _bucket_ts(datetime.datetime.now(datetime.timezone.utc))
            with _hist_lock:
                with open(HISTORY, "a") as f:
                    f.write(f"{b}\t{len(ips)}\n")
        except Exception:
            pass


def read_history(minutes=None):
    """Return list of (epoch, viewers) samples, optionally within last N minutes."""
    samples = []
    try:
        with open(HISTORY, "r") as f:
            for ln in f:
                parts = ln.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue
                try:
                    ts = int(parts[0])
                    v = int(parts[1])
                except ValueError:
                    continue
                if minutes is not None:
                    now = datetime.datetime.now(datetime.timezone.utc)
                    cutoff = int(now.timestamp()) - minutes * 60
                    if ts < cutoff:
                        continue
                samples.append((ts, v))
    except FileNotFoundError:
        pass
    return samples


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path.rstrip("/")
        if path not in ("/api/viewers", "/api/viewers/peak", "/health"):
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error":"not found"}')
            return

        if path == "/health":
            self._json({"status": "ok"})
            return

        qs = urllib.parse.parse_qs(parsed.query)

        if path == "/api/viewers/peak":
            try:
                minutes = int(qs.get("minutes", [0])[0])
            except ValueError:
                minutes = 0
            minutes = minutes if minutes > 0 else None
            samples = read_history(minutes)
            if samples:
                peak_ts, peak_v = max(samples, key=lambda s: s[1])
                peak = {
                    "peak_viewers": peak_v,
                    "peak_time": datetime.datetime.fromtimestamp(peak_ts, datetime.timezone.utc).isoformat(),
                    "window_minutes": minutes,
                    "samples": len(samples),
                }
            else:
                peak = {"peak_viewers": 0, "peak_time": None, "window_minutes": minutes, "samples": 0}
            try:
                current = list(set(extract_ips(read_window(DEFAULT_MINUTES))))
                current, _ = filter_dc_ips(current)
                peak["current_viewers"] = len(current)
            except Exception:
                pass
            self._json(peak)
            return

        try:
            minutes = min(int(qs.get("minutes", [DEFAULT_MINUTES])[0]), MAX_MINUTES)
        except ValueError:
            minutes = DEFAULT_MINUTES
        with_countries = qs.get("countries", ["0"])[0].lower() in ("1", "true", "yes")
        filter_dc = qs.get("filter_dc", ["1"])[0].lower() not in ("0", "false", "no")
        try:
            stats = build_stats(minutes, with_countries, filter_dc)
            self._json(stats)
        except Exception as e:
            self._json({"error": str(e)}, 500)

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8099"))
    print("backfilling history from log...")
    backfill_history()
    t = threading.Thread(target=sample_loop, daemon=True)
    t.start()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"viewers API listening on :{port}")
    server.serve_forever()
