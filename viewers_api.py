#!/usr/bin/env python3
"""Viewer stats API for the LLHLS edge.
Reads the NPM real-viewer log (X-Forwarded-For), computes distinct viewers
over a window, and returns JSON with optional country/ISP breakdown.

Endpoints:
  GET /health                          -> {"status": "ok"}
  GET /api/viewers?minutes=5           -> live viewers (+ streams, optional countries=1)
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
        url = "http://ip-api.com/json/" + urllib.parse.quote(ip) + "?fields=query,countryCode,country,isp"
        with urllib.request.urlopen(url, timeout=4) as r:
            data = json.loads(r.read().decode())
        return data.get("countryCode", ""), data.get("country", ""), data.get("isp", "")
    except Exception:
        return "", "", ""


def cached_geo(ip):
    try:
        with open(CACHE, "r") as f:
            for ln in f:
                if ln.startswith(ip + "\t"):
                    parts = ln.rstrip("\n").split("\t")
                    if len(parts) >= 4:
                        return parts[1], parts[2], parts[3]
    except FileNotFoundError:
        pass
    return None


def store_geo(ip, cc, cn, isp):
    try:
        with open(CACHE, "a") as f:
            f.write(f"{ip}\t{cc}\t{cn}\t{isp}\n")
    except Exception:
        pass


def build_stats(minutes, with_countries):
    rows = read_window(minutes)
    ip_set = sorted(set(i for i in (_CLIENT.search(l).group(1) for l in rows if _CLIENT.search(l)) if i and ":" not in i))
    result = {
        "viewers": len(ip_set),
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
        if ":" in ip:
            continue
        streams.setdefault(sm.group(1), set()).add(ip)
    result["streams"] = {k: len(v) for k, v in sorted(streams.items())}

    if with_countries and ip_set:
        countries = Counter()
        isps = Counter()
        for ip in ip_set:
            g = cached_geo(ip)
            if g is None:
                g = geo_lookup(ip)
                store_geo(ip, g[0], g[1], g[2])
                time.sleep(0.2)
            if g[0]:
                countries[(g[0], g[1])] += 1
                isps[(g[0], g[2])] += 1
        result["countries"] = [{"code": c, "name": n, "viewers": v} for (c, n), v in countries.most_common()]
        result["isps"] = [{"code": c, "isp": n, "viewers": v} for (c, n), v in isps.most_common()]
    return result


def backfill_history():
    """Scan the log once, bucket distinct IPv4 viewers per minute, write history."""
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
                if ":" in ip:
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
            ips = set(i for i in (_CLIENT.search(l).group(1) for l in rows if _CLIENT.search(l)) if i and ":" not in i)
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
                peak["current_viewers"] = len(
                    set(i for i in (_CLIENT.search(l).group(1) for l in read_window(DEFAULT_MINUTES) if _CLIENT.search(l)) if i and ":" not in i)
                )
            except Exception:
                pass
            self._json(peak)
            return

        try:
            minutes = min(int(qs.get("minutes", [DEFAULT_MINUTES])[0]), MAX_MINUTES)
        except ValueError:
            minutes = DEFAULT_MINUTES
        with_countries = qs.get("countries", ["0"])[0].lower() in ("1", "true", "yes")
        try:
            stats = build_stats(minutes, with_countries)
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