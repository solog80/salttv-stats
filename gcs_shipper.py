#!/usr/bin/env python3
"""Ship NPM real-viewer log lines to GCS as gzipped NDJSON for BigQuery.

Parses each line into fields, batches, gzips, and uploads to
gs://<bucket>/viewer-logs/date=YYYYMMDD/hour=HH/part-<ts>-<n>.jsonl.gz

Idempotent across restarts: tracks byte offset (inode+offset) in a state file.
"""
import gzip
import io
import json
import os
import re
import time
import threading
import calendar
import datetime

import google.auth
from google.auth.transport.requests import Request
from google.cloud import storage

LOG = os.environ.get("LOG", "/data/logs/proxy-host-4_viewers.log")
BUCKET = os.environ.get("BUCKET", "salt-media-app1-viewer-logs")
STATE = os.environ.get("STATE", "/state/offset")
FLUSH_SECONDS = float(os.environ.get("FLUSH_SECONDS", "60"))
FLUSH_BYTES = int(os.environ.get("FLUSH_BYTES", "1048576"))
MAX_OLD_LINES = int(os.environ.get("MAX_OLD_LINES", "200000"))

_TS = re.compile(r"^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})")
_MONTHS = {m: i for i, m in enumerate(calendar.month_abbr) if m}
_STATUS = re.compile(r' (\d{3}) "')
_URI = re.compile(r' "([^"]*)" ')
_CLIENT = re.compile(r"client=(\S+)")
_UA = re.compile(r'ua="([^"]*)"')
_REF = re.compile(r'ref="([^"]*)"')
_SESSION = re.compile(r"[?&]session=([^&\s]+)")


def parse_ts(s):
    m = _TS.match(s)
    if not m:
        return None
    try:
        offset_h, offset_m = int(m.group(7)[1:3]), int(m.group(7)[3:5])
        sign = 1 if m.group(7)[0] == "+" else -1
        dt = datetime.datetime(
            int(m.group(3)), _MONTHS[m.group(2)], int(m.group(1)),
            int(m.group(4)), int(m.group(5)), int(m.group(6)),
            tzinfo=datetime.timezone.utc,
        ) - datetime.timedelta(hours=sign * offset_h, minutes=sign * offset_m)
        return dt
    except Exception:
        return None


def parse_line(line):
    ts = parse_ts(line)
    if ts is None:
        return None
    m = _STATUS.search(line)
    status = int(m.group(1)) if m else None
    mu = _URI.search(line)
    uri = mu.group(1) if mu else ""
    mc = _CLIENT.search(line)
    client = mc.group(1) if mc else ""
    # client may be a comma-separated XFF list; take the first (real viewer).
    client = client.split(",")[0].strip()
    ma = _UA.search(line)
    ua = ma.group(1) if ma else ""
    mr = _REF.search(line)
    ref = mr.group(1) if mr else ""
    ms = _SESSION.search(uri)
    session = ms.group(1) if ms else ""
    # stream name from /app/<stream>/...
    sm = re.search(r"/app/([^/]+)/", uri)
    stream = sm.group(1) if sm else ""
    path = uri.split("?", 1)[0]
    if "chunklist" in path:
        ftype = "chunklist"
    elif "init_" in path:
        ftype = "init"
    elif "part_" in path:
        ftype = "part"
    elif "seg_" in path:
        ftype = "seg"
    elif path.endswith(".m3u8"):
        ftype = "playlist"
    else:
        ftype = "other"
    return {
        "ts": ts.isoformat(),
        "status": status,
        "uri": uri,
        "stream": stream,
        "file_type": ftype,
        "session": session,
        "client_ip": client,
        "user_agent": ua,
        "referer": ref,
    }


def _hive(ts):
    return ts.strftime("%Y%m%d"), ts.strftime("%H")


class Shipper:
    def __init__(self):
        self.offset = 0
        self.inode = None
        self._client = None
        self.buf = io.StringIO()
        self.count = 0
        self.part = 0
        self.lock = threading.Lock()
        self._load_state()

    @property
    def gcs(self):
        if self._client is None:
            if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
                creds, _ = google.auth.default(
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    request=Request(),
                )
            else:
                creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            self._client = storage.Client(project="salt-media-app1", credentials=creds)
        return self._client

    def _load_state(self):
        try:
            with open(STATE) as f:
                line = f.read().strip().split()
                if len(line) == 2:
                    self.inode, self.offset = int(line[0]), int(line[1])
        except Exception:
            pass

    def _save_state(self):
        try:
            os.makedirs(os.path.dirname(STATE), exist_ok=True)
            tmp = STATE + ".tmp"
            with open(tmp, "w") as f:
                f.write(f"{self.inode} {self.offset}")
            os.replace(tmp, STATE)
        except Exception:
            pass

    def _read_new(self):
        """Return new lines since last offset. Resets if file rotated."""
        st = os.stat(LOG)
        cur_inode = st.st_ino
        cur_size = st.st_size
        if cur_inode != self.inode or self.inode is None:
            # New file (rotation). Start from the end to avoid re-shipping.
            self.inode = cur_inode
            self.offset = 0 if cur_size < 10_000_000 else max(0, cur_size - 1_000_000)
        if self.offset > cur_size:
            self.offset = 0
        lines = []
        with open(LOG, "rb") as f:
            f.seek(self.offset)
            data = f.read()
            self.offset = f.tell()
            for bl in data.split(b"\n"):
                if bl:
                    try:
                        lines.append(bl.decode("utf-8", "replace"))
                    except Exception:
                        pass
        self._save_state()
        return lines

    def _flush(self):
        with self.lock:
            payload = self.buf.getvalue()
            count = self.count
            self.buf = io.StringIO()
            self.count = 0
            part = self.part
            self.part += 1
        if not payload:
            return
        gz = io.BytesIO()
        with gzip.GzipFile(fileobj=gz, mode="wb", compresslevel=6) as g:
            g.write(payload.encode("utf-8"))
        now = datetime.datetime.now(datetime.timezone.utc)
        date, hour = _hive(now)
        name = f"viewer-logs/date={date}/hour={hour}/part-{int(time.time())}-{part}.jsonl.gz"
        blob = self.gcs.bucket(BUCKET).blob(name)
        blob.content_type = "application/json"
        blob.content_encoding = "gzip"
        blob.upload_from_string(gz.getvalue(), content_type="application/json")
        print(f"uploaded {name} ({count} lines, {gz.getbuffer().nbytes} bytes)", flush=True)

    def run(self):
        last_flush = time.time()
        total = 0
        while True:
            try:
                for line in self._read_new():
                    rec = parse_line(line)
                    if rec is None:
                        continue
                    total += 1
                    with self.lock:
                        self.buf.write(json.dumps(rec, separators=(",", ":")) + "\n")
                        self.count += 1
            except FileNotFoundError:
                pass
            if total > MAX_OLD_LINES:
                print(f"rate cap hit ({total} lines), dropping tail", flush=True)
                return
            now = time.time()
            size = self.buf.tell()
            if (now - last_flush >= FLUSH_SECONDS) or (size >= FLUSH_BYTES):
                self._flush()
                last_flush = now
            time.sleep(2)


if __name__ == "__main__":
    Shipper().run()
