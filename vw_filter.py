#!/usr/bin/env python3
import sys, re, datetime, calendar
mins = int(sys.argv[1])
now = datetime.datetime.now(datetime.timezone.utc)
cutoff = now - datetime.timedelta(minutes=mins)
pat = re.compile(r'^(\d{2})/([A-Za-z]{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2})')
months = {m: i for i, m in enumerate(calendar.month_abbr) if m}
for ln in sys.stdin:
    m = pat.match(ln)
    if not m:
        continue
    try:
        ts = datetime.datetime(int(m.group(3)), months[m.group(2)], int(m.group(1)),
                               int(m.group(4)), int(m.group(5)), int(m.group(6)),
                               tzinfo=datetime.timezone.utc) - datetime.timedelta(hours=3)
    except Exception:
        continue
    if ts >= cutoff:
        sys.stdout.write(ln)