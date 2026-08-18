#!/usr/bin/env python3
import sys
from collections import Counter

DC_ASNS = {
    24940, 213239, 212238, 60068, 15169, 16509, 14618, 8075,
    14061, 16276, 20473, 63949, 51167, 13335, 31898, 45102,
    54113, 20940,
}
DC_KEYWORDS = (
    "hetzner", "datacamp", "digitalocean", "vultr", "linode", "contabo",
    "ovh", "scaleway", "upcloud", "choopa", "amazon", "amazonaws",
    "microsoft azure", "google cloud", "oracle cloud", "alibaba",
    "cloudflare", "fastly", "akamai", "incapsula", "imperva",
    "datacenter", "data center", "hosting", "dedicated server", "vps",
)

countries = Counter()
isps = Counter()
excluded = 0
for ln in open(sys.argv[1]):
    parts = ln.rstrip('\n').split('\t')
    if len(parts) < 4:
        continue
    ip, cc, cn, isp = parts[0], parts[1], parts[2], parts[3]
    asn = parts[4] if len(parts) > 4 else ""
    org = parts[5] if len(parts) > 5 else ""
    is_dc = False
    try:
        if int(asn) in DC_ASNS:
            is_dc = True
    except (TypeError, ValueError):
        pass
    if not is_dc and org:
        o = org.lower()
        if any(k in o for k in DC_KEYWORDS):
            is_dc = True
    if is_dc:
        excluded += 1
        continue
    countries[(cc, cn)] += 1
    isps[(cc, isp)] += 1

print("Country breakdown (datacenter IPs excluded):")
for (cc, cn), n in countries.most_common():
    print(f"{n:3d}  {cc:3s}  {cn}")
print()
print("ISP detail:")
for (cc, isp), n in isps.most_common():
    print(f"{n:3d}  {cc:3s}  {isp}")
if excluded:
    print()
    print(f"Excluded datacenter IPs: {excluded}")
