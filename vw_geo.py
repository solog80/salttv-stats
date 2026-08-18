#!/usr/bin/env python3
import sys
from collections import Counter

countries = Counter()
isps = Counter()
for ln in open(sys.argv[1]):
    parts = ln.rstrip('\n').split('\t')
    if len(parts) < 4:
        continue
    ip, cc, cn, isp = parts[0], parts[1], parts[2], parts[3]
    countries[(cc, cn)] += 1
    isps[(cc, isp)] += 1

print("Country breakdown:")
for (cc, cn), n in countries.most_common():
    print(f"{n:3d}  {cc:3s}  {cn}")
print()
print("ISP detail:")
for (cc, isp), n in isps.most_common():
    print(f"{n:3d}  {cc:3s}  {isp}")