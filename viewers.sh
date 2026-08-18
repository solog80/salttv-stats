#!/bin/bash
MINUTES="${1:-1}"
SHOW_COUNTRY="${2:-}"
LOG=/data/logs/proxy-host-4_viewers.log
CACHE=/tmp/viewer_geo_cache.tsv

docker exec npm-app-1 sh -c "tail -4000 $LOG" > /tmp/vw.log 2>/dev/null

python3 /home/customer/vw_filter.py "$MINUTES" < /tmp/vw.log > /tmp/vw_window.log

grep -oE "client=[^ ]+" /tmp/vw_window.log | cut -d= -f2 | grep -vE "^-$" | sort -u > /tmp/vw_ips.txt
TOTAL=$(wc -l < /tmp/vw_ips.txt | tr -d " ")
echo "=== Live LLHLS Viewers (last ${MINUTES} min) ==="
echo "Distinct viewer IPs: $TOTAL"
V4=$(grep -vcE ":" /tmp/vw_ips.txt || true)
V6=$(grep -cE ":" /tmp/vw_ips.txt || true)
echo "  IPv4: $V4  IPv6: $V6"

if [ "$SHOW_COUNTRY" = "--countries" ] && [ "$TOTAL" -gt 0 ]; then
  : > /tmp/vw_geo.tsv
  while read -r ip; do
    [ -z "$ip" ] && continue
    cached=$(grep -F -m1 "$ip" "$CACHE" 2>/dev/null | cut -f2-)
    if [ -n "$cached" ]; then
      echo -e "$ip\t$cached" >> /tmp/vw_geo.tsv
      continue
    fi
    if echo "$ip" | grep -q ":"; then
      geo=$(curl -s -m 4 "https://ipwho.is/$ip" 2>/dev/null)
      cc=$(echo "$geo" | grep -oE "\"country_code\":\"[^\"]*\"" | head -1 | cut -d"\"" -f4)
      cname=$(echo "$geo" | grep -oE "\"country\":\"[^\"]*\"" | head -1 | cut -d"\"" -f4)
      isp=$(echo "$geo" | grep -oE "\"isp\":\"[^\"]*\"" | head -1 | cut -d"\"" -f4)
    else
      geo=$(curl -s -m 4 "http://ip-api.com/json/$ip?fields=query,countryCode,country,isp" 2>/dev/null)
      cc=$(echo "$geo" | grep -oE "\"countryCode\":\"[^\"]*\"" | cut -d"\"" -f4)
      cname=$(echo "$geo" | grep -oE "\"country\":\"[^\"]*\"" | cut -d"\"" -f4)
      isp=$(echo "$geo" | grep -oE "\"isp\":\"[^\"]*\"" | cut -d"\"" -f4)
    fi
    echo -e "$ip\t$cc\t$cname\t$isp" >> /tmp/vw_geo.tsv
    echo -e "$ip\t$cc\t$cname\t$isp" >> "$CACHE"
    sleep 0.3
  done < /tmp/vw_ips.txt
  python3 /home/customer/vw_geo.py /tmp/vw_geo.tsv
fi
