#!/bin/bash
# TEMPORARY: samples host and container memory every few seconds to a CSV in
# logs/, for sizing Spark memory. Run on the prod host from the repo root,
# ideally in tmux so it survives logout:
#
#   ./sample-memory.sh [hours] [interval_seconds]
#
# Host memory values are in kB, read from /proc/meminfo. Swap columns are
# cumulative page counts and memory_full_stall_us is cumulative microseconds
# since boot, so diff consecutive rows to get rates. Container memory is
# recorded as reported by `docker stats` (e.g. "12.3GiB"). Gaps between
# timestamps longer than the interval indicate the host itself stalled.
set -euo pipefail

HOURS="${1:-16}"
INTERVAL="${2:-5}"
CONTAINERS=(spark-node-master-prod spark-node-worker-prod)
OUT="logs/memory-samples-$(date -u +%Y-%m-%dT%H-%M-%SZ).csv"

meminfo() { awk -v k="$1:" '$1 == k {print $2}' /proc/meminfo; }
vmstat() { awk -v k="$1" '$1 == k {print $2}' /proc/vmstat; }

echo "timestamp_utc,mem_free_kb,mem_available_kb,anon_kb,cached_kb,slab_kb,swap_free_kb,pswpin,pswpout,memory_full_stall_us,master_mem,worker_mem" \
    > "$OUT"
echo "Writing samples to $OUT every ${INTERVAL}s for ${HOURS}h"

end=$(( $(date +%s) + HOURS * 3600 ))
while [ "$(date +%s)" -lt "$end" ]; do
    # Bound docker stats so a hung daemon doesn't stall sampling. Missing
    # containers (e.g. stopped) are recorded as blank
    stats="$(timeout 10 docker stats --no-stream \
        --format '{{.Name}} {{.MemUsage}}' "${CONTAINERS[@]}" 2>/dev/null || true)"
    master_mem="$(awk -v c="${CONTAINERS[0]}" '$1 == c {print $2}' <<< "$stats")"
    worker_mem="$(awk -v c="${CONTAINERS[1]}" '$1 == c {print $2}' <<< "$stats")"
    full_stall="$(awk '/^full/ {sub("total=", "", $5); print $5}' /proc/pressure/memory)"

    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ),$(meminfo MemFree),$(meminfo MemAvailable),$(meminfo AnonPages),$(meminfo Cached),$(meminfo Slab),$(meminfo SwapFree),$(vmstat pswpin),$(vmstat pswpout),${full_stall},${master_mem},${worker_mem}" \
        >> "$OUT"
    sleep "$INTERVAL"
done
