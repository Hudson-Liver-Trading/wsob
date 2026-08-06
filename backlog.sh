#!/usr/bin/env bash
# One-shot catch-up: merge the ob2 backlog (2026-02-27 -> yesterday) oldest-first,
# one day per invocation so a crash resumes at the next day rather than restarting.
# Measured ~13 s per hour-group with 32-way parallel downloads (~2 days total).
set -uo pipefail
cd /home/ubuntu/wsob
START=${1:-20260227}
END=$(date -u -d "yesterday" +%Y%m%d)
d=$START
while [ "$d" -le "$END" ]; do
  echo "===== backlog day $d ====="
  ./.venv/bin/python aws_data_merge.py --since "$d" --until "$d" || echo "  (day $d had errors, continuing)"
  d=$(date -u -d "$d + 1 day" +%Y%m%d)
done
echo "BACKLOG COMPLETE through $END"
