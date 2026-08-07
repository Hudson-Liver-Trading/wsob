#!/usr/bin/env bash
# One hourly cycle: keep the recent window merged, then chip away at the backlog.
#
# Replaces the previous long-running one-shot backlog service, which died twice
# (once to a reboot, once to a SIGTERM) and had to be babysat. There is no
# long-lived process here: each run is bounded, and anything interrupted is
# simply retried next hour. Merged hours have their ob2 sources deleted, so a
# repeated day is cheap -- the work is idempotent by construction.
set -uo pipefail
cd /home/ubuntu/wsob
PY=./.venv/bin/python
STATE=/home/ubuntu/wsob/.backlog_day          # next backlog day to attempt
BUDGET=${BACKLOG_BUDGET:-1800}                # seconds of backlog work per hour

# 1. recent window (cheap once caught up; this is the steady-state job)
timeout 600 "$PY" aws_data_merge.py || echo "recent-window merge hit its limit, will resume next hour"

# 2. backlog: one day per run, time-boxed so we never overrun the hour
[ -f "$STATE" ] || echo 20260227 > "$STATE"
DAY=$(cat "$STATE")
YDAY=$(date -u -d "yesterday" +%Y%m%d)
if [ "$DAY" -le "$YDAY" ]; then
  echo "=== backlog day $DAY (budget ${BUDGET}s) ==="
  if timeout "$BUDGET" "$PY" aws_data_merge.py --since "$DAY" --until "$DAY"; then
    date -u -d "$DAY + 1 day" +%Y%m%d > "$STATE"
    echo "backlog day $DAY complete -> next $(cat "$STATE")"
  else
    echo "backlog day $DAY incomplete (budget/interrupt) -- retrying same day next hour"
  fi
else
  echo "backlog caught up through $YDAY"
fi
