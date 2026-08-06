# wsob deployment (Tokyo box i-050010d88d2ee615e, ap-northeast-1)

Installed 2026-08-07. Before this, the scraper was a bare `python main.py`
started by hand (162 days uptime, would NOT survive a reboot) and the hourly
merge had never been automated at all — last run manually 2026-02-27, leaving
a 5-month backlog of ~500k tiny ob2 objects.

    sudo cp deploy/systemd/* /etc/systemd/system/ && sudo systemctl daemon-reload
    sudo systemctl enable --now wsob-scraper.service   # scraper, Restart=always
    sudo systemctl enable --now wsob-merge.timer       # hourly at :10
    sudo systemctl start wsob-backlog.service          # one-shot catch-up

`aws_data_merge.py` fixes, all found live:
  * never merges/deletes the IN-PROGRESS hour (was a real data-loss race)
  * verifies the uploaded hourly object before deleting sources
  * lists by narrow per-day prefix (whole-prefix listing took >15 min)
  * 32-way parallel downloads (3.5 -> ~55 files/s; backlog 1 month -> ~2 days)
  * --since/--until bounds; scheduled runs default to a 2-day window

## CPU contention (learned the hard way, 2026-08-06)

The box is a 1-vCPU t2.micro shared with the live scraper. A 32-worker merge
pinned CPU at 100%, starved the scraper off the CPU, and stopped capture for
13 minutes — losing orderbook data that cannot be backfilled from any source.

Guards now in place:
  * DOWNLOAD_WORKERS = 6 (not 32)
  * drop-ins in deploy/systemd/dropins/ give the scraper Nice=-5/CPUWeight=200
    and the merge/backlog Nice=10-19/CPUWeight=20-50, so capture always wins.

    sudo mkdir -p /etc/systemd/system/wsob-{scraper,merge,backlog}.service.d
    sudo cp deploy/systemd/dropins/wsob-scraper.priority.conf /etc/systemd/system/wsob-scraper.service.d/
    sudo cp deploy/systemd/dropins/wsob-backlog.nice.conf     /etc/systemd/system/wsob-backlog.service.d/
    sudo cp deploy/systemd/dropins/wsob-merge.nice.conf       /etc/systemd/system/wsob-merge.service.d/
    sudo systemctl daemon-reload

Verify after any change to the merge: watch that ob2 objects keep appearing
every ~5s while the merge runs.
