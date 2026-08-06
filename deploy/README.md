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
