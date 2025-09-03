#!/usr/bin/env python3
import os, sys, subprocess, datetime as dt
from r2_push import push

def run(cmd):
    print("$", " ".join(cmd)); subprocess.check_call(cmd)

def main():
    job = sys.argv[1] if len(sys.argv) > 1 else "daily"
    if job == "daily":
        run([sys.executable, "pipeline/collectors/fred_wti.py"])  # saves WTI_DAILY_latest.csv
        if os.environ.get("R2_ACCOUNT_ID"):
            dated = dt.datetime.utcnow().strftime("%Y-%m-%d") + ".csv"
            push("pipeline/outputs/WTI_DAILY_latest.csv", "WTI-DAILY", dated)
    elif job == "weekly":
        run([sys.executable, "pipeline/models/build_weekly.py"])  # handles R2 push
    else:
        raise SystemExit("unknown job: daily|weekly")

if __name__ == "__main__":
    main()
