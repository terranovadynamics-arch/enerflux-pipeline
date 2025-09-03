#!/usr/bin/env python3
import os, sys, subprocess, datetime as dt
from pathlib import Path

def run(cmd):
    print("$", " ".join(cmd)); subprocess.check_call(cmd)

def main():
    # ceinture et bretelles : crée le répertoire de sortie
    Path("pipeline/outputs").mkdir(parents=True, exist_ok=True)

    job = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if job == "daily":
        # 1) collecte quotidienne WTI
        run([sys.executable, "pipeline/collectors/fred_wti.py"])

        # 2) push vers R2 si secrets présents
        if os.environ.get("R2_ACCOUNT_ID"):
            from r2_push import push
            dated = dt.datetime.utcnow().strftime("%Y-%m-%d") + ".csv"
            push("pipeline/outputs/WTI_DAILY_latest.csv", "WTI-DAILY", dated)

    elif job == "weekly":
        # construit le pack hebdo (CSV + PDF) et pousse si secrets
        run([sys.executable, "pipeline/models/build_weekly.py"])

    else:
        raise SystemExit("unknown job: daily|weekly")

if __name__ == "__main__":
    main()
