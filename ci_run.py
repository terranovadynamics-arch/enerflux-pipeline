#!/usr/bin/env python3
import os, sys, subprocess, datetime as dt
from pathlib import Path

def run(cmd):
    print("$", " ".join(cmd)); subprocess.check_call(cmd)

def ensure_outputs_dir():
    Path("pipeline/outputs").mkdir(parents=True, exist_ok=True)

def daily_collect_and_push():
    # 1) collecte quotidienne WTI -> pipeline/outputs/WTI_DAILY_latest.csv
    run([sys.executable, "pipeline/collectors/fred_wti.py"])
    # 2) push vers R2 si secrets présents
    if os.environ.get("R2_ACCOUNT_ID"):
        from r2_push import push
        dated = dt.datetime.utcnow().strftime("%Y-%m-%d") + ".csv"
        push("pipeline/outputs/WTI_DAILY_latest.csv", "WTI-DAILY", dated)

def main():
    ensure_outputs_dir()
    job = sys.argv[1] if len(sys.argv) > 1 else "daily"

    if job == "daily":
        daily_collect_and_push()

    elif job == "weekly":
        # Assure d'abord la présence du daily (nécessaire au weekly)
        daily_csv = Path("pipeline/outputs/WTI_DAILY_latest.csv")
        if not daily_csv.exists():
            print("[weekly] Daily CSV absent -> génération…")
            daily_collect_and_push()
            assert daily_csv.exists(), "[weekly] Daily CSV introuvable après génération."

        # Construit le pack hebdo (CSV + PDF) ; ce script pousse vers R2 si secrets
        run([sys.executable, "pipeline/models/build_weekly.py"])

    else:
        raise SystemExit("unknown job: daily|weekly")

if __name__ == "__main__":
    main()
