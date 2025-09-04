#!/usr/bin/env python3
import os, sys, subprocess, datetime as dt
from pathlib import Path

# --- ensure repo root on sys.path for "from pipeline..." imports ---
REPO_ROOT = Path(__file__).resolve().parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

def sh(cmd):
    print("$", " ".join(cmd)); subprocess.check_call(cmd)

def ensure_outputs():
    Path("pipeline/outputs").mkdir(parents=True, exist_ok=True)

def write_hashes(*files):
    from pipeline.utils.checks import write_sha256
    for f in files:
        if Path(f).exists():
            sha = write_sha256(f)
            print("sha256:", sha)

def maybe_push(file_path: str, sku: str, dated_ext: str):
    # push to R2 only if secrets are set
    if os.environ.get("R2_ACCOUNT_ID"):
        from r2_push import push
        stamp = dt.datetime.utcnow().strftime("%Y-%m-%d")
        push(file_path, sku, f"{stamp}{dated_ext}")

def run_daily_collectors():
    # Open sources (FRED)
    if Path("pipeline/collectors/fred_wti.py").exists():
        sh([sys.executable, "pipeline/collectors/fred_wti.py"])
    if Path("pipeline/collectors/fred_brent.py").exists():
        sh([sys.executable, "pipeline/collectors/fred_brent.py"])
    # Paid (NDL) – harmless if no API key
    sh([sys.executable, "pipeline/collectors/nasdaq_datalink.py"])

def main():
    ensure_outputs()
    job = sys.argv[1] if len(sys.argv) > 1 else "daily"
    print("Running job:", job)

    if job == "daily":
        run_daily_collectors()

        # Hash + optional push for daily datasets we expect
        candidates = [
            "pipeline/outputs/WTI_DAILY_latest.csv",       # FRED WTI spot
            "pipeline/outputs/BRENT_DAILY_latest.csv",     # FRED Brent spot
            "pipeline/outputs/WTI_FUT1_DAILY_latest.csv",  # NDL futures
            "pipeline/outputs/NG_FUT1_DAILY_latest.csv",
            "pipeline/outputs/EUA_FUT1_DAILY_latest.csv",
        ]
        write_hashes(*candidates)

        # optional R2 push (use your existing SKUs)
        for f in candidates:
            if Path(f).exists():
                base = Path(f).name.replace("_latest", "")
                sku = base.split("_latest")[0].replace(".csv", "").upper()
                maybe_push(f, sku, ".csv")
                sha = f + ".sha256"
                if Path(sha).exists():
                    maybe_push(sha, sku, ".csv.sha256")

    elif job == "weekly":
        # make sure dailies exist; if not, generate them
        needed = [
            "pipeline/outputs/WTI_DAILY_latest.csv",
            "pipeline/outputs/BRENT_DAILY_latest.csv",
            "pipeline/outputs/NG_FUT1_DAILY_latest.csv",
            "pipeline/outputs/EUA_FUT1_DAILY_latest.csv",
        ]
        if not all(Path(p).exists() for p in needed):
            print("[weekly] missing dailies -> running daily collectors first")
            run_daily_collectors()

        sh([sys.executable, "pipeline/models/build_weekly.py"])

    else:
        raise SystemExit("Unknown job. Use: daily | weekly")

if __name__ == "__main__":
    main()
