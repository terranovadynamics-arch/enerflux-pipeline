#!/usr/bin/env python3
# Nasdaq Data Link (ex-Quandl) collector – CHRIS continuous futures
# Saves cleaned CSVs in pipeline/outputs/
import os, sys, time
import pathlib
import requests
import pandas as pd
import io

OUTDIR = pathlib.Path("pipeline/outputs")
OUTDIR.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("NDL_API_KEY", "").strip()
if not API_KEY:
    print("[NDL] Pas de NDL_API_KEY -> skip.")
    sys.exit(0)

DATASETS = [
    ("WTI_DAILY", "CHRIS/CME_CL1"),
    ("NG_DAILY",  "CHRIS/CME_NG1"),
    ("EUA_DAILY", "CHRIS/ICE_C1"),
]

def fetch_csv(code: str) -> pd.DataFrame:
    url = f"https://data.nasdaq.com/api/v3/datasets/{code}.csv?api_key={API_KEY}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for {code}: {r.text[:180]}")
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.lower().strip() for c in df.columns]
    if "date" not in df.columns:
        raise ValueError(f"{code}: date column missing")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df

def save(df: pd.DataFrame, slug: str):
    out = OUTDIR / f"{slug}_latest.csv"
    df.to_csv(out, index=False)
    print(f"[NDL] Written {out} ({len(df)} rows)")

def main():
    any_ok = False
    for slug, code in DATASETS:
        try:
            df = fetch_csv(code)
            save(df, slug)
            any_ok = True
            time.sleep(0.8)
        except Exception as e:
            print(f"[NDL][WARN] {code}: {e}")
    if not any_ok:
        print("[NDL] No dataset retrieved (invalid key or no subscription).")
        sys.exit(0)

if __name__ == "__main__":
    main()
