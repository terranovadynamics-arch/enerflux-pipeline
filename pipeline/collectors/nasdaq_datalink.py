#!/usr/bin/env python3
# Nasdaq Data Link (ex-Quandl) – CHRIS continuous futures (front month)
# Écrit des CSV standardisés dans pipeline/outputs/ avec colonnes: date, price
import os, io, time
from pathlib import Path
import requests, pandas as pd

OUTDIR = Path("pipeline/outputs")
OUTDIR.mkdir(parents=True, exist_ok=True)

API_KEY = os.getenv("NDL_API_KEY", "").strip()
if not API_KEY:
    print("[NDL] No NDL_API_KEY in env – skipping gracefully.")
    raise SystemExit(0)

DATASETS = [
    ("WTI_FUT1_DAILY", "CHRIS/CME_CL1"),   # WTI front month (NYMEX)
    ("NG_FUT1_DAILY",  "CHRIS/CME_NG1"),   # Henry Hub front (NYMEX)
    ("EUA_FUT1_DAILY", "CHRIS/ICE_C1"),    # EUA front (ICE)
]

def fetch_csv(code: str) -> pd.DataFrame:
    url = f"https://data.nasdaq.com/api/v3/datasets/{code}.csv?api_key={API_KEY}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    # normalise
    df.columns = [c.strip().lower() for c in df.columns]
    if "date" not in df.columns:
        # parfois 'observation_date'
        if "observation_date" in df.columns:
            df.rename(columns={"observation_date": "date"}, inplace=True)
        else:
            raise RuntimeError(f"{code}: no date column; got {df.columns}")
    # prix: settle > last > premier non-date
    price_col = None
    for cand in ("settle", "last", "value", "price"):
        if cand in df.columns:
            price_col = cand; break
    if not price_col:
        candidates = [c for c in df.columns if c != "date"]
        if not candidates:
            raise RuntimeError(f"{code}: no price-like column")
        price_col = candidates[0]
    df = df[["date", price_col]].copy()
    df.rename(columns={price_col: "price"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "price"]).sort_values("date")
    return df

def main():
    for slug, code in DATASETS:
        try:
            df = fetch_csv(code)
            out = OUTDIR / f"{slug}_latest.csv"
            df.to_csv(out, index=False)
            print(f"[NDL] Wrote {out} rows={len(df)}")
            time.sleep(0.8)
        except Exception as e:
            print(f"[NDL][WARN] {code}: {e}")

if __name__ == "__main__":
    main()
