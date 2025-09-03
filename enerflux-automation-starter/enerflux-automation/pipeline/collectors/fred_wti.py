#!/usr/bin/env python3
import os, io, datetime as dt, pandas as pd, requests

FRED_SERIES = "DCOILWTICO"  # WTI spot price
CSV_URL = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={FRED_SERIES}"

def fetch_wti():
    r = requests.get(CSV_URL, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.lower() for c in df.columns]
    df['date'] = pd.to_datetime(df['date'])
    df.rename(columns={FRED_SERIES.lower(): 'wti_usd_bbl'}, inplace=True)
    df = df.dropna()
    return df

if __name__ == "__main__":
    df = fetch_wti()
    out = "pipeline/outputs/WTI_DAILY_latest.csv"
    df.to_csv(out, index=False)
    print("saved:", out, len(df))
