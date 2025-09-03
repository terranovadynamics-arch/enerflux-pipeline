#!/usr/bin/env python3
import io, os
import pandas as pd, requests

FRED_SERIES = "DCOILWTICO"  # WTI spot price
CSV_URL = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={FRED_SERIES}"

def fetch_wti():
    # User-Agent pour éviter une page HTML sur certains runners
    r = requests.get(CSV_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    # normaliser les colonnes
    df.columns = [c.strip().lower() for c in df.columns]

    # Accepter 'date' OU 'observation_date'
    date_col = None
    for candidate in ("date", "observation_date"):
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        raise RuntimeError(f"Unexpected CSV header from FRED: {df.columns.tolist()}")

    # valeur = nom de série (dcoilwtico) ou dernière colonne
    value_col = "dcoilwtico" if "dcoilwtico" in df.columns else [c for c in df.columns if c != date_col][0]

    df.rename(columns={date_col: "date", value_col: "wti_usd_bbl"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "wti_usd_bbl"])
    return df

if __name__ == "__main__":
    df = fetch_wti()
    out = "pipeline/outputs/WTI_DAILY_latest.csv"
    os.makedirs(os.path.dirname(out), exist_ok=True)  # <-- crée le dossier
    df.to_csv(out, index=False)
    print("saved:", out, len(df))
