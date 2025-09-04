#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from pipeline.utils.checks import write_sha256

OUT = Path("pipeline/outputs")
OUT.mkdir(parents=True, exist_ok=True)

def load_series(path, label):
    if not Path(path).exists():
        return pd.DataFrame(columns=["date", label])
    df = pd.read_csv(path, parse_dates=["date"])
    # détecte la colonne prix
    price_col = None
    for c in ("price","wti_usd_bbl","brent_usd_bbl"):
        if c in df.columns: price_col = c; break
    if not price_col:
        # fallback: 2e colonne
        price_col = [c for c in df.columns if c != "date"][0]
    df = df[["date", price_col]].rename(columns={price_col: label})
    return df

def build_weekly():
    # dailies
    wti  = load_series("pipeline/outputs/WTI_DAILY_latest.csv",      "WTI_USD_bbl")
    brnt = load_series("pipeline/outputs/BRENT_DAILY_latest.csv",    "Brent_USD_bbl")
    ng   = load_series("pipeline/outputs/NG_FUT1_DAILY_latest.csv",  "NG_USD_MMBtu")
    eua  = load_series("pipeline/outputs/EUA_FUT1_DAILY_latest.csv", "EUA_EUR_tCO2")

    # resample weekly mean
    def weekly(df): 
        return (df.set_index("date").resample("W").mean().reset_index() 
                if not df.empty else df)

    wti_w, brnt_w, ng_w, eua_w = map(weekly, (wti, brnt, ng, eua))

    # merge outer on date
    df = wti_w.merge(brnt_w, on="date", how="outer")\
              .merge(ng_w, on="date", how="outer")\
              .merge(eua_w, on="date", how="outer")\
              .sort_values("date")
    if df.empty:
        raise RuntimeError("No weekly data to build.")

    df["week"] = df["date"].dt.strftime("%G-W%V")
    week = df["week"].iloc[-1]

    csv_out = OUT / f"ENERGY_WEEKLY_{week}.csv"
    pdf_out = OUT / f"ENERGY_WEEKLY_{week}.pdf"

    df.to_csv(csv_out, index=False)

    # simple PDF table (last 12 obs)
    last = df.tail(12).copy()
    last["date"] = last["date"].dt.strftime("%Y-%m-%d")
    fig = plt.figure(figsize=(11,6)); plt.axis("off")
    tbl = plt.table(cellText=last.round(3).values,
                    colLabels=last.columns,
                    loc="center")
    tbl.scale(1, 1.2)
    plt.savefig(pdf_out, format="pdf", bbox_inches="tight")
    plt.close(fig)

    # checksums
    csv_sha = write_sha256(csv_out)
    pdf_sha = write_sha256(pdf_out)

    # optional push to R2
    if os.environ.get("R2_ACCOUNT_ID"):
        from r2_push import push
        push(csv_out.as_posix(), "ENERGY-WEEKLY", f"{week}.csv")
        push(pdf_out.as_posix(), "ENERGY-WEEKLY", f"{week}.pdf")
        push(csv_sha, "ENERGY-WEEKLY", f"{week}.csv.sha256")
        push(pdf_sha, "ENERGY-WEEKLY", f"{week}.pdf.sha256")

    print("built:", csv_out, pdf_out)
    return week

if __name__ == "__main__":
    build_weekly()
