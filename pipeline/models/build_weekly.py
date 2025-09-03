#!/usr/bin/env python3
import os, sys, pandas as pd, matplotlib.pyplot as plt
from datetime import datetime

def build_weekly(wti_csv="pipeline/outputs/WTI_DAILY_latest.csv"):
    df = pd.read_csv(wti_csv, parse_dates=["date"]).dropna()
    df = df.set_index("date").resample("W").mean().reset_index()
    df["week"] = df["date"].dt.strftime("%G-W%V")
    week = df["week"].iloc[-1]
    csv_out = f"pipeline/outputs/ENERGY_WEEKLY_{week}.csv"
    df.tail(12).to_csv(csv_out, index=False)
    pdf_out = f"pipeline/outputs/ENERGY_WEEKLY_{week}.pdf"
    fig = plt.figure(figsize=(10,6)); plt.axis("off")
    tbl = plt.table(cellText=df.tail(12).round(2).values,
                    colLabels=df.columns, loc="center")
    tbl.scale(1, 1.3)
    plt.savefig(pdf_out, format="pdf", bbox_inches="tight"); plt.close(fig)
    return week, csv_out, pdf_out

if __name__ == "__main__":
    week, csv_out, pdf_out = build_weekly()
    print("built:", csv_out, pdf_out)
    # N'IMPORTER r2_push QUE SI on a les secrets (sinon pas de push)
    if os.environ.get("R2_ACCOUNT_ID"):
        from r2_push import push
        push(csv_out, "ENERGY-WEEKLY", f"{week}.csv")
        push(pdf_out, "ENERGY-WEEKLY", f"{week}.pdf")
        print("pushed to R2 for week:", week)
