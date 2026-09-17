from __future__ import annotations
from pathlib import Path
import argparse
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

def inject(df: pd.DataFrame, anomaly: str, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    out = df.copy()
    n = len(out)

    if anomaly == "A1":
        idx = rng.choice(out.index, size=max(1, int(n * 0.03)), replace=False)
        out.loc[idx, "user_id"] = None
    elif anomaly == "A2":
        idx = rng.choice(out.index, size=max(1, int(n * 0.03)), replace=False)
        out.loc[idx, "ms_played"] = (out.loc[idx, "duration_ms"] * 1.25).astype(int)
    elif anomaly == "A3":
        idx = rng.choice(out.index, size=max(1, int(n * 0.02)), replace=False)
        out.loc[idx, "event_type"] = "share"
    elif anomaly == "A4":
        k = max(1, int(n * 0.01))
        out = pd.concat([out, out.sample(k, random_state=seed)], ignore_index=True)
    elif anomaly == "A5":
        idx = rng.choice(out.index, size=max(1, int(n * 0.02)), replace=False)
        out.loc[idx, "song_id"] = "S99999"
    elif anomaly == "A6":
        out = out.drop(columns=["country"])
    elif anomaly == "A7":
        # A load delay: the batch is ten hours stale by the time it lands. Both
        # columns move, otherwise dbt source freshness (which reads _ingested_at)
        # can never see it.
        out["event_ts"] = pd.to_datetime(out["event_ts"]) - pd.Timedelta(hours=10)
        out["_ingested_at"] = pd.to_datetime(out["_ingested_at"]) - pd.Timedelta(hours=10)
    elif anomaly == "A8":
        br = out.index[out["country"] == "BR"]
        if len(br):
            drop = rng.choice(br, size=int(len(br) * 0.40), replace=False)
            out = out.drop(drop)
    elif anomaly == "A9":
        skips = out.index[out["event_type"] == "skip"]
        target_drop = int(len(skips) * 0.42)
        if target_drop:
            change = rng.choice(skips, size=target_drop, replace=False)
            out.loc[change, "event_type"] = "play"
            ratio = rng.uniform(0.75, 1.0, size=target_drop)
            out.loc[change, "ms_played"] = (out.loc[change, "duration_ms"].to_numpy() * ratio).astype(int)
    elif anomaly == "A10":
        out = pd.concat([out, out.copy()], ignore_index=True)
        out.loc[n:, "event_id"] = [f"dupvol-{i}" for i in range(len(out) - n)]
    elif anomaly == "A11":
        # Mobile telemetry is mislabelled, not lost. Deleting the rows would drop
        # ~57% of volume and any volume monitor would catch it; the point of A11
        # is an anomaly visible only in the distribution of a field.
        mobile = out.index[out["device_type"] == "mobile"]
        if len(mobile):
            out.loc[mobile, "device_type"] = rng.choice(
                ["desktop", "web", "smart_speaker"], size=len(mobile), p=[0.42, 0.48, 0.10]
            )
    elif anomaly == "A12":
        out["ms_played"] = np.minimum(out["duration_ms"], (out["ms_played"] * 1.25).astype(int))
    elif anomaly == "A13":
        counts = out["user_id"].value_counts()
        affected = counts.sample(frac=0.15, random_state=seed).index
        mask = out["user_id"].isin(affected)
        keep_idx = out[mask].groupby("user_id").head(1).index
        out = pd.concat([out[~mask], out.loc[keep_idx]], ignore_index=True)
    elif anomaly == "A14":
        out["duration_ms"] = out["duration_ms"].astype(str)
    else:
        raise ValueError(f"Unknown anomaly: {anomaly}")
    return out.reset_index(drop=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--anomaly", required=True, choices=[f"A{i}" for i in range(1,15)])
    parser.add_argument("--output")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    src = Path(args.input)
    out = Path(args.output) if args.output else src.with_name(f"{src.stem}_{args.anomaly}{src.suffix}")
    df = pd.read_csv(src, parse_dates=["event_ts", "_ingested_at"]) if src.suffix.lower() == ".csv" else pd.read_parquet(src)
    injected = inject(df, args.anomaly, args.seed)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.suffix.lower() == ".csv":
        injected.to_csv(out, index=False)
    else:
        injected.to_parquet(out, index=False)
    print(f"{args.anomaly}: {len(df):,} -> {len(injected):,} rows -> {out}")

if __name__ == "__main__":
    main()
