"""Build the detection matrix from what was OBSERVED, never from what was expected.

Reads every run_*.csv produced by run_injection.py and renders the matrix, then
compares it against the hypothesis in expected_matrix.yml. Divergences are the
interesting output: they are the places where intuition about what each tool
catches did not survive contact with the pipeline.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "experiment" / "results"
EXPECTED = ROOT / "experiment" / "expected_matrix.yml"
CONTROLS = ["great_expectations", "dbt", "soda", "monte_carlo"]
GLYPH = {True: "●", False: "—", None: "·"}


def load_runs() -> pd.DataFrame:
    files = sorted(RESULTS.glob("run_*.csv"))
    if not files:
        raise SystemExit(
            "No runs found in experiment/results/.\n"
            "The matrix is measured, not declared: run `make experiment` first."
        )
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df["detected"] = df["detected"].map({True: True, False: False, "True": True, "False": False})
    # Keep the most recent run per (anomaly, control).
    df = df.sort_values("detected_at").drop_duplicates(["anomaly", "control"], keep="last")
    return df


def observed_matrix(runs: pd.DataFrame) -> pd.DataFrame:
    order = sorted(runs["anomaly"].unique(), key=lambda a: int(a[1:]))
    grid = runs.pivot(index="anomaly", columns="control", values="detected")
    grid = grid.reindex(index=order, columns=CONTROLS)
    return grid


def latency_table(runs: pd.DataFrame) -> pd.DataFrame:
    det = runs[runs["detected"] == True]  # noqa: E712
    if det.empty:
        return pd.DataFrame()
    return det.pivot_table(index="anomaly", columns="control",
                           values="latency_seconds", aggfunc="min")


def render(grid: pd.DataFrame) -> str:
    head = "| #    | " + " | ".join(c.replace("_", " ").title() for c in CONTROLS) + " |"
    sep = "| --- " * (len(CONTROLS) + 1) + "|"
    lines = [head, sep]
    for anomaly, row in grid.iterrows():
        cells = " | ".join(GLYPH[row.get(c) if pd.notna(row.get(c)) else None] for c in CONTROLS)
        lines.append(f"| {anomaly:<4} | {cells} |")
    return "\n".join(lines)


def compare(grid: pd.DataFrame) -> list[dict]:
    if not EXPECTED.exists():
        return []
    expected = yaml.safe_load(EXPECTED.read_text())["expected"]
    out = []
    for anomaly, row in grid.iterrows():
        exp = set(expected.get(anomaly, []))
        obs = {c for c in CONTROLS if row.get(c) is True}
        unknown = {c for c in CONTROLS if pd.isna(row.get(c))}
        for c in sorted((exp - obs) - unknown):
            out.append({"anomaly": anomaly, "control": c,
                        "divergence": "expected to detect, did not"})
        for c in sorted(obs - exp):
            out.append({"anomaly": anomaly, "control": c,
                        "divergence": "detected, was not expected to"})
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(ROOT / "docs" / "detection_matrix.md"))
    args = parser.parse_args()

    runs = load_runs()
    grid = observed_matrix(runs)
    divergences = compare(grid)
    lat = latency_table(runs)

    covered = sorted(grid.index, key=lambda a: int(a[1:]))
    missing = [f"A{i}" for i in range(1, 15) if f"A{i}" not in covered]
    only_mc = [a for a, r in grid.iterrows()
               if r.get("monte_carlo") is True
               and not any(r.get(c) is True for c in CONTROLS if c != "monte_carlo")]
    only_soda = [a for a, r in grid.iterrows()
                 if r.get("soda") is True
                 and not any(r.get(c) is True for c in CONTROLS if c != "soda")]

    body = [
        "# Detection matrix (observed)",
        "",
        f"Generated from {len(runs['run_id'].unique())} runs covering "
        f"{len(covered)} of 14 anomalies.",
        "",
        "`●` detected · `—` not detected · `·` control unavailable or not reached",
        "",
        render(grid),
        "",
        "## Independence of the layers",
        "",
        f"- Detected only by Monte Carlo: {', '.join(only_mc) or 'none'}",
        f"- Detected only by Soda: {', '.join(only_soda) or 'none'}",
        "",
        "Success criterion 3 of the project requires at least three anomalies in the "
        "first list and at least one in the second. If a layer detects nothing that "
        "no other layer detects, that layer is redundant and the stack is oversized.",
        "",
    ]
    if missing:
        body += [f"**Not yet measured:** {', '.join(missing)}", ""]
    if not lat.empty:
        body += ["## Detection latency (seconds from injection)", "",
                 lat.round(1).to_markdown(), ""]
    body += ["## Divergences from the hypothesis", ""]
    body += ([pd.DataFrame(divergences).to_markdown(index=False)] if divergences
             else ["None: every control behaved as predicted."])
    body += ["", "---", "", "Regenerate with `make matrix`. Do not edit by hand: "
             "this file is measured output."]

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(body) + "\n")
    grid.to_csv(RESULTS / "detection_matrix.csv")

    print(render(grid))
    print(f"\nonly Monte Carlo: {only_mc or 'none'}")
    print(f"only Soda:        {only_soda or 'none'}")
    print(f"divergences:      {len(divergences)}")
    print(f"\n-> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
