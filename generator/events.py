from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

from generator.catalog import write_catalog

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "generator" / "config" / "scenarios.yml"

# Relative listening volume by hour of day (UTC). Monte Carlo needs a repeating
# shape to learn from; uniform random timestamps teach it nothing.
HOURLY_SHAPE = np.array([
    0.30, 0.20, 0.15, 0.12, 0.15, 0.25, 0.50, 0.85,
    1.15, 1.20, 1.10, 1.05, 1.10, 1.05, 1.00, 1.05,
    1.20, 1.45, 1.55, 1.50, 1.35, 1.10, 0.75, 0.45,
])

# Monday-to-Sunday multiplier.
WEEKDAY_SHAPE = np.array([1.00, 0.98, 1.00, 1.04, 1.18, 1.22, 1.10])


def _load_cfg() -> dict:
    return yaml.safe_load(CONFIG.read_text())


def _make_event_ids(rng: np.random.Generator, n: int) -> list[str]:
    """Proper RFC 4122 v4 UUIDs, derived from the seeded generator."""
    raw = rng.integers(0, 256, size=(n, 16), dtype=np.uint8)
    raw[:, 6] = (raw[:, 6] & 0x0F) | 0x40  # version 4
    raw[:, 8] = (raw[:, 8] & 0x3F) | 0x80  # RFC 4122 variant
    return [str(uuid.UUID(bytes=row.tobytes())) for row in raw]


def _timestamps(rng: np.random.Generator, total: int, days: int, now: datetime) -> np.ndarray:
    """Timestamps that follow a daily and weekly shape instead of being uniform."""
    start = (now - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)
    hours = int((now - start).total_seconds() // 3600)
    slots = pd.date_range(start=start, periods=hours, freq="h")

    weight = HOURLY_SHAPE[slots.hour.to_numpy()] * WEEKDAY_SHAPE[slots.dayofweek.to_numpy()]
    weight = weight / weight.sum()

    slot_idx = rng.choice(hours, size=total, p=weight)
    offset_s = rng.integers(0, 3600, size=total)
    return slots.to_numpy()[slot_idx] + offset_s.astype("timedelta64[s]")


def _session_ids(user_ids: np.ndarray, ts: np.ndarray, gap_minutes: int = 30) -> np.ndarray:
    """Real sessions: consecutive events by the same user within `gap_minutes`.

    Without this, session features are noise. A session is a contiguous run of
    activity, not a random bucket.
    """
    order = np.lexsort((ts, user_ids))
    u_sorted = user_ids[order]
    t_sorted = ts[order]

    new_user = np.empty(len(order), dtype=bool)
    new_user[0] = True
    new_user[1:] = u_sorted[1:] != u_sorted[:-1]

    gap = np.empty(len(order), dtype=bool)
    gap[0] = True
    gap[1:] = (t_sorted[1:] - t_sorted[:-1]) > np.timedelta64(gap_minutes, "m")

    session_no = np.cumsum(new_user | gap)
    out = np.empty(len(order), dtype=object)
    out[order] = [f"SS-{n:09d}" for n in session_no]
    return out


def generate_events(
    days: int = 1,
    users: int = 10_000,
    events_per_day: int = 100_000,
    seed: int = 42,
    as_of: datetime | None = None,
) -> pd.DataFrame:
    """`as_of` pins the reference instant so a run is byte-identical given a seed.

    Without it the output depends on wall-clock time and the experiment is not
    reproducible: baseline and injected runs would sit on different windows.
    """
    rng = np.random.default_rng(seed)
    cfg = _load_cfg()

    _, songs_path = write_catalog(seed=seed)
    songs = pd.read_csv(songs_path)

    user_ids = np.array([f"U{i:06d}" for i in range(1, users + 1)])
    user_activity = rng.lognormal(mean=0.0, sigma=1.0, size=users)
    user_activity = user_activity / user_activity.sum()

    countries = np.array(list(cfg["countries"].keys()))
    country_probs = np.array(list(cfg["countries"].values()), dtype=float)
    country_probs = country_probs / country_probs.sum()
    user_country = rng.choice(countries, size=users, p=country_probs)

    genres = np.array(cfg["genres"])
    genre_pos = {g: i for i, g in enumerate(genres)}
    user_primary = rng.integers(0, len(genres), size=users)
    user_secondary = rng.integers(0, len(genres), size=users)

    event_types = np.array(list(cfg["event_type_weights"].keys()))
    event_probs = np.array(list(cfg["event_type_weights"].values()), dtype=float)
    event_probs = event_probs / event_probs.sum()

    song_genre_pos = songs["genre"].map(genre_pos).to_numpy()
    by_genre = [np.flatnonzero(song_genre_pos == i) for i in range(len(genres))]
    all_idx = np.arange(len(songs))

    total = days * events_per_day
    user_pos = rng.choice(users, size=total, p=user_activity)
    type_idx = rng.choice(len(event_types), size=total, p=event_probs)
    sampled_types = event_types[type_idx]

    now = as_of or datetime.now(timezone.utc).replace(tzinfo=None)
    ts = _timestamps(rng, total, days, now)

    # Vectorised song choice: 58% primary genre, 24% secondary, 18% catalogue-wide.
    draw = rng.random(total)
    pool_choice = np.where(draw < 0.58, 0, np.where(draw < 0.82, 1, 2))
    wanted = np.where(
        pool_choice == 0, user_primary[user_pos],
        np.where(pool_choice == 1, user_secondary[user_pos], -1),
    )
    song_indices = np.empty(total, dtype=np.int64)
    for g in range(len(genres)):
        mask = wanted == g
        pool = by_genre[g] if len(by_genre[g]) else all_idx
        song_indices[mask] = rng.choice(pool, size=int(mask.sum()))
    rest = wanted == -1
    song_indices[rest] = rng.choice(all_idx, size=int(rest.sum()))

    chosen = songs.iloc[song_indices].reset_index(drop=True)
    duration = chosen["duration_ms"].to_numpy()

    # Vectorised completion ratio, one distribution per event type.
    completion = rng.beta(7, 2, size=total)
    u = rng.random(total)
    is_skip = sampled_types == "skip"
    is_pause = sampled_types == "pause"
    is_like = sampled_types == "like"
    is_replay = sampled_types == "replay"
    completion[is_skip] = 0.02 + u[is_skip] * 0.53
    completion[is_pause] = 0.15 + u[is_pause] * 0.75
    completion[is_replay] = 0.85 + u[is_replay] * 0.15
    like_silent = is_like & (rng.random(total) < 0.75)
    completion[is_like] = 0.60 + u[is_like] * 0.40
    completion[like_silent] = 0.0

    ms_played = np.minimum(duration, (duration * completion).astype(np.int64))

    df = pd.DataFrame({
        "event_id": _make_event_ids(rng, total),
        "user_id": user_ids[user_pos],
        "session_id": _session_ids(user_ids[user_pos], ts),
        "song_id": chosen["song_id"].to_numpy(),
        "artist_id": chosen["artist_id"].to_numpy(),
        "genre": chosen["genre"].to_numpy(),
        "event_type": sampled_types,
        "event_ts": ts,
        "duration_ms": duration,
        "ms_played": ms_played,
        "device_type": rng.choice(
            ["mobile", "desktop", "web", "smart_speaker"],
            size=total, p=[0.58, 0.17, 0.20, 0.05],
        ),
        "country": user_country[user_pos],
        "_ingested_at": pd.Timestamp(now),
        "_source_file": f"synthetic_{seed}_{days}d",
    })
    return df.sort_values("event_ts").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=int(os.getenv("GENERATOR_DAYS", "1")))
    parser.add_argument("--users", type=int, default=int(os.getenv("GENERATOR_USERS", "10000")))
    parser.add_argument("--events-per-day", type=int, default=int(os.getenv("GENERATOR_EVENTS_PER_DAY", "100000")))
    parser.add_argument("--seed", type=int, default=int(os.getenv("GENERATOR_SEED", "42")))
    parser.add_argument("--output", default=str(ROOT / "data" / "generated" / "listening_events.parquet"))
    parser.add_argument("--as-of", default=os.getenv("GENERATOR_AS_OF"),
                        help="ISO instant to generate against, e.g. 2026-09-16T00:00:00. "
                             "Pin it to make a run reproducible.")
    args = parser.parse_args()

    as_of = datetime.fromisoformat(args.as_of) if args.as_of else None

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = generate_events(days=args.days, users=args.users,
                         events_per_day=args.events_per_day, seed=args.seed, as_of=as_of)
    if out.suffix.lower() == ".csv":
        df.to_csv(out, index=False)
    else:
        df.to_parquet(out, index=False)
    print(f"generated={len(df):,} rows -> {out}")


if __name__ == "__main__":
    main()
