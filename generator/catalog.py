from __future__ import annotations
from pathlib import Path
import argparse
import numpy as np
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "generator" / "config" / "scenarios.yml"

def build_catalog(seed: int = 42, n_artists: int = 400, n_songs: int = 5000) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    cfg = yaml.safe_load(CONFIG.read_text())
    genres = cfg["genres"]

    artist_ids = [f"A{i:04d}" for i in range(1, n_artists + 1)]
    artist_genres = rng.choice(genres, size=n_artists, replace=True)
    artists = pd.DataFrame({
        "artist_id": artist_ids,
        "artist_name": [f"Artist {i:04d}" for i in range(1, n_artists + 1)],
        "genre": artist_genres,
    })

    song_ids = [f"S{i:05d}" for i in range(1, n_songs + 1)]
    song_artist_idx = rng.integers(0, n_artists, size=n_songs)
    durations = np.clip(rng.normal(220_000, 65_000, size=n_songs), 30_000, 900_000).astype(int)
    songs = pd.DataFrame({
        "song_id": song_ids,
        "song_name": [f"Song {i:05d}" for i in range(1, n_songs + 1)],
        "artist_id": [artist_ids[i] for i in song_artist_idx],
        "genre": [artist_genres[i] for i in song_artist_idx],
        "duration_ms": durations,
    })
    return artists, songs

def write_catalog(output_dir: Path | None = None, seed: int = 42) -> tuple[Path, Path]:
    output_dir = output_dir or ROOT / "data" / "catalog"
    output_dir.mkdir(parents=True, exist_ok=True)
    artists, songs = build_catalog(seed=seed)
    artists_path = output_dir / "artists.csv"
    songs_path = output_dir / "songs.csv"
    artists.to_csv(artists_path, index=False)
    songs.to_csv(songs_path, index=False)
    return artists_path, songs_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    a, s = write_catalog(seed=args.seed)
    print(f"artists={a}")
    print(f"songs={s}")
