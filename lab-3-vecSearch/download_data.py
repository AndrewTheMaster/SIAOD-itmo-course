#!/usr/bin/env python3
"""Скачивание SIFT1M base (1M векторов, размерность 128) — стандартный корпус для ANN."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests
from tqdm import tqdm

SIFT_BASE_URL = "http://corpus-texmex.irisa.fr/vectors/sift/sift_base.fvecs"
DEFAULT_OUT = Path(__file__).resolve().parent / "data" / "sift_base.fvecs"


def download(url: str, dest: Path, chunk: int = 1 << 20) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        print(f"Уже есть: {dest}", file=sys.stderr)
        return
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(
        total=total, unit="B", unit_scale=True, desc=dest.name
    ) as pbar:
        for c in r.iter_content(chunk_size=chunk):
            if c:
                f.write(c)
                pbar.update(len(c))
    print(f"Сохранено: {dest}", file=sys.stderr)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--url", default=SIFT_BASE_URL)
    args = p.parse_args()
    download(args.url, args.out)


if __name__ == "__main__":
    main()
