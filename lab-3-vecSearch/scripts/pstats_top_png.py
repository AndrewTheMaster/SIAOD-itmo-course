#!/usr/bin/env python3
"""Рисует горизонтальные топ-N cumulative по профилю cProfile (matplotlib PNG)."""
from __future__ import annotations

import argparse
import pstats
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


def main() -> None:
    here = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser()
    p.add_argument("--prof", type=Path, default=here / "metrics" / "profiles" / "vec_search.prof")
    p.add_argument("--out", type=Path, default=here / "metrics" / "plots" / "cpu_profile_python_top.png")
    p.add_argument("--top", type=int, default=22)
    args = p.parse_args()

    stats = pstats.Stats(str(args.prof))
    stats.strip_dirs()
    pairs: list[tuple[str, float]] = []
    for key, tup in stats.stats.items():
        _cc, _nc, _tt, cumt = tup[0], tup[1], tup[2], tup[3]
        fn, lineno, fname = key
        short = Path(str(fn)).name
        pairs.append((f"{fname} ({short}:{lineno})", float(cumt)))

    pairs.sort(key=lambda t: -t[1])
    top = pairs[: args.top]
    if not top:
        raise SystemExit("Пустой профиль или не удалось прочитать.")

    top = list(reversed(top))
    labels = [t[0] for t in top]
    vals = np.array([t[1] for t in top], dtype=float)

    fig, ax = plt.subplots(figsize=(10, 6.2))
    ax.barh(np.arange(len(labels)), vals, color="#377eb8", edgecolor="white", linewidth=0.5)
    ax.set_yticks(np.arange(len(labels)), labels, fontsize=7.8)
    ax.set_xlabel("cumulative time (с), cProfile")
    ax.set_title("Топ функций по cumulative время (ось Python; ядро Faiss внутри FFI)")
    ax.grid(True, axis="x", alpha=0.35)
    fig.tight_layout()
    args.out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out, dpi=150)
    plt.close(fig)


if __name__ == "__main__":
    main()
