#!/usr/bin/env python3
"""Строит PNG из metrics/raw/plot_series.tsv (после run_benchmark.py)."""
from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np


def read_tsv(path: Path) -> dict[str, np.ndarray]:
    rec: dict[str, list] = {"family": [], "label": [], "build_sec": [], "index_bytes": [], "search_sec": [], "recall": []}
    with path.open(encoding="utf-8") as f:
        hdr = f.readline().strip().split("\t")
        if hdr != ["family", "label", "build_sec", "index_bytes", "search_sec", "recall"]:
            raise ValueError(f"Неожиданный заголовок TSV {path}: {hdr}")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) != 6:
                continue
            family, lab, bs, ib, ss, rk = parts
            rec["family"].append(family)
            rec["label"].append(lab)
            rec["build_sec"].append(float(bs))
            rec["index_bytes"].append(int(ib))
            rec["search_sec"].append(float(ss))
            rec["recall"].append(float(rk))
    return {k: np.array(v) if k != "index_bytes" else np.asarray(v, dtype=np.int64) for k, v in rec.items()}


def plot_recall_vs_search(data: dict[str, np.ndarray], out: Path, title_suffix: str) -> None:
    families = sorted(set(data["family"]))
    cmap = plt.get_cmap("tab10")
    fig, ax = plt.subplots(figsize=(9.2, 5.6))
    for i, fam in enumerate(families):
        m = data["family"] == fam
        ax.scatter(
            data["search_sec"][m],
            data["recall"][m],
            s=42,
            alpha=0.78,
            label=fam,
            color=cmap(i % 10),
            edgecolors="white",
            linewidths=0.4,
        )
    ax.set_xlabel("Время поиска (полный batched search), с")
    ax.set_ylabel("Recall@k (средний по запросам)")
    ax.set_title(f"Recall vs latency — {title_suffix}")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.grid(True, alpha=0.35)
    ax.legend(loc="lower right")
    ax.set_xlim(left=0)
    ax.set_ylim(0.0, 1.02)
    fig.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=150)
    plt.close(fig)


def plot_recall_vs_index_mb(data: dict[str, np.ndarray], out: Path, title_suffix: str) -> None:
    families = sorted(set(data["family"]))
    cmap = plt.get_cmap("tab10")
    mb = data["index_bytes"].astype(np.float64) / (1024 * 1024)
    fig, ax = plt.subplots(figsize=(9.2, 5.6))
    for i, fam in enumerate(families):
        m = data["family"] == fam
        ax.scatter(
            mb[m],
            data["recall"][m],
            s=42,
            alpha=0.78,
            label=fam,
            color=cmap(i % 10),
            edgecolors="white",
            linewidths=0.4,
        )
    ax.set_xlabel("Размер сериализованного индекса, МБ")
    ax.set_ylabel("Recall@k")
    ax.set_title(f"Recall vs размер индекса — {title_suffix}")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.grid(True, alpha=0.35)
    ax.legend(loc="lower right")
    ax.set_xlim(left=0)
    ax.set_ylim(0.0, 1.02)
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    plt.close(fig)


def main() -> None:
    here = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser()
    p.add_argument("--tsv", type=Path, default=here / "metrics" / "raw" / "plot_series.tsv")
    p.add_argument("--out-dir", type=Path, default=here / "metrics" / "plots")
    p.add_argument("--title-suffix", default="векторный поиск (FAISS)")
    args = p.parse_args()
    data = read_tsv(args.tsv)
    if len(data["family"]) == 0:
        raise SystemExit(f"Пустой TSV: {args.tsv}")
    plot_recall_vs_search(data, args.out_dir / "recall_vs_search_sec.png", args.title_suffix)
    plot_recall_vs_index_mb(data, args.out_dir / "recall_vs_index_mb.png", args.title_suffix)


if __name__ == "__main__":
    main()
