#!/usr/bin/env python3
"""
Сценарий для cProfile: построение HNSW + IVF-PQ на случайных векторах и batched search.
Основная нагрузка уходит в нативную libfaiss; в профиле Python видно обёртки и NumPy.

Запуск: python -m cProfile -o metrics/profiles/vec_search.prof scripts/profile_hotpath.py
"""
from __future__ import annotations

import faiss
import numpy as np


def main() -> None:
    rng = np.random.default_rng(0)
    n, d, nq = 55_000, 128, 800
    xb = rng.standard_normal((n, d)).astype(np.float32)
    xq = rng.standard_normal((nq, d)).astype(np.float32)

    h = faiss.IndexHNSWFlat(d, 16)
    h.hnsw.efConstruction = 100
    h.add(xb)
    h.hnsw.efSearch = 64
    _, _ = h.search(xq, 50)

    nlist = 256
    m = 32
    quantizer = faiss.IndexFlatL2(d)
    ivf = faiss.IndexIVFPQ(quantizer, d, nlist, m, 8)
    train = min(n, 80 * nlist)
    xt = xb[rng.choice(n, size=train, replace=False)]
    ivf.train(xt)
    ivf.add(xb)
    ivf.nprobe = 16
    _, _ = ivf.search(xq, 50)


if __name__ == "__main__":
    main()
