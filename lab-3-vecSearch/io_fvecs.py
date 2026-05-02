"""Чтение формата fvecs (int32 d, затем d float32)."""
from __future__ import annotations

import struct
from pathlib import Path

import numpy as np


def read_fvecs(path: Path | str, max_vectors: int | None = None) -> np.ndarray:
    path = Path(path)
    vecs = []
    with path.open("rb") as f:
        while True:
            d_bytes = f.read(4)
            if len(d_bytes) < 4:
                break
            (d,) = struct.unpack("<i", d_bytes)
            buf = f.read(4 * d)
            if len(buf) != 4 * d:
                break
            vec = np.frombuffer(buf, dtype=np.float32)
            vecs.append(vec)
            if max_vectors is not None and len(vecs) >= max_vectors:
                break
    if not vecs:
        raise ValueError(f"Пустой или битый fvecs: {path}")
    return np.stack(vecs, axis=0).astype(np.float32, copy=False)
