#!/usr/bin/env python3
"""
Эксперименты: ground truth (Flat L2 k=100), затем HNSW / IVF+PQ / LSH (FAISS).
Метрики: Recall@100, время построения, размер сериализованного индекса, время поиска (опционально).
"""
from __future__ import annotations

import argparse
import csv
import json
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import faiss  # noqa: I001 — после numpy в окружении
import numpy as np
from tqdm import tqdm

from io_fvecs import read_fvecs


def set_seed(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def recall_at_k(I_approx: np.ndarray, I_truth: np.ndarray, k: int) -> float:
    """Mean per-query recall: |intersection|/k усреднённое по запросам."""
    assert I_approx.shape == I_truth.shape
    nq, kk = I_truth.shape
    assert kk == k
    total = 0.0
    for i in range(nq):
        total += len(set(map(int, I_approx[i])).intersection(set(map(int, I_truth[i]))))
    return total / (nq * k)


def index_serialized_bytes(index: faiss.Index) -> int:
    with tempfile.NamedTemporaryFile(suffix=".index", delete=True) as f:
        faiss.write_index(index, f.name)
        f.flush()
        return Path(f.name).stat().st_size


@dataclass
class Row:
    family: str
    params_json: str
    build_sec: float
    index_bytes: int
    search_sec: float
    recall: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "params": self.params_json,
            "build_sec": f"{self.build_sec:.6f}",
            "index_bytes": self.index_bytes,
            "search_sec": f"{self.search_sec:.6f}",
            "recall_at_k": self.recall,
        }


def row_plot_label(row: Row) -> str:
    p = json.loads(row.params_json)
    if row.family == "HNSW":
        return f"M{p['M']}_efc{p['efConstruction']}_efs{p['efSearch']}"
    if row.family == "IVF_PQ":
        return f"L{p['nlist']}_m{p['m']}_np{p['nprobe']}"
    if row.family == "LSH":
        return f"nbits{p['nbits']}"
    return "cfg"


def write_plot_series_tsv(rows: Iterable[Row], path: Path) -> None:
    """Табуляция без запятых в полях — удобно для gnuplot/внешних скриптов графиков."""
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("family\tlabel\tbuild_sec\tindex_bytes\tsearch_sec\trecall\n")
        for r in rows:
            lab = row_plot_label(r).replace("\t", " ")
            f.write(
                f"{r.family}\t{lab}\t{r.build_sec}\t{r.index_bytes}\t{r.search_sec}\t{float(r.recall)}\n"
            )


def run_search_timed(index: faiss.Index, xq: np.ndarray, k: int, configure: Callable[[], None] | None) -> tuple[np.ndarray, float]:
    if configure:
        configure()
    t0 = time.perf_counter()
    _, I = index.search(xq, k)
    t1 = time.perf_counter()
    return I, t1 - t0


def pick_queries(rng: np.random.Generator, n: int, nq: int) -> np.ndarray:
    return rng.choice(n, size=nq, replace=False)


def filter_ivf_nlists(nlists: list[int], n: int, min_points_per_cluster: int = 80) -> list[int]:
    """FAISS k-means на IVF: согласовать с нижней границей обучающей выборки (~80×nlist)."""
    return [nl for nl in nlists if n >= min_points_per_cluster * nl]


def compute_ground_truth(xb: np.ndarray, xq: np.ndarray, k: int) -> np.ndarray:
    d = xb.shape[1]
    index = faiss.IndexFlatL2(d)
    index.add(xb)
    _, I = index.search(xq, k)
    return I


def experiments_hnsw(
    d: int,
    xb: np.ndarray,
    xq: np.ndarray,
    I_gt: np.ndarray,
    k: int,
    Ms: Iterable[int],
    ef_constructs: Iterable[int],
    ef_searches: Iterable[int],
    rows_out: list[Row],
) -> None:
    for M in Ms:
        for efc in ef_constructs:
            t0 = time.perf_counter()
            index = faiss.IndexHNSWFlat(d, M)
            index.hnsw.efConstruction = efc
            index.add(xb)
            build_sec = time.perf_counter() - t0
            nbytes = index_serialized_bytes(index)
            params = {"M": M, "efConstruction": efc}

            ef_list = list(ef_searches)
            for es in tqdm(ef_list, desc=f"HNSW M={M} efc={efc}"):
                I, dt = run_search_timed(
                    index,
                    xq,
                    k,
                    configure=lambda ef=es: setattr(index.hnsw, "efSearch", ef),
                )
                r = recall_at_k(I, I_gt, k)
                rows_out.append(
                    Row(
                        family="HNSW",
                        params_json=json.dumps({**params, "efSearch": es}),
                        build_sec=build_sec,
                        index_bytes=nbytes,
                        search_sec=dt,
                        recall=f"{r:.6f}",
                    )
                )


def experiments_ivfpq(
    d: int,
    xb: np.ndarray,
    xq: np.ndarray,
    I_gt: np.ndarray,
    k: int,
    rng: np.random.Generator,
    nlists: Iterable[int],
    ms: Iterable[int],
    nprobes: Iterable[int],
    train_samples: int,
    rows_out: list[Row],
) -> None:
    n = xb.shape[0]
    for nlist in nlists:
        for m_sub in ms:
            if d % m_sub != 0:
                continue
            quantizer = faiss.IndexFlatL2(d)
            index = faiss.IndexIVFPQ(quantizer, d, nlist, m_sub, 8)

            # IVF k-means: Faiss рекомендует >> nlist точек (на практике ~80×nlist — без этого сыплет WARN).
            nt_floor = max(80 * nlist, 8192)
            nt = min(n, max(train_samples, nt_floor))
            ix = rng.choice(n, size=nt, replace=False)
            xt = xb[ix]

            t0 = time.perf_counter()
            index.train(xt)
            index.add(xb)
            build_sec = time.perf_counter() - t0
            nbytes = index_serialized_bytes(index)
            params_base = {"nlist": nlist, "m": m_sub}

            np_list = list(nprobes)
            for probe in tqdm(np_list, desc=f"IVF+PQ nlist={nlist} m={m_sub}"):
                I, dt = run_search_timed(
                    index,
                    xq,
                    k,
                    configure=lambda npb=probe: setattr(index, "nprobe", npb),
                )
                r = recall_at_k(I, I_gt, k)
                rows_out.append(
                    Row(
                        family="IVF_PQ",
                        params_json=json.dumps({**params_base, "nprobe": probe}),
                        build_sec=build_sec,
                        index_bytes=nbytes,
                        search_sec=dt,
                        recall=f"{r:.6f}",
                    )
                )


def experiments_lsh(
    d: int,
    xb: np.ndarray,
    xq: np.ndarray,
    I_gt: np.ndarray,
    k: int,
    nbits_list: Iterable[int],
    rows_out: list[Row],
) -> None:
    """IndexLSH: битовые хэши + таблицы; качество задаётся nbits."""
    for nbits in tqdm(list(nbits_list), desc="LSH"):
        t0 = time.perf_counter()
        index = faiss.IndexLSH(d, nbits)
        index.add(xb)
        build_sec = time.perf_counter() - t0
        nbytes = index_serialized_bytes(index)
        # У LSH при увеличении nprobe в faiss можно выставить параметры — упрощаем один проход поиска
        I, search_sec = run_search_timed(index, xq, k, configure=None)
        r = recall_at_k(I, I_gt, k)
        rows_out.append(
            Row(
                family="LSH",
                params_json=json.dumps({"nbits": nbits}),
                build_sec=build_sec,
                index_bytes=nbytes,
                search_sec=search_sec,
                recall=f"{r:.6f}",
            )
        )


def summarize_best(rows: list[Row]) -> dict[str, Any]:
    """Для каждого family: конфиг с максимальным recall; при равенстве — меньше index_bytes и build_sec."""

    def parse_r(row: Row) -> float:
        return float(row.recall)

    by_fam: dict[str, list[Row]] = {}
    for r in rows:
        by_fam.setdefault(r.family, []).append(r)

    summary: dict[str, Any] = {}
    winners: dict[str, Any] = {}

    for fam, lst in sorted(by_fam.items()):
        lst_sorted = sorted(
            lst,
            key=lambda r: (-parse_r(r), r.index_bytes, float(r.build_sec)),
        )
        best = lst_sorted[0]
        summary[fam] = {
            "best_by_recall_then_size": best.as_dict(),
            "configs_tested": len(lst),
        }
        winners[fam] = float(best.recall)

    # Глобальный «лучший» — эвристика: средний балл после нормализации по рядам (простая формулировка для отчёта).
    scored: list[tuple[float, Row]] = []
    if not rows:
        return {"per_family": summary, "note": "no rows"}

    max_r = max(float(r.recall) for r in rows)
    sizes = [r.index_bytes for r in rows]
    min_sz = min(sizes)
    max_sz = max(sizes)
    min_b = min(float(r.build_sec) for r in rows)
    max_b = max(float(r.build_sec) for r in rows)
    min_s = min(float(r.search_sec) for r in rows)
    max_s = max(float(r.search_sec) for r in rows)

    def norm_r(x: float) -> float:
        return x / max_r if max_r > 0 else 0.0

    def norm_sz(x: int) -> float:
        if max_sz <= min_sz:
            return 1.0
        # меньше размер — лучше
        return 1.0 - (x - min_sz) / (max_sz - min_sz)

    def norm_bt(x: float) -> float:
        if max_b <= min_b:
            return 1.0
        return 1.0 - (x - min_b) / (max_b - min_b)

    def norm_search(x: float) -> float:
        if max_s <= min_s:
            return 1.0
        return 1.0 - (x - min_s) / (max_s - min_s)

    for r in rows:
        pr = float(r.recall)
        score = (
            0.40 * norm_r(pr)
            + 0.20 * norm_sz(r.index_bytes)
            + 0.20 * norm_bt(float(r.build_sec))
            + 0.20 * norm_search(float(r.search_sec))
        )
        scored.append((score, r))

    scored.sort(key=lambda t: (-t[0], -float(t[1].recall)))
    global_best = scored[0][1]

    return {
        "per_family": summary,
        "global_tradeoff_pick": global_best.as_dict(),
        "global_tradeoff_score_weights": {
            "recall": 0.40,
            "inverse_size": 0.20,
            "inverse_build_time": 0.20,
            "inverse_search_time": 0.20,
        },
        "winner_recall_only": max(winners.items(), key=lambda kv: kv[1]),
    }


def main() -> None:
    here = Path(__file__).resolve().parent
    p = argparse.ArgumentParser()
    p.add_argument(
        "--data",
        type=Path,
        default=here / "data" / "sift_base.fvecs",
        help="Файл fvecs корпуса (по умолчанию SIFT1M base).",
    )
    p.add_argument(
        "--synthetic",
        type=int,
        default=0,
        metavar="N",
        help="Если >0 — не читать файл, сгенерировать случайный корпус N×128 (для отладки).",
    )
    p.add_argument("--max-vectors", type=int, default=0, help="0 = все вектора в файле.")
    p.add_argument("--nq", type=int, default=10_000, help="Число случайных запросов.")
    p.add_argument("--k", type=int, default=100)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--quick", action="store_true", help="Меньший корпус/сетка для отладки.")
    p.add_argument("--family", choices=("all", "hnsw", "ivfpq", "lsh"), default="all")
    p.add_argument(
        "--out-dir",
        type=Path,
        default=here / "metrics" / "raw",
        help="Каталог для results.csv, summary.json, manifest.json, plot_series.tsv",
    )
    args = p.parse_args()

    rng = set_seed(args.seed)

    if args.synthetic > 0:
        xb = rng.standard_normal((args.synthetic, 128), dtype=np.float32)
    else:
        if not args.data.exists():
            raise SystemExit(f"Нет файла данных: {args.data}. Запустите: python download_data.py")
        mv = None if args.max_vectors == 0 else args.max_vectors
        xb = read_fvecs(args.data, max_vectors=mv).astype(np.float32)
    # SIFT benchmark: точные соседи в метрике L2 без нормализации векторов.

    n, d = xb.shape
    nq_req = args.nq
    if args.quick:
        nq_req = min(nq_req, 2000)

    if nq_req > n:
        raise SystemExit(f"Число запросов ({nq_req}) превышает размер корпуса ({n}).")
    qi = pick_queries(rng, n, nq_req)
    xq = xb[qi].copy()

    args.out_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "vectors_n": int(n),
        "dim": int(d),
        "nq": int(nq_req),
        "k": int(args.k),
        "seed": int(args.seed),
        "data_path": str(args.data.resolve()) if args.synthetic == 0 else None,
        "synthetic_n": int(args.synthetic) if args.synthetic > 0 else None,
        "quick": bool(args.quick),
    }

    print("Exact kNN (IndexFlatL2) → ground truth…", flush=True)
    gt0 = time.perf_counter()
    I_gt = compute_ground_truth(xb, xq, args.k)
    gt_sec = time.perf_counter() - gt0
    manifest["gt_sec"] = round(gt_sec, 4)
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"GT готово за {gt_sec:.2f}s", flush=True)

    rows_out: list[Row] = []

    if args.quick:
        Ms = [8, 16, 24]
        ef_construct = [40, 120]
        ef_search = [32, 64, 128]
        nlists = [128, 512]
        ms_pq = [16, 32]
        nprobes = [1, 4, 16, 64]
        nbits_list = [64, 128, 256, 512]
        train_mult = 64  # nt = min(n, train_mult * nlist_max)
    else:
        Ms = [8, 16, 24, 32, 48]
        ef_construct = [40, 120, 200]
        ef_search = [32, 64, 96, 128, 192, 256]
        nlists = [256, 512, 1024, 2048, 4096]
        ms_pq = [16, 32, 64]
        nprobes = [1, 2, 4, 8, 16, 32, 64]
        nbits_list = [64, 128, 192, 256, 384, 512]
        train_mult = 256

    fam = args.family
    if fam in ("all", "hnsw"):
        experiments_hnsw(d, xb, xq, I_gt, args.k, Ms, ef_construct, ef_search, rows_out)
    if fam in ("all", "ivfpq"):
        nlists_eff = filter_ivf_nlists(list(nlists), n)
        if not nlists_eff:
            print("IVF+PQ: ни один nlist не подходит к размеру корпуса; уменьшите nlist или увеличьте N.", flush=True)
        else:
            nlist_max = max(nlists_eff)
            train_samples = min(n, max(train_mult * nlist_max, 39 * nlist_max))
            experiments_ivfpq(
                d, xb, xq, I_gt, args.k, rng, nlists_eff, ms_pq, nprobes, train_samples, rows_out
            )
    if fam in ("all", "lsh"):
        experiments_lsh(d, xb, xq, I_gt, args.k, nbits_list, rows_out)

    csv_path = args.out_dir / "results.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["family", "params", "build_sec", "index_bytes", "search_sec", "recall_at_k"])
        w.writeheader()
        for r in rows_out:
            w.writerow(r.as_dict())

    summary_path = args.out_dir / "summary.json"
    summary_path.write_text(
        json.dumps(summarize_best(rows_out), ensure_ascii=False, indent=2), encoding="utf-8"
    )

    plot_tsv = args.out_dir / "plot_series.tsv"
    write_plot_series_tsv(rows_out, plot_tsv)

    print(f"Результаты: {csv_path}", flush=True)
    print(f"Сводка: {summary_path}", flush=True)
    print(f"TSV для графиков: {plot_tsv}", flush=True)


if __name__ == "__main__":
    main()
