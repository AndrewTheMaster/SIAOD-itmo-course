.PHONY: bench plots profile artifacts all

METRICS ?= metrics

# Быстрая воспроизводимая сетка без скачивания SIFT (для REPORT/графиков).
SYNTH ?= 100000
NQ ?= 2000

bench:
	mkdir -p $(METRICS)/raw $(METRICS)/plots $(METRICS)/profiles
	.venv/bin/python run_benchmark.py --quick --synthetic $(SYNTH) --nq $(NQ)

plots:
	.venv/bin/python scripts/export_plots.py

profile:
	mkdir -p $(METRICS)/profiles $(METRICS)/plots
	.venv/bin/python -m cProfile -o $(METRICS)/profiles/vec_search.prof scripts/profile_hotpath.py
	.venv/bin/python scripts/pstats_top_png.py --prof $(METRICS)/profiles/vec_search.prof --out $(METRICS)/plots/cpu_profile_python_top.png

artifacts: bench plots profile

all: artifacts
