reset
csv_dir = raw_dir

set terminal pngcairo size 960, 560 enhanced font "Arial,11"
set key outside right top
set grid
set border linewidth 1.2
set logscale x 2
set xlabel "Preload size (distinct keys)"

series(bench, impl) = sprintf('%s/series_%s_%s.tsv', csv_dir, bench, impl)

# ── Parallel Get ─────────────────────────────────────────────────
set output plot_dir . "/latency_parallel_get_hit.png"
set title "Parallel Get-hit"
set ylabel "ns/op"
unset logscale y
plot series("BenchmarkParallelGetHit", "concmap") \
      using 1:2 with linespoints lw 2 pt 7 lc rgb "#377eb8" title "concmap", \
     series("BenchmarkParallelGetHit", "plain") \
      using 1:2 with linespoints lw 2 pt 9 lc rgb "#e41a1c" title "plain RWMutex+map"

# ── Parallel Put overwrite ───────────────────────────────────────
set output plot_dir . "/latency_parallel_put_overwrite.png"
set title "Parallel Put overwrite"
set ylabel "ns/op"
plot series("BenchmarkParallelPutOverwrite", "concmap") using 1:2 \
      with linespoints lw 2 pt 7 lc rgb "#377eb8" title "concmap", \
     series("BenchmarkParallelPutOverwrite", "plain") using 1:2 \
      with linespoints lw 2 pt 9 lc rgb "#e41a1c" title "plain"

# ── Mixed R/W ─────────────────────────────────────────────────────
set output plot_dir . "/latency_parallel_mixed_rw.png"
set title "Parallel mixed RW (~ 1:1:1)"
set ylabel "ns/op"
plot series("BenchmarkParallelMixedRW", "concmap") using 1:2 \
      with linespoints lw 2 pt 7 lc rgb "#377eb8" title "concmap", \
     series("BenchmarkParallelMixedRW", "plain") using 1:2 \
      with linespoints lw 2 pt 9 lc rgb "#e41a1c" title "plain"

# ── Range (single goroutine harness) ───────────────────────────────
set output plot_dir . "/latency_range_full_table.png"
set title "Range full table"
set ylabel "ns/op"
plot series("BenchmarkRangeFullTable", "concmap") using 1:2 \
      with linespoints lw 2 pt 7 lc rgb "#377eb8" title "concmap", \
     series("BenchmarkRangeFullTable", "plain") using 1:2 \
      with linespoints lw 2 pt 9 lc rgb "#e41a1c" title "plain"
