SIZES        ?= 1000,10000,50000,100000,500000
METRICS_OUT  ?= metrics
FUZZ_TIME    ?= 15s
BENCH_FILTER ?= ^Benchmark
RAW_DIR      := $(METRICS_OUT)/raw
PLOT_DIR     := $(METRICS_OUT)/plots
PROF_DIR     := $(METRICS_OUT)/profiles

.PHONY: test test-short fuzz bench collect metrics plot profile clean

test:
	go test ./...

test-short:
	go test -short ./...

fuzz:
	go test -run '$$^' -fuzz '^FuzzSearcherAgainstBrute$$' -fuzztime $(FUZZ_TIME) ./internal/geoindex/

bench:
	SIZES='$(SIZES)' go test -run '$$^' -bench '$(BENCH_FILTER)' -benchmem ./internal/geoindex/

collect:
	mkdir -p $(RAW_DIR) $(PLOT_DIR)
	SIZES='$(SIZES)' go test -run '$$^' -bench '$(BENCH_FILTER)' -benchmem ./internal/geoindex/ > $(RAW_DIR)/benchmarks.txt
	awk 'BEGIN { OFS=","; print "operation,impl,size,iterations,ns_per_op,ns_per_item,ci95_ns_per_item,ops_per_sec,bytes_per_op" } \
	/^Benchmark(Insert|FindNearby_VaryN|BuildKDTree)\// { \
	  name=$$1; iters=$$2; \
	  split(name, a, "/"); bench=a[1]; sub("^Benchmark", "", bench); \
	  impl=""; size=0; \
	  for (k=2; k<=length(a); k++) { \
	    if (a[k] ~ /^size=/) { split(a[k], s, "="); split(s[2], sd, "-"); size=sd[1]; } \
	    else { split(a[k], sd, "-"); impl=sd[1]; } \
	  } \
	  delete m; \
	  for (i=3; i+1<=NF; i+=2) m[$$(i+1)]=$$i; \
	  ns_item=("ns/item" in m)?m["ns/item"]:0; \
	  ci=("ci95_ns/item" in m)?m["ci95_ns/item"]:0; \
	  ops=("ops/s" in m)?m["ops/s"]:0; \
	  bop=("B/op" in m)?m["B/op"]:0; \
	  nsop=("ns/op" in m)?m["ns/op"]:0; \
	  print bench,impl,size,iters,nsop,ns_item,ci,ops,bop; \
	}' $(RAW_DIR)/benchmarks.txt > $(RAW_DIR)/benchmarks.csv

metrics: collect plot

plot:
	mkdir -p $(PLOT_DIR)
	gnuplot -e "raw_dir='$(RAW_DIR)'; plot_dir='$(PLOT_DIR)'" plot_metrics.gnuplot

profile:
	mkdir -p $(PROF_DIR) $(PLOT_DIR)
	SIZES='10000,100000' go test -run '$$^' \
		-bench '^BenchmarkFindNearby_VaryN$$' -benchmem \
		-cpuprofile $(PROF_DIR)/cpu_findnearby.prof \
		-memprofile $(PROF_DIR)/mem_findnearby.prof \
		./internal/geoindex/ > /dev/null
	SIZES='10000,100000' go test -run '$$^' \
		-bench '^BenchmarkInsert$$' -benchmem \
		-cpuprofile $(PROF_DIR)/cpu_insert.prof \
		-memprofile $(PROF_DIR)/mem_insert.prof \
		./internal/geoindex/ > /dev/null
	go tool pprof -top $(PROF_DIR)/cpu_findnearby.prof  > $(PROF_DIR)/cpu_findnearby_top.txt
	go tool pprof -top $(PROF_DIR)/cpu_insert.prof      > $(PROF_DIR)/cpu_insert_top.txt
	go tool pprof -top $(PROF_DIR)/mem_findnearby.prof  > $(PROF_DIR)/mem_findnearby_top.txt
	go tool pprof -top $(PROF_DIR)/mem_insert.prof      > $(PROF_DIR)/mem_insert_top.txt
	@if command -v dot >/dev/null 2>&1; then \
		go tool pprof -png $(PROF_DIR)/cpu_findnearby.prof > $(PLOT_DIR)/pprof_cpu_findnearby.png; \
		go tool pprof -png $(PROF_DIR)/cpu_insert.prof     > $(PLOT_DIR)/pprof_cpu_insert.png; \
		go tool pprof -png $(PROF_DIR)/mem_findnearby.prof > $(PLOT_DIR)/pprof_mem_findnearby.png; \
		go tool pprof -png $(PROF_DIR)/mem_insert.prof     > $(PLOT_DIR)/pprof_mem_insert.png; \
		echo "pprof PNG call graphs saved to $(PLOT_DIR)"; \
	else \
		echo "graphviz not found — install with: sudo dnf install graphviz"; \
	fi
	@for prof in cpu_findnearby cpu_insert mem_findnearby mem_insert; do \
		go tool pprof -http=:8082 $(PROF_DIR)/$${prof}.prof & \
		PPID=$$!; sleep 2; \
		curl -s "http://localhost:8082/ui/flamegraph" -o $(PLOT_DIR)/flamegraph_$${prof}.html; \
		kill $$PPID 2>/dev/null; sleep 1; \
		echo "flamegraph saved: $(PLOT_DIR)/flamegraph_$${prof}.html"; \
	done

clean:
	rm -rf metrics
