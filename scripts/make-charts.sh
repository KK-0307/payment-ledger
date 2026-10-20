#!/usr/bin/env bash
# Regenerates the charts (docs/img) and full tables (docs/results) quoted in
# docs/BENCHMARKS.md from the result files under bench/sweeps.
#   scripts/make-charts.sh
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
go build -o "${TMPDIR:-/tmp}/benchtool" ./cmd/bench
B="${TMPDIR:-/tmp}/benchtool"
mkdir -p docs/img docs/results
C=(-arm single-row=bench/sweeps/contention-2/baseline -arm sharding-off=bench/sweeps/contention-2/shard-off -arm sharding-on=bench/sweeps/contention-2/shard-on)
HOT10K=hot90-c8-n10000,hot90-c32-n10000,hot90-c128-n10000,hot50-c8-n10000,hot50-c32-n10000,hot50-c128-n10000,zipf-c8-n10000,zipf-c32-n10000,zipf-c128-n10000
"$B" plot "${C[@]}" -metric throughput -scenarios "$HOT10K" -title "Throughput, 10,000 operations, closed loop (workload-concurrency)" -out docs/img/contention-throughput-10k.svg
"$B" plot "${C[@]}" -metric p99 -scenarios "$HOT10K" -title "p99 latency of succeeded payments, 10,000 operations, closed loop" -out docs/img/contention-p99-10k.svg
"$B" plot "${C[@]}" -metric lockshare -scenarios hot90-c8-n10000,hot90-c32-n10000,hot90-c128-n10000,hot50-c32-n10000,zipf-c8-n10000,zipf-c32-n10000,zipf-c128-n10000 -title "Row-lock waits as a share of active-backend samples, 10,000 operations" -out docs/img/contention-lockshare-10k.svg
"$B" plot "${C[@]}" -metric throughput -scenarios hot90-c8-n1000,hot90-c32-n1000,hot90-c128-n1000,hot50-c32-n1000,hot50-c128-n1000,zipf-c8-n1000,zipf-c32-n1000,zipf-c128-n1000 -title "Throughput, 1,000 operations, closed loop" -out docs/img/contention-throughput-1k.svg
"$B" plot "${C[@]}" -metric throughput -scenarios zipf-open1000-n10000,zipf-open2500-n10000,zipf-open5000-n10000,hot90-open1000-n10000,hot90-open2500-n10000,hot90-open5000-n10000 -title "Achieved throughput at offered open-loop rates (arrivals/s in the name)" -out docs/img/contention-open-loop.svg
SUITE=(-arm single-row=bench/sweeps/suite-3/baseline -arm sharding-off=bench/sweeps/suite-3/shard-off -arm sharding-on=bench/sweeps/suite-3/adaptive)
"$B" plot "${SUITE[@]}" -metric throughput -scenarios 01-baseline,03-processor-errors,06-cached-processor-500s,07-database-faults -title "Suite scenarios with 7 interleaved rounds" -out docs/img/suite-throughput.svg
"$B" matrix "${C[@]}" > docs/results/contention-matrix.md
"$B" matrix "${SUITE[@]}" > docs/results/suite-matrix.md
"$B" matrix -arm single-row=bench/sweeps/workloads/baseline -arm sharding-off=bench/sweeps/workloads/shard-off -arm sharding-on=bench/sweeps/workloads/shard-on > docs/results/workloads-matrix.md
"$B" matrix -arm single-row=bench/sweeps/lockorder-fix/single-row -arm before-fix=bench/sweeps/lockorder-fix/before-fix -arm after-fix=bench/sweeps/lockorder-fix/after-fix > docs/results/lockorder-matrix.md

# ---- charts for docs/ENGINEERING_CASE_STUDY.md and the README
CS=(-arm "single-row (original)=bench/sweeps/contention-2/baseline" -arm "sharding off (new code, disabled)=bench/sweeps/contention-2/shard-off" -arm "sharding on (default)=bench/sweeps/contention-2/shard-on")
COND='Closed loop, 10,000 payment operations, one API instance, Stripe simulator, no injected faults, no retries needed.\nMedian of 5 interleaved rounds per arm (whiskers: min-max). Apple M4 Max laptop, PostgreSQL 16; every component shares the host.'
"$B" plot "${CS[@]}" -metric throughput -scenarios hot90-c8-n10000,hot90-c32-n10000,hot90-c128-n10000 -labels "8 workers,32 workers,128 workers" -values \
  -title "Merchant sharding: throughput when 90% of operations hit one merchant" -subtitle "$COND" -out docs/img/cs-sharding-hot90-throughput.svg
"$B" plot "${CS[@]}" -metric throughput -scenarios zipf-c8-n10000,zipf-c32-n10000,zipf-c128-n10000 -labels "8 workers,32 workers,128 workers" -values \
  -title "Merchant sharding: throughput on the suite's own Zipf merchant mix (about 21% to the top merchant)" -subtitle "$COND\nDifferences under roughly 20% are inside the run-to-run noise (the sharding-off arm differs from single-row by up to 21%)." -out docs/img/cs-sharding-zipf-throughput.svg
"$B" plot "${CS[@]}" -metric p99 -scenarios hot90-c8-n10000,hot90-c32-n10000,hot90-c128-n10000,zipf-c8-n10000,zipf-c32-n10000,zipf-c128-n10000 \
  -labels "90% hot - 8 workers,90% hot - 32 workers,90% hot - 128 workers,Zipf mix - 8 workers,Zipf mix - 32 workers,Zipf mix - 128 workers" -values \
  -title "Merchant sharding: p99 latency of succeeded payments (lower is better)" -subtitle "$COND" -out docs/img/cs-sharding-p99.svg
LO=(-arm "before fix=bench/sweeps/lockorder-fix/before-fix" -arm "after fix=bench/sweeps/lockorder-fix/after-fix")
LCOND='Closed loop, 32 workers, 20,000 operations; 20% of operations are sent twice at once with the same Idempotency-Key. Sharding on in both arms.\nMedian of 5 interleaved rounds (whiskers: min-max). Apple M4 Max laptop, PostgreSQL 16; every component shares the host.'
"$B" plot "${LO[@]}" -metric throughput -scenarios duplicates-zipf,duplicates-hot90 -labels "Zipf merchant mix,90% of operations to one merchant" -values \
  -title "Concurrent duplicate requests: throughput before and after the lock-order fix" -subtitle "$LCOND" -out docs/img/cs-duplicates-throughput.svg
"$B" plot "${LO[@]}" -metric p99 -scenarios duplicates-zipf,duplicates-hot90 -labels "Zipf merchant mix,90% of operations to one merchant" -values \
  -title "Concurrent duplicate requests: p99 latency before and after the lock-order fix (lower is better)" -subtitle "$LCOND" -out docs/img/cs-duplicates-p99.svg
"$B" outcomes \
  -group "Protected suite: faults injected (3 arms)=bench/sweeps/suite-3/baseline+bench/sweeps/suite-3/shard-off+bench/sweeps/suite-3/adaptive@skip=05-@note=Processor errors, lost responses, cached 500s, DB resets and blackouts, API SIGKILLs, chaos." \
  -group "Hot-merchant contention matrix (3 arms)=bench/sweeps/contention-2/baseline+bench/sweeps/contention-2/shard-off+bench/sweeps/contention-2/shard-on@note=24 scenarios: hot merchant, concurrency 8-128, 1,000 and 10,000 operations, open-loop rates." \
  -group "Workload shapes: steady, bursty, duplicates (3 arms)=bench/sweeps/workloads/baseline+bench/sweeps/workloads/shard-off+bench/sweeps/workloads/shard-on@note=Deadlocks here come from the duplicate runs before the lock-order fix; PostgreSQL aborted and the code retried." \
  -group "Lock-order fix checks (before/after)=bench/sweeps/lockorder-fix/single-row+bench/sweeps/lockorder-fix/before-fix+bench/sweeps/lockorder-fix/after-fix+bench/sweeps/lockorder-cost/before-fix+bench/sweeps/lockorder-cost/after-fix@note=All deadlocks are in the before-fix and single-row arms; after the fix: 0." \
  -group "Ablation: idempotency and recovery switched off=bench/sweeps/suite-3/baseline+bench/sweeps/suite-3/adaptive@only=05-@expect-failure@note=The same requests and injected faults as scenario 04, which passed in every run. This shows the oracle detects the failures it checks for." \
  -title "Accounting correctness outcomes across the recorded benchmark runs" \
  -subtitle "Oracle per run: 12 database invariant checks, comparison with the Stripe simulator's own records, and client-response checks (docs/BENCHMARKS.md, The oracle)." \
  -out docs/img/cs-correctness.svg
echo "charts and tables regenerated"
