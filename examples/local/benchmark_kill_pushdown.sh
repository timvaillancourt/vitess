#!/bin/bash

# Benchmark script for query kill pushdown feature.
#
# Usage:
#   ./benchmark_kill_pushdown.sh prepare <num_rows>
#   ./benchmark_kill_pushdown.sh run <slow_threads> <fast_threads>
#   ./benchmark_kill_pushdown.sh sweep <pool:slow:fast> [pool:slow:fast] ...
#
# Expects a local Vitess cluster to be running (via 101_initial_cluster.sh).
# The script will restart vttablet(s) with tuned flags for benchmarking.
#
# Environment variables (with defaults):
#   VTGATE_HOST       - vtgate MySQL host (default: 127.0.0.1)
#   VTGATE_PORT       - vtgate MySQL port (default: 15306)
#   POOL_SIZE         - queryserver pool size (default: 10)
#   QUERY_TIMEOUT     - query timeout (default: 2s)
#   TABLET_UIDS       - space-separated tablet UIDs (default: "100 101")
#   VTBENCH_COUNT     - queries per thread per vtbench run (default: 100)
#   RESULTS_DIR       - directory for results (default: ./benchmark_results)
#   BENCH_DB          - database/keyspace (default: commerce)
#   LOAD_AVG_ABORT    - abort benchmark if load average exceeds this (default: 2 * num_cpus)

set -eo pipefail

# Find real mysql binary before sourcing env.sh (which aliases mysql)
MYSQL_BIN=$(command -v mysql)

source ../common/env.sh

# --- Configuration ---
VTGATE_HOST="${VTGATE_HOST:-127.0.0.1}"
VTGATE_WEB_PORT="${VTGATE_WEB_PORT:-15001}"
VTGATE_MYSQL_PORT="${VTGATE_MYSQL_PORT:-15306}"
VTGATE_GRPC_PORT="${VTGATE_GRPC_PORT:-15991}"
POOL_SIZE="${POOL_SIZE:-10}"
QUERY_TIMEOUT="${QUERY_TIMEOUT:-2s}"
QUERY_TIMEOUT_MS="${QUERY_TIMEOUT_MS:-2000}"
TABLET_UIDS="${TABLET_UIDS:-100 101}"
VTBENCH_COUNT="${VTBENCH_COUNT:-100}"
VTBENCH_DEADLINE="${VTBENCH_DEADLINE:-300s}"
COOLDOWN_SECS="${COOLDOWN_SECS:-300}"
LOAD_AVG_THRESHOLD="${LOAD_AVG_THRESHOLD:-3.0}"
NUM_CPUS=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)
LOAD_AVG_ABORT="${LOAD_AVG_ABORT:-$(echo "$NUM_CPUS * 2" | bc)}"
RESULTS_DIR="${RESULTS_DIR:-./benchmark_results}"
BENCH_DB="${BENCH_DB:-commerce}"
CELL="${CELL:-zone1}"

SLOW_QUERY="SELECT COUNT(*) FROM bench_data WHERE pad LIKE '%benchmark_needle%' OR pad LIKE '%another_needle%'"
FAST_QUERY="SELECT * FROM bench_data WHERE id = 1 LIMIT 1"

# --- Helpers ---

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

fail() {
    log "ERROR: $*"
    exit 1
}

# Wait for 1-minute load average to drop below threshold.
wait_for_low_load() {
    local threshold=$LOAD_AVG_THRESHOLD
    while true; do
        local load
        load=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || uptime | awk -F'load averages?: ' '{print $2}' | awk -F'[, ]' '{print $1}')
        if [ "$(echo "$load < $threshold" | bc -l)" -eq 1 ]; then
            log "Load average ($load) below threshold ($threshold)"
            return
        fi
        log "Waiting for load average to drop (current: $load, threshold: $threshold)..."
        sleep 5
    done
}

# Monitor load average during benchmark and kill vtbench if it exceeds the abort threshold.
# Usage: start_load_watchdog; ... ; stop_load_watchdog
_load_watchdog_pid=""
start_load_watchdog() {
    (
        while true; do
            local load
            load=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || uptime | awk -F'load averages?: ' '{print $2}' | awk -F'[, ]' '{print $1}')
            if [ "$(echo "$load > $LOAD_AVG_ABORT" | bc -l)" -eq 1 ]; then
                log "ABORT: Load average ($load) exceeded abort threshold ($LOAD_AVG_ABORT) — killing vtbench"
                pkill -f vtbench 2>/dev/null || true
                exit 1
            fi
            sleep 2
        done
    ) &
    _load_watchdog_pid=$!
}

stop_load_watchdog() {
    if [ -n "$_load_watchdog_pid" ]; then
        kill "$_load_watchdog_pid" 2>/dev/null || true
        wait "$_load_watchdog_pid" 2>/dev/null || true
        _load_watchdog_pid=""
    fi
}

mysql_exec() {
    "$MYSQL_BIN" --no-defaults -h "$VTGATE_HOST" -P "$VTGATE_MYSQL_PORT" --binary-as-hex=false -e "$1" "$BENCH_DB" 2>&1
}

mysql_exec_quiet() {
    "$MYSQL_BIN" --no-defaults -h "$VTGATE_HOST" -P "$VTGATE_MYSQL_PORT" --binary-as-hex=false -N -e "$1" "$BENCH_DB" 2>&1
}

# Get the primary tablet UID by checking which tablet is serving as primary.
get_primary_uid() {
    for uid in $TABLET_UIDS; do
        local port=$((15000 + uid))
        local tablet_type
        tablet_type=$(curl -s "http://127.0.0.1:${port}/debug/vars" 2>/dev/null | \
            python3 -c "import sys,json; print(json.load(sys.stdin).get('TabletType',''))" 2>/dev/null || echo "")
        if [ "$tablet_type" = "primary" ] || [ "$tablet_type" = "PRIMARY" ]; then
            echo "$uid"
            return
        fi
    done
    # Fallback: assume first UID is primary
    echo "${TABLET_UIDS%% *}"
}

# Restart a vttablet with benchmark flags.
# Args: uid
restart_vttablet() {
    local uid=$1

    local port=$((15000 + uid))
    local grpc_port=$((16000 + uid))
    printf -v alias '%s-%010d' "$CELL" "$uid"
    printf -v tablet_dir 'vt_%010d' "$uid"
    printf -v tablet_logfile 'vttablet_%010d_querylog.txt' "$uid"

    local tablet_type=replica
    if [[ "${uid: -1}" -gt 1 ]]; then
        tablet_type=rdonly
    fi

    # Kill existing vttablet for this UID by matching the tablet-path flag
    local pid
    pid=$(pgrep -f "vttablet.*--tablet-path $alias" 2>/dev/null || true)
    if [ -n "$pid" ]; then
        log "Stopping vttablet $alias (pid $pid)..."
        kill "$pid"
        # Wait for process to exit
        for _ in $(seq 1 30); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    fi

    log "Starting vttablet $alias with benchmark flags..."

    # shellcheck disable=SC2086
    vttablet \
        $TOPOLOGY_FLAGS \
        --log-queries-to-file "$VTDATAROOT/tmp/$tablet_logfile" \
        --tablet-path "$alias" \
        --tablet-hostname "" \
        --init-keyspace "$BENCH_DB" \
        --init-shard "0" \
        --init-tablet-type "$tablet_type" \
        --health-check-interval 5s \
        --backup-storage-implementation file \
        --file-backup-storage-root "$VTDATAROOT/backups" \
        --restore-from-backup \
        --port "$port" \
        --grpc-port "$grpc_port" \
        --service-map 'grpc-queryservice,grpc-tabletmanager,grpc-updatestream' \
        --pid-file "$VTDATAROOT/$tablet_dir/vttablet.pid" \
        --heartbeat-on-demand-duration=5s \
        --pprof-http \
        --log-format text \
        --queryserver-config-pool-size "$POOL_SIZE" \
        --queryserver-config-query-timeout "$QUERY_TIMEOUT" \
        --enable-consolidator=false \
        >"$VTDATAROOT/$tablet_dir/vttablet.out" 2>&1 &

    # Wait for tablet to be up
    for _ in $(seq 0 300); do
        curl -I "http://127.0.0.1:$port/debug/status" >/dev/null 2>&1 && break
        sleep 0.1
    done
    curl -I "http://127.0.0.1:$port/debug/status" >/dev/null 2>&1 || fail "vttablet $alias failed to start"
    log "vttablet $alias is running (port=$port, grpc=$grpc_port)"
}

# Restart all vttablets with given extra flags.
restart_all_vttablets() {
    for uid in $TABLET_UIDS; do
        restart_vttablet "$uid"
    done

    # Without vtorc, we need to ensure a primary is elected.
    # Pick the first tablet UID as the primary.
    local primary_uid
    primary_uid=$(echo "$TABLET_UIDS" | awk '{print $1}')
    printf -v primary_alias '%s-%010d' "$CELL" "$primary_uid"
    log "Ensuring primary election (PlannedReparentShard -> $primary_alias)..."
    vtctldclient PlannedReparentShard --new-primary="$primary_alias" "$BENCH_DB/0" 2>&1 || true

    # Wait for primary to be healthy
    log "Waiting for healthy shard..."
    local num_tablets
    num_tablets=$(echo "$TABLET_UIDS" | wc -w | tr -d ' ')
    wait_for_healthy_shard "$BENCH_DB" 0 "$num_tablets" || fail "Shard did not become healthy"
    log "Shard is healthy"
}

# Restart vtgate with optional extra flags.
restart_vtgate() {
    local extra_flags=("$@")

    local pid
    pid=$(pgrep -f "vtgate.*--port $VTGATE_WEB_PORT" 2>/dev/null || true)
    if [ -n "$pid" ]; then
        log "Stopping vtgate (pid $pid)..."
        kill "$pid"
        for _ in $(seq 1 30); do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.5
        done
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null
    fi

    log "Starting vtgate..."
    # shellcheck disable=SC2086
    vtgate \
        $TOPOLOGY_FLAGS \
        --log-queries-to-file "$VTDATAROOT/tmp/vtgate_querylog.txt" \
        --port "$VTGATE_WEB_PORT" \
        --grpc-port "$VTGATE_GRPC_PORT" \
        --mysql-server-port "$VTGATE_MYSQL_PORT" \
        --mysql-server-socket-path /tmp/mysql.sock \
        --cell "$CELL" \
        --cells-to-watch "$CELL" \
        --tablet-types-to-wait PRIMARY,REPLICA \
        --service-map 'grpc-vtgateservice' \
        --pid-file "$VTDATAROOT/tmp/vtgate.pid" \
        --enable-buffer \
        --mysql-auth-server-impl none \
        --pprof-http \
        --log-format text \
        --query-timeout "$QUERY_TIMEOUT_MS" \
        "${extra_flags[@]}" \
        >"$VTDATAROOT/tmp/vtgate.out" 2>&1 &

    for _ in $(seq 0 300); do
        curl -I "http://127.0.0.1:$VTGATE_WEB_PORT/debug/status" >/dev/null 2>&1 && break
        sleep 0.1
    done
    curl -I "http://127.0.0.1:$VTGATE_WEB_PORT/debug/status" >/dev/null 2>&1 || fail "vtgate failed to start"
    log "vtgate is running (port=$VTGATE_WEB_PORT, grpc=$VTGATE_GRPC_PORT, mysql=$VTGATE_MYSQL_PORT)"
}

# Convert ps cputime format (HH:MM:SS.ss or M:SS.ss) to milliseconds.
cputime_to_ms() {
    local t=$1
    python3 -c "
parts = '$t'.split(':')
if len(parts) == 3:
    h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
elif len(parts) == 2:
    h, m, s = 0, int(parts[0]), float(parts[1])
else:
    h, m, s = 0, 0, float(parts[0])
print(int((h * 3600 + m * 60 + s) * 1000))
"
}

# Get process CPU time in milliseconds.
# Args: pid
get_process_cpu_ms() {
    local pid=$1
    local t
    t=$(ps -o cputime= -p "$pid" 2>/dev/null | tr -d ' ')
    [ -n "$t" ] && cputime_to_ms "$t" || echo "0"
}

# Get the primary tablet's mysqld pid.
get_primary_mysqld_pid() {
    local uid
    uid=$(get_primary_uid)
    local pidfile
    pidfile="$VTDATAROOT/vt_$(printf '%010d' "$uid")/mysql.pid"
    if [ -f "$pidfile" ]; then
        cat "$pidfile"
    else
        pgrep -f "mysqld.*vt_$(printf '%010d' "$uid")" | head -1
    fi
}

# Get the primary tablet's vttablet pid.
get_primary_vttablet_pid() {
    local uid
    uid=$(get_primary_uid)
    printf -v alias '%s-%010d' "$CELL" "$uid"
    pgrep -f "vttablet.*--tablet-path $alias" 2>/dev/null | head -1
}

# Scrape kill counters from the primary tablet's /debug/vars.
scrape_kill_counters() {
    local uid
    uid=$(get_primary_uid)
    local port=$((15000 + uid))
    curl -s "http://127.0.0.1:${port}/debug/vars" 2>/dev/null | \
        python3 -c "
import sys, json
data = json.load(sys.stdin)
kills = data.get('Kills', {})
print('Queries=' + str(kills.get('Queries', 0)))
print('QueriesPushdown=' + str(kills.get('QueriesPushdown', 0)))
print('Transactions=' + str(kills.get('Transactions', 0)))
pool = data.get('ConnPoolCapacity', 0)
pool_available = data.get('ConnPoolAvailable', 0)
pool_wait = data.get('ConnPoolWaitCount', 0)
pool_wait_time = data.get('ConnPoolWaitTime', 0)
print('PoolCapacity=' + str(pool))
print('PoolAvailable=' + str(pool_available))
print('PoolWaitCount=' + str(pool_wait))
print('PoolWaitTime=' + str(pool_wait_time))
grace_ms = data.get('KillPushdownGracePeriodMs', 0)
print('GracePeriodMs=' + str(grace_ms))
rt = data.get('KillPushdownResponseTime', {})
total_count = rt.get('TotalCount', 0)
total_time_ns = rt.get('TotalTime', 0)
avg_rt_ms = round(total_time_ns / total_count / 1e6, 1) if total_count > 0 else 0
print('PushdownResponseCount=' + str(total_count))
print('PushdownAvgResponseMs=' + str(avg_rt_ms))
" 2>/dev/null
}

# Parse a counter value from scrape output.
get_counter() {
    local name=$1
    local scrape=$2
    echo "$scrape" | grep "^${name}=" | cut -d= -f2
}

# Parse vtbench output file into key=value pairs.
# Handles both "Errors: 0" and multi-line error formats.
# Computes p50/p95/p99 from the histogram buckets.
parse_vtbench_output() {
    local file=$1
    if [ ! -f "$file" ] || [ ! -s "$file" ]; then
        echo "errors=0"
        echo "completed=0"
        echo "qps=n/a"
        echo "avg_latency=n/a"
        echo "p50=n/a"
        echo "p95=n/a"
        echo "p99=n/a"
        return
    fi
    python3 -c "
import re, sys

lines = open('$file').readlines()

# Parse errors
errors = 0
for i, l in enumerate(lines):
    l = l.strip()
    if l == 'Errors: 0':
        break
    if l == 'Errors:':
        for j in range(i+1, len(lines)):
            parts = lines[j].strip().split(': ')
            if len(parts) == 2 and parts[1].isdigit():
                errors += int(parts[1])
            else:
                break
        break

# Parse QPS and avg latency
qps = 'n/a'
avg_latency = 'n/a'
for l in lines:
    if l.startswith('QPS (Total):'):
        qps = l.split(':')[1].strip()
    elif l.startswith('Average Query Time:'):
        avg_latency = l.split(':')[1].strip()

# Parse histogram and compute percentiles
# Bucket format: '0s-500\u00b5s: 343' or '500\u00b5s-1ms: 726'
buckets = []
in_histogram = False
for l in lines:
    l = l.strip()
    if l == 'Query Timings:':
        in_histogram = True
        continue
    if not in_histogram:
        continue
    m = re.match(r'^(.+?):\s+(\d+)$', l)
    if not m:
        break
    label, count = m.group(1), int(m.group(2))
    # Convert bucket upper bound to microseconds for sorting
    def to_us(s):
        s = s.strip()
        if 'inf' in s: return float('inf')
        # Handle ranges like '0s-500\u00b5s' or '1ms-5ms' or '1s-5s'
        parts = s.split('-')
        upper = parts[-1] if len(parts) > 1 else parts[0]
        upper = upper.strip()
        if upper.endswith('\u00b5s'): return float(upper[:-2])
        if upper.endswith('ms'): return float(upper[:-2]) * 1000
        if upper.endswith('s'): return float(upper[:-1]) * 1000000
        return float(upper)
    buckets.append((to_us(label), count))

completed = sum(c for _, c in buckets)

# Compute percentiles from histogram
def percentile(buckets, total, pct):
    if total == 0: return 'n/a'
    target = total * pct / 100.0
    cumulative = 0
    for upper_us, count in buckets:
        cumulative += count
        if cumulative >= target:
            if upper_us == float('inf'): return '>5s'
            if upper_us >= 1000000: return '%.1fs' % (upper_us / 1000000)
            if upper_us >= 1000: return '%.1fms' % (upper_us / 1000)
            return '%.0f\u00b5s' % upper_us
    return 'n/a'

p50 = percentile(buckets, completed, 50)
p95 = percentile(buckets, completed, 95)
p99 = percentile(buckets, completed, 99)

print(f'errors={errors}')
print(f'completed={completed}')
print(f'qps={qps}')
print(f'avg_latency={avg_latency}')
print(f'p50={p50}')
print(f'p95={p95}')
print(f'p99={p99}')
" 2>/dev/null || {
        echo "errors=0"
        echo "completed=0"
        echo "qps=n/a"
        echo "avg_latency=n/a"
        echo "p50=n/a"
        echo "p95=n/a"
        echo "p99=n/a"
    }
}

# --- Prepare Phase ---

do_prepare() {
    local target_rows=$1

    log "=== PREPARE PHASE ==="

    # Build vtbench from vtbench-errors branch if not already present
    if [ ! -x /tmp/vtbench ]; then
        log "Building vtbench from vtbench-errors branch..."
        local worktree="/tmp/vtbench-build"
        git worktree add "$worktree" vtbench-errors 2>/dev/null || git worktree add "$worktree" origin/vtbench-errors
        (cd "$worktree" && source build.env && go build -o /tmp/vtbench ./go/cmd/vtbench/)
        git worktree remove "$worktree"
        log "vtbench built at /tmp/vtbench"
    else
        log "vtbench already exists at /tmp/vtbench"
    fi
    export PATH="/tmp:$PATH"

    log "Target rows: $target_rows"

    # Create table if it doesn't exist
    mysql_exec "CREATE TABLE IF NOT EXISTS bench_data (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        indexed_col BIGINT NOT NULL,
        pad VARCHAR(255) NOT NULL,
        INDEX idx_indexed (indexed_col)
    )" || fail "Failed to create table"

    # Check current row count
    local current_rows
    current_rows=$(mysql_exec_quiet "SELECT COUNT(*) FROM bench_data" | tr -d '[:space:]')
    current_rows=${current_rows:-0}
    log "Current rows: $current_rows, target: $target_rows"

    if [ "$current_rows" -ge "$target_rows" ]; then
        log "Table already has $current_rows rows (>= $target_rows). Nothing to do."
        return
    fi

    local rows_needed=$((target_rows - current_rows))
    log "Inserting $rows_needed rows..."

    # Insert in batches of 1000 using CTE (stays under default cte_max_recursion_depth of 1001)
    local batch_size=1000
    local inserted=0

    while [ "$inserted" -lt "$rows_needed" ]; do
        local batch=$batch_size
        if [ $((inserted + batch)) -gt "$rows_needed" ]; then
            batch=$((rows_needed - inserted))
        fi

        mysql_exec "INSERT INTO bench_data (indexed_col, pad)
            WITH RECURSIVE seq AS (
                SELECT 1 AS n
                UNION ALL
                SELECT n + 1 FROM seq WHERE n < $batch
            )
            SELECT
                FLOOR(RAND() * 1000000),
                CONCAT(MD5(RAND()), '-', MD5(RAND()), '-', MD5(RAND()), '-', MD5(RAND()))
            FROM seq" || fail "Failed to insert batch"

        inserted=$((inserted + batch))
        if [ $((inserted % 10000)) -eq 0 ] || [ "$inserted" -ge "$rows_needed" ]; then
            log "  Inserted $inserted / $rows_needed rows..."
        fi
    done

    local final_count
    final_count=$(mysql_exec_quiet "SELECT COUNT(*) FROM bench_data" | tr -d '[:space:]')
    log "Table now has $final_count rows"
    log "=== PREPARE COMPLETE ==="
}

# --- Run Phase ---

do_run() {
    local slow_threads=$1
    local fast_threads=$2

    log "=== RUN PHASE ==="
    log "Threads: $slow_threads slow + $fast_threads fast"
    log "Pool size: $POOL_SIZE, Query timeout: $QUERY_TIMEOUT"
    log "Queries per thread: $VTBENCH_COUNT, Deadline: $VTBENCH_DEADLINE, Cooldown: ${COOLDOWN_SECS}s"

    # Verify table exists and has data
    local row_count
    row_count=$(mysql_exec_quiet "SELECT COUNT(*) FROM bench_data" | tr -d '[:space:]')
    if [ -z "$row_count" ] || [ "$row_count" -eq 0 ]; then
        fail "bench_data table is empty. Run 'prepare' first."
    fi
    log "bench_data has $row_count rows"

    mkdir -p "$RESULTS_DIR"

    # Restart vttablets with benchmark pool size/timeout settings
    restart_all_vttablets

    # Wait for system to be idle before starting
    wait_for_low_load

    # Run without pushdown
    log ""
    log "--- Phase 1: Pushdown DISABLED ---"
    restart_vtgate
    run_benchmark "no_pushdown" "$slow_threads" "$fast_threads"

    # Cooldown between phases
    log ""
    log "Cooling down for ${COOLDOWN_SECS}s..."
    sleep "$COOLDOWN_SECS"
    wait_for_low_load

    # Run with pushdown
    log ""
    log "--- Phase 2: Pushdown ENABLED ---"
    restart_vtgate "--query-timeout-select-pushdown"
    run_benchmark "pushdown" "$slow_threads" "$fast_threads"

    # Print comparison
    print_comparison

    log "=== RUN COMPLETE ==="
    log "Full results in: $RESULTS_DIR/"
}

run_benchmark() {
    local label=$1
    local slow_threads=$2
    local fast_threads=$3

    local result_file="$RESULTS_DIR/${label}.txt"

    # Scrape counters before
    local before
    before=$(scrape_kill_counters)
    local before_kills
    before_kills=$(get_counter "Queries" "$before")
    local before_pushdown
    before_pushdown=$(get_counter "QueriesPushdown" "$before")
    local before_pool_wait
    before_pool_wait=$(get_counter "PoolWaitCount" "$before")
    local before_rt_count
    before_rt_count=$(get_counter "PushdownResponseCount" "$before")

    # Scrape CPU times before
    local mysqld_pid vttablet_pid
    mysqld_pid=$(get_primary_mysqld_pid)
    vttablet_pid=$(get_primary_vttablet_pid)
    local before_mysqld_cpu before_vttablet_cpu
    before_mysqld_cpu=$(get_process_cpu_ms "$mysqld_pid")
    before_vttablet_cpu=$(get_process_cpu_ms "$vttablet_pid")

    log "Starting vtbench ($label): $slow_threads slow threads + $fast_threads fast threads"
    log "Duration: $VTBENCH_DEADLINE, load abort threshold: $LOAD_AVG_ABORT"

    local slow_output="$RESULTS_DIR/${label}_slow.txt"
    local fast_output="$RESULTS_DIR/${label}_fast.txt"

    start_load_watchdog

    # Start slow query vtbench
    if [ "$slow_threads" -gt 0 ]; then
        vtbench \
            --protocol grpc-vtgate \
            --host "$VTGATE_HOST" \
            --port "$VTGATE_GRPC_PORT" \
            --db "$BENCH_DB" \
            --threads "$slow_threads" \
            --count "$VTBENCH_COUNT" \
            --deadline "$VTBENCH_DEADLINE" \
            --continue-on-error \
            --sql "$SLOW_QUERY" \
            >"$slow_output" 2>/dev/null &
        local slow_pid=$!
    fi

    # Start fast query vtbench
    if [ "$fast_threads" -gt 0 ]; then
        vtbench \
            --protocol grpc-vtgate \
            --host "$VTGATE_HOST" \
            --port "$VTGATE_GRPC_PORT" \
            --db "$BENCH_DB" \
            --threads "$fast_threads" \
            --count "$VTBENCH_COUNT" \
            --deadline "$VTBENCH_DEADLINE" \
            --continue-on-error \
            --sql "$FAST_QUERY" \
            >"$fast_output" 2>/dev/null &
        local fast_pid=$!
    fi

    # Wait for both to finish
    [ -n "${slow_pid:-}" ] && wait "$slow_pid" 2>/dev/null || true
    [ -n "${fast_pid:-}" ] && wait "$fast_pid" 2>/dev/null || true

    stop_load_watchdog

    log "vtbench complete ($label)"

    # Scrape counters after
    local after
    after=$(scrape_kill_counters)
    local after_kills
    after_kills=$(get_counter "Queries" "$after")
    local after_pushdown
    after_pushdown=$(get_counter "QueriesPushdown" "$after")
    local after_pool_wait
    after_pool_wait=$(get_counter "PoolWaitCount" "$after")
    local after_rt_count
    after_rt_count=$(get_counter "PushdownResponseCount" "$after")
    local grace_period_ms
    grace_period_ms=$(get_counter "GracePeriodMs" "$after")
    local pushdown_avg_rt_ms
    pushdown_avg_rt_ms=$(get_counter "PushdownAvgResponseMs" "$after")

    local delta_kills=$((after_kills - before_kills))
    local delta_pushdown=$((after_pushdown - before_pushdown))
    local delta_pool_wait=$((after_pool_wait - before_pool_wait))
    local delta_rt_count=$((after_rt_count - before_rt_count))

    # Scrape CPU times after and compute deltas
    local after_mysqld_cpu after_vttablet_cpu
    after_mysqld_cpu=$(get_process_cpu_ms "$mysqld_pid")
    after_vttablet_cpu=$(get_process_cpu_ms "$vttablet_pid")
    local delta_mysqld_cpu_ms=$((after_mysqld_cpu - before_mysqld_cpu))
    local delta_vttablet_cpu_ms=$((after_vttablet_cpu - before_vttablet_cpu))

    # Parse vtbench output: errors, completed, QPS, avg latency, percentiles.
    local slow_stats fast_stats
    slow_stats=$(parse_vtbench_output "$slow_output")
    fast_stats=$(parse_vtbench_output "$fast_output")

    local slow_errors slow_completed slow_qps slow_avg_ms slow_p50 slow_p95 slow_p99
    slow_errors=$(echo "$slow_stats" | grep "^errors=" | cut -d= -f2)
    slow_completed=$(echo "$slow_stats" | grep "^completed=" | cut -d= -f2)
    slow_qps=$(echo "$slow_stats" | grep "^qps=" | cut -d= -f2)
    slow_avg_ms=$(echo "$slow_stats" | grep "^avg_latency=" | cut -d= -f2)
    slow_p50=$(echo "$slow_stats" | grep "^p50=" | cut -d= -f2)
    slow_p95=$(echo "$slow_stats" | grep "^p95=" | cut -d= -f2)
    slow_p99=$(echo "$slow_stats" | grep "^p99=" | cut -d= -f2)

    local fast_errors fast_completed fast_qps fast_avg_ms fast_p50 fast_p95 fast_p99
    fast_errors=$(echo "$fast_stats" | grep "^errors=" | cut -d= -f2)
    fast_completed=$(echo "$fast_stats" | grep "^completed=" | cut -d= -f2)
    fast_qps=$(echo "$fast_stats" | grep "^qps=" | cut -d= -f2)
    fast_avg_ms=$(echo "$fast_stats" | grep "^avg_latency=" | cut -d= -f2)
    fast_p50=$(echo "$fast_stats" | grep "^p50=" | cut -d= -f2)
    fast_p95=$(echo "$fast_stats" | grep "^p95=" | cut -d= -f2)
    fast_p99=$(echo "$fast_stats" | grep "^p99=" | cut -d= -f2)

    # Write summary
    {
        echo "=== $label ==="
        echo "slow_threads=$slow_threads"
        echo "fast_threads=$fast_threads"
        echo "pool_size=$POOL_SIZE"
        echo "query_timeout=$QUERY_TIMEOUT"
        echo "count_per_thread=$VTBENCH_COUNT"
        echo "kills_regular=$delta_kills"
        echo "kills_pushdown=$delta_pushdown"
        echo "pool_wait_count=$delta_pool_wait"
        echo "grace_period_ms=$grace_period_ms"
        echo "pushdown_avg_response_ms=$pushdown_avg_rt_ms"
        echo "pushdown_response_count=$delta_rt_count"
        echo "mysqld_cpu_ms=$delta_mysqld_cpu_ms"
        echo "vttablet_cpu_ms=$delta_vttablet_cpu_ms"
        echo "slow_errors=$slow_errors"
        echo "slow_completed=$slow_completed"
        echo "slow_qps=$slow_qps"
        echo "slow_avg_latency=$slow_avg_ms"
        echo "slow_p50=$slow_p50"
        echo "slow_p95=$slow_p95"
        echo "slow_p99=$slow_p99"
        echo "fast_errors=$fast_errors"
        echo "fast_completed=$fast_completed"
        echo "fast_qps=$fast_qps"
        echo "fast_avg_latency=$fast_avg_ms"
        echo "fast_p50=$fast_p50"
        echo "fast_p95=$fast_p95"
        echo "fast_p99=$fast_p99"
        echo ""
        if [ -n "$slow_output" ] && [ -f "$slow_output" ]; then
            echo "--- Slow query vtbench output ---"
            cat "$slow_output"
            echo ""
        fi
        if [ -n "$fast_output" ] && [ -f "$fast_output" ]; then
            echo "--- Fast query vtbench output ---"
            cat "$fast_output"
            echo ""
        fi
    } >"$result_file"

    log "Results ($label):"
    log "  Regular kills:      $delta_kills"
    log "  Pushdown kills:     $delta_pushdown"
    log "  Pool waits:         $delta_pool_wait"
    log "  Grace period:       ${grace_period_ms}ms"
    log "  Pushdown avg RT:    ${pushdown_avg_rt_ms}ms (n=$delta_rt_count)"
    log "  mysqld CPU:         ${delta_mysqld_cpu_ms}ms"
    log "  vttablet CPU:       ${delta_vttablet_cpu_ms}ms"
    log "  Slow: completed=$slow_completed errors=$slow_errors qps=$slow_qps avg=$slow_avg_ms p50=$slow_p50 p95=$slow_p95 p99=$slow_p99"
    log "  Fast: completed=$fast_completed errors=$fast_errors qps=$fast_qps avg=$fast_avg_ms p50=$fast_p50 p95=$fast_p95 p99=$fast_p99"

    if [ -n "$slow_output" ] && [ -f "$slow_output" ]; then
        log "  Slow vtbench:"
        sed 's/^/    /' "$slow_output"
    fi
    if [ -n "$fast_output" ] && [ -f "$fast_output" ]; then
        log "  Fast vtbench:"
        sed 's/^/    /' "$fast_output"
    fi
}

print_comparison() {
    local no_push="$RESULTS_DIR/no_pushdown.txt"
    local push="$RESULTS_DIR/pushdown.txt"

    if [ ! -f "$no_push" ] || [ ! -f "$push" ]; then
        log "Missing result files for comparison"
        return
    fi

    log ""
    log "========================================="
    log "         COMPARISON SUMMARY"
    log "========================================="
    log ""

    local np_kills np_pushdown np_pool_wait
    np_kills=$(grep "^kills_regular=" "$no_push" | cut -d= -f2)
    np_pushdown=$(grep "^kills_pushdown=" "$no_push" | cut -d= -f2)
    np_pool_wait=$(grep "^pool_wait_count=" "$no_push" | cut -d= -f2)

    local np_slow_err np_slow_done np_slow_qps np_slow_avg
    np_slow_err=$(grep "^slow_errors=" "$no_push" | cut -d= -f2)
    np_slow_done=$(grep "^slow_completed=" "$no_push" | cut -d= -f2)
    np_slow_qps=$(grep "^slow_qps=" "$no_push" | cut -d= -f2)
    np_slow_avg=$(grep "^slow_avg_latency=" "$no_push" | cut -d= -f2)

    local np_fast_err np_fast_done np_fast_qps np_fast_avg
    np_fast_err=$(grep "^fast_errors=" "$no_push" | cut -d= -f2)
    np_fast_done=$(grep "^fast_completed=" "$no_push" | cut -d= -f2)
    np_fast_qps=$(grep "^fast_qps=" "$no_push" | cut -d= -f2)
    np_fast_avg=$(grep "^fast_avg_latency=" "$no_push" | cut -d= -f2)

    local np_mysqld_cpu np_vttablet_cpu
    np_mysqld_cpu=$(grep "^mysqld_cpu_ms=" "$no_push" | cut -d= -f2)
    np_vttablet_cpu=$(grep "^vttablet_cpu_ms=" "$no_push" | cut -d= -f2)

    local p_kills p_pushdown p_pool_wait p_grace p_avg_rt
    p_kills=$(grep "^kills_regular=" "$push" | cut -d= -f2)
    p_pushdown=$(grep "^kills_pushdown=" "$push" | cut -d= -f2)
    p_pool_wait=$(grep "^pool_wait_count=" "$push" | cut -d= -f2)
    p_grace=$(grep "^grace_period_ms=" "$push" | cut -d= -f2)
    p_avg_rt=$(grep "^pushdown_avg_response_ms=" "$push" | cut -d= -f2)

    local p_mysqld_cpu p_vttablet_cpu
    p_mysqld_cpu=$(grep "^mysqld_cpu_ms=" "$push" | cut -d= -f2)
    p_vttablet_cpu=$(grep "^vttablet_cpu_ms=" "$push" | cut -d= -f2)

    local p_slow_err p_slow_done p_slow_qps p_slow_avg
    p_slow_err=$(grep "^slow_errors=" "$push" | cut -d= -f2)
    p_slow_done=$(grep "^slow_completed=" "$push" | cut -d= -f2)
    p_slow_qps=$(grep "^slow_qps=" "$push" | cut -d= -f2)
    p_slow_avg=$(grep "^slow_avg_latency=" "$push" | cut -d= -f2)

    local p_fast_err p_fast_done p_fast_qps p_fast_avg
    p_fast_err=$(grep "^fast_errors=" "$push" | cut -d= -f2)
    p_fast_done=$(grep "^fast_completed=" "$push" | cut -d= -f2)
    p_fast_qps=$(grep "^fast_qps=" "$push" | cut -d= -f2)
    p_fast_avg=$(grep "^fast_avg_latency=" "$push" | cut -d= -f2)

    printf "%-25s %15s %15s\n" "Metric" "No Pushdown" "Pushdown"
    printf "%-25s %15s %15s\n" "-------------------------" "---------------" "---------------"
    printf "%-25s %15s %15s\n" "Regular kills" "$np_kills" "$p_kills"
    printf "%-25s %15s %15s\n" "Pushdown kills" "$np_pushdown" "$p_pushdown"
    printf "%-25s %15s %15s\n" "Pool wait count" "$np_pool_wait" "$p_pool_wait"
    printf "%-25s %15s %15s\n" "Grace period (ms)" "n/a" "${p_grace:-n/a}"
    printf "%-25s %15s %15s\n" "Avg pushdown RT (ms)" "n/a" "${p_avg_rt:-n/a}"
    printf "%-25s %15s %15s\n" "" "" ""
    printf "%-25s %15s %15s\n" "mysqld CPU (ms)" "${np_mysqld_cpu:-n/a}" "${p_mysqld_cpu:-n/a}"
    printf "%-25s %15s %15s\n" "vttablet CPU (ms)" "${np_vttablet_cpu:-n/a}" "${p_vttablet_cpu:-n/a}"
    printf "%-25s %15s %15s\n" "" "" ""
    printf "%-25s %15s %15s\n" "Slow: completed" "$np_slow_done" "$p_slow_done"
    printf "%-25s %15s %15s\n" "Slow: errors" "$np_slow_err" "$p_slow_err"
    printf "%-25s %15s %15s\n" "Slow: QPS" "$np_slow_qps" "$p_slow_qps"
    printf "%-25s %15s %15s\n" "Slow: avg latency" "$np_slow_avg" "$p_slow_avg"
    printf "%-25s %15s %15s\n" "Slow: p50" "$(grep '^slow_p50=' "$no_push" | cut -d= -f2)" "$(grep '^slow_p50=' "$push" | cut -d= -f2)"
    printf "%-25s %15s %15s\n" "Slow: p95" "$(grep '^slow_p95=' "$no_push" | cut -d= -f2)" "$(grep '^slow_p95=' "$push" | cut -d= -f2)"
    printf "%-25s %15s %15s\n" "Slow: p99" "$(grep '^slow_p99=' "$no_push" | cut -d= -f2)" "$(grep '^slow_p99=' "$push" | cut -d= -f2)"
    printf "%-25s %15s %15s\n" "" "" ""
    printf "%-25s %15s %15s\n" "Fast: completed" "$np_fast_done" "$p_fast_done"
    printf "%-25s %15s %15s\n" "Fast: errors" "$np_fast_err" "$p_fast_err"
    printf "%-25s %15s %15s\n" "Fast: QPS" "$np_fast_qps" "$p_fast_qps"
    printf "%-25s %15s %15s\n" "Fast: avg latency" "$np_fast_avg" "$p_fast_avg"
    printf "%-25s %15s %15s\n" "Fast: p50" "$(grep '^fast_p50=' "$no_push" | cut -d= -f2)" "$(grep '^fast_p50=' "$push" | cut -d= -f2)"
    printf "%-25s %15s %15s\n" "Fast: p95" "$(grep '^fast_p95=' "$no_push" | cut -d= -f2)" "$(grep '^fast_p95=' "$push" | cut -d= -f2)"
    printf "%-25s %15s %15s\n" "Fast: p99" "$(grep '^fast_p99=' "$no_push" | cut -d= -f2)" "$(grep '^fast_p99=' "$push" | cut -d= -f2)"
    log ""
}

# --- Main ---

# --- Sweep: run benchmarks across multiple pool sizes and generate CSV ---

do_sweep() {
    local specs=("$@")  # pool:slow:fast specs

    local sweep_dir
    sweep_dir="$RESULTS_DIR/sweep_$(date '+%Y%m%d_%H%M%S')"
    mkdir -p "$sweep_dir"

    log "=== SWEEP: specs=${specs[*]} ==="
    log "Results directory: $sweep_dir"

    for spec in "${specs[@]}"; do
        IFS=: read -r pool slow fast <<< "$spec"
        log ""
        log "############################################"
        log "  POOL: $pool  SLOW: $slow  FAST: $fast"
        log "############################################"
        POOL_SIZE="$pool" RESULTS_DIR="$sweep_dir/pool${pool}_s${slow}_f${fast}" do_run "$slow" "$fast"
    done

    # Generate CSV summary
    local csv="$sweep_dir/summary.csv"
    log ""
    log "=== GENERATING CSV: $csv ==="

    # CSV header
    echo "pool_size,slow_threads,fast_threads,mode,kills_regular,kills_pushdown,pool_wait_count,grace_period_ms,pushdown_avg_response_ms,mysqld_cpu_ms,vttablet_cpu_ms,slow_completed,slow_errors,slow_qps,slow_avg_latency,slow_p50,slow_p95,slow_p99,fast_completed,fast_errors,fast_qps,fast_avg_latency,fast_p50,fast_p95,fast_p99" >"$csv"

    for spec in "${specs[@]}"; do
        IFS=: read -r pool slow fast <<< "$spec"
        local dir="$sweep_dir/pool${pool}_s${slow}_f${fast}"
        for mode in no_pushdown pushdown; do
            local f="$dir/${mode}.txt"
            [ -f "$f" ] || continue
            local row="$pool,$slow,$fast,$mode"
            for field in kills_regular kills_pushdown pool_wait_count grace_period_ms pushdown_avg_response_ms \
                         mysqld_cpu_ms vttablet_cpu_ms \
                         slow_completed slow_errors slow_qps slow_avg_latency slow_p50 slow_p95 slow_p99 \
                         fast_completed fast_errors fast_qps fast_avg_latency fast_p50 fast_p95 fast_p99; do
                local val
                val=$(grep "^${field}=" "$f" | cut -d= -f2)
                row="$row,${val:-n/a}"
            done
            echo "$row" >>"$csv"
        done
    done

    log "CSV written to: $csv"
    log ""
    cat "$csv"
    log ""
    log "=== SWEEP COMPLETE ==="
}

# --- Main ---

case "${1:-}" in
    prepare)
        [ -z "${2:-}" ] && fail "Usage: $0 prepare <num_rows>"
        do_prepare "$2"
        ;;
    run)
        [ -z "${2:-}" ] || [ -z "${3:-}" ] && fail "Usage: $0 run <slow_threads> <fast_threads>"
        do_run "$2" "$3"
        ;;
    sweep)
        [ -z "${2:-}" ] && fail "Usage: $0 sweep <pool:slow:fast> [pool:slow:fast] ..."
        shift 1
        do_sweep "$@"
        ;;
    *)
        echo "Usage:"
        echo "  $0 prepare <num_rows>                              - Create/grow benchmark dataset"
        echo "  $0 run <slow_threads> <fast_threads>               - Run benchmark with current POOL_SIZE"
        echo "  $0 sweep <pool:slow:fast> [pool:slow:fast] ...     - Run benchmarks across multiple configurations"
        echo ""
        echo "Examples:"
        echo "  $0 sweep 4:4:8 6:6:12 8:8:16 10:10:20             - Mixed slow+fast workload"
        echo "  $0 sweep 4:0:8 6:0:12 8:0:16 10:0:20              - Fast-only workload"
        echo "  $0 sweep 10:10:0                                   - Slow-only workload"
        echo ""
        echo "Environment variables:"
        echo "  POOL_SIZE=$POOL_SIZE          - vttablet query pool size (overridden by sweep spec)"
        echo "  QUERY_TIMEOUT=$QUERY_TIMEOUT       - vttablet query timeout"
        echo "  VTBENCH_COUNT=$VTBENCH_COUNT       - queries per thread"
        echo "  RESULTS_DIR=$RESULTS_DIR - output directory"
        exit 1
        ;;
esac
