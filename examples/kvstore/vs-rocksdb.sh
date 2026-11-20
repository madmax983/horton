#!/usr/bin/env bash
# horton's kvstore against RocksDB, on db_bench's workloads.
#
#   examples/kvstore/vs-rocksdb.sh [NUM] [--sync]
#
# Runs the same benchmarks, with the same keys and values, on:
#
#   rocksdb          db_bench as it ships: 64 MiB memtable, 32 MiB block
#                    cache, 4 KiB blocks, background flush and compaction
#   rocksdb (small)  db_bench given the kvstore's memory: 1 MiB memtable,
#                    8 MiB block cache, 16 KiB blocks
#   horton           this repository's kvstore example
#
# and prints operations per second side by side. NUM is the number of
# distinct keys (default 100000). --sync makes every write durable
# before it returns (db_bench --sync=1; the kvstore's default) and runs
# the write benchmarks only.
#
# Needs RocksDB's db_bench on PATH (Debian/Ubuntu: apt install
# rocksdb-tools). Set KVSTORE_BASELINE to another kvstore binary to add
# it as a column, for before/after runs.
#
# Reading the numbers: each engine runs 4 client threads. db_bench gives
# every thread NUM operations (4 x NUM in all); the kvstore splits NUM
# between its threads. Rates are comparable; totals are not. Both write
# 16-byte keys and 100-byte values that compress to half, and both hold
# NUM distinct keys once filled. docs/THROUGHPUT.md has a recorded run
# and what the gaps mean.
set -euo pipefail

num=100000
sync=0
for arg in "$@"; do
  case "$arg" in
    --sync) sync=1 ;;
    ''|*[!0-9]*) echo "usage: $0 [NUM] [--sync]" >&2; exit 2 ;;
    *) num=$arg ;;
  esac
done

cd "$(dirname "$0")/../.."
command -v db_bench >/dev/null || {
  echo "db_bench is not on PATH (Debian/Ubuntu: apt install rocksdb-tools)" >&2
  exit 1
}

work=target/vs-rocksdb
mkdir -p "$work"
if [ "$sync" = 1 ]; then
  benchmarks=fillseq,fillrandom,overwrite
else
  benchmarks=fillseq,fillrandom,overwrite,readrandom,readmissing,readseq,readreverse,seekrandom,deleterandom,compact,readrandom
fi

cargo build --release --example kvstore >&2

# Each run appends "name ops_per_second" lines to its column file. A
# benchmark that appears twice (readrandom, before and after compact) is
# numbered on its second appearance.
number() {
  awk '{ n[$1]++; print (n[$1] > 1 ? $1 "#" n[$1] : $1), $2 }'
}

rocksdb() { # column-file, extra db_bench flags...
  local out=$1
  shift
  rm -rf "$work/rocksdb"
  db_bench --db="$work/rocksdb" --benchmarks="$benchmarks" --num="$num" \
    --threads=4 --key_size=16 --value_size=100 --compression_ratio=0.5 \
    --bloom_bits=10 --sync="$sync" "$@" 2>&1 |
    awk '$2 == ":" && /ops\/sec/ { for (i = 1; i <= NF; i++) if ($i == "ops/sec") print $1, $(i - 1) }' |
    number >"$out"
  rm -rf "$work/rocksdb"
}

horton() { # column-file, kvstore binary
  local out=$1 bin=$2 flag=--no-sync
  [ "$sync" = 1 ] && flag=
  # `compact` reports seconds, not a rate: left out of the table.
  "$bin" --db "$work/horton.db" $flag bench --num "$num" --threads 4 \
    --benchmarks "$benchmarks" 2>&1 |
    awk '$2 == ":" && /ops\/s\)/ { for (i = 1; i <= NF; i++) if ($i ~ /^\(/ && $(i + 1) == "ops/s)") print $1, substr($i, 2) }' |
    number >"$out"
}

echo "RocksDB, as shipped ..." >&2
rocksdb "$work/col.1"
echo "RocksDB, small ..." >&2
rocksdb "$work/col.2" --write_buffer_size=1048576 --cache_size=8388608 --block_size=16384
columns=("rocksdb" "rocksdb (small)")
files=("$work/col.1" "$work/col.2")
if [ -n "${KVSTORE_BASELINE:-}" ]; then
  echo "horton, baseline ..." >&2
  horton "$work/col.3" "$KVSTORE_BASELINE"
  columns+=("horton (before)")
  files+=("$work/col.3")
fi
echo "horton ..." >&2
horton "$work/col.4" target/release/examples/kvstore
columns+=("horton")
files+=("$work/col.4")

echo
db_bench --version 2>&1 | head -1
echo "$num keys, 16-byte keys, 100-byte values, 4 threads, $([ "$sync" = 1 ] && echo "every write synced" || echo "no fsync"); ops/s"
echo
# One row per benchmark the kvstore reported, one column per run, and the
# ratio of the last column (horton) to the second (rocksdb, small).
awk -v names="$(IFS='|'; echo "${columns[*]}")" '
  FNR == 1 { run++ }
  { rate[run, $1] = $2; if (run == ARGC - 1) order[++rows] = $1 }
  END {
    n = split(names, title, "|")
    printf "%-26s", "benchmark"
    for (c = 1; c <= n; c++) printf "%17s", title[c]
    printf "%26s\n", "horton / rocksdb (small)"
    for (r = 1; r <= rows; r++) {
      name = order[r]
      label = name
      sub(/#2$/, " (after compact)", label)
      printf "%-26s", label
      for (c = 1; c <= n; c++) printf "%17s", ((c, name) in rate) ? sprintf("%d", rate[c, name]) : "-"
      small = rate[2, name]; mine = rate[n, name]
      printf "%26s\n", (small > 0 && mine > 0) ? sprintf("%.2fx", mine / small) : "-"
    }
  }
' "${files[@]}"
rm -f "$work"/col.*
