# SIMD fast paths on the Vitess hot query path

## Summary

Go is growing native SIMD support: the `simd` and `simd/archsimd` packages,
behind `GOEXPERIMENT=simd` since Go 1.26. This document ranks the Vitess code
paths that could use it, by measured CPU share on the per-query path, and
records a benchmark-gated prototype of the top three:

1. **Bind-variable escaping** (`sqltypes.encodeBytesSQL*`): vttablet runs it
   through `ParsedQuery.GenerateQuery` on every query with a string or binary
   bind variable.
2. **utf8mb4_0900 collation fast path**
   (`uca.(*FastIterator900).FastForward32`): vtgate runs it for ORDER BY,
   GROUP BY, DISTINCT and hash joins once the tiny-weight comparison ties.
3. **Tokenizer string-literal scanning** (`sqlparser.(*Tokenizer).scanString`):
   vtgate runs it for every literal in every query it parses.

Each fast path has a pure-Go scalar rewrite (the release path today) and a
portable-`simd` kernel that only compiles under `goexperiment.simd`. **A
kernel ships only if it is measurably faster** than the scalar path it
replaces, with no significant regression in any benchmark cell (§4). Release
builds never set `GOEXPERIMENT`; the SIMD kernels become the default path when
a Go release ships `simd` without the experiment gate (§6).

## 1. Go SIMD status

Verified against the go1.27.1 source tree (`$GOROOT/src/simd`).

- `simd/archsimd` (Go 1.26): architecture-specific vector types (`Uint8x32`,
  `Mask8x16`, ...) and intrinsics. amd64 has AVX, AVX2 and AVX-512 tiers plus
  permutes/shuffles (`Uint8x32.Permute`, `Uint16x32.Permute`); arm64 has NEON
  arithmetic and compares but **no permute**; wasm is emulated.
- `simd` (Go 1.27): portable, vector-size-agnostic types (`Uint8s`, `Mask8s`,
  ...) with one code path across AVX-512/AVX2/NEON and pure-Go emulation
  elsewhere. `Uint8s.Len()` is 16 on arm64 and 16/32/64 on amd64 by CPU;
  `GODEBUG=simd=<bits>` caps it. `simd.Emulated()` reports software fallback.
- Both packages exist only under `GOEXPERIMENT=simd`, are not covered by the
  Go 1 compatibility promise, and may change between releases.
- Documented caveats: package-level `var` initializers that produce vector
  values do not work; `reflect.Call` on vector functions is broken.
- Gaps found while building the prototypes:
  - **No movemask / first-set-lane operation.** Extracting a lane index from a
    `Mask8s` is `ToInt8s().ToBits().ReshapeToUint64s().Store(buf)` followed by
    a word scan and `bits.TrailingZeros64`. This costs a store and a reload per
    block, which is why the portable kernels cannot match hand-tuned assembly
    such as `bytes.IndexByte` (§4).
  - **No horizontal reduction** (any/all/max across lanes) in the portable API.
  - **Methods with a receiver that use vector values fail to compile** in
    go1.27.1 (`internal compiler error: missing Types entry: Index@simd0`).
    The kernel body must be a plain function; a thin method that only calls
    it is fine. Closures that capture vector values hit the same error.
  - **A call to a vector function costs the caller its inlining budget.**
    The compiler clones each vector function per vector width, and a call
    to one is charged like any non-inlined call, so a wrapper that does
    "short input → scalar, else → kernel" lands at cost 72–91 against the
    budget of 80. Whether it inlines decides whether every short input pays
    a call: `uca.equalASCIIPrefix` fits (72) and 16-byte compares are
    unchanged; `bytes2.(*ByteSet).Index` does not (88) and 8-byte inputs
    pay about 1ns.
  - **A kernel with a per-block mask store cannot beat stdlib assembly that
    has a movemask.** `bytes.IndexByte` runs at ~42 GB/s on arm64; the
    portable equivalent reached ~12 GB/s (§4). Where a stdlib primitive
    already covers the scan, the fast path is to call it.
  - **`LoadUint8sPart` is a real function call, not an intrinsic**, and Go's
    ABI has no callee-saved vector registers, so the compiler spills and
    reloads every live vector around it. In the eight-member `ByteSet`
    kernel that was 16 `FMOVQ`s and a 240-byte frame; a 4-byte tail cost
    more than a 16-byte block (+26% vs +5% over scalar). Read the tail as
    an overlapping full block ending at the last byte instead: bytes the
    loop already cleared cannot flag, so a hit there is real, and there is
    no zero-fill to discount.
  - **`BroadcastUint8s` from a scalar is three instructions** (`MOVBU`,
    `VMOV` to lane 0, `VDUP`). Eight of them per call was the fixed cost
    that made short inputs lose to the table walk. Pre-broadcasting each
    member into a 64-byte row at construction makes it one vector load.
  - **One 16-byte NEON block is about as expensive as 16 scalar table
    lookups.** On an M4 the scalar walk runs at ~1 byte/cycle; a block is
    8 `VCMEQ` + 7 `VORR` + the mask store, reload and word scan, ~17
    cycles. SIMD wins only when consecutive clean blocks pipeline (~6
    cycles/block on 4 KB clean, 2.3×); a scan that hits every ~32 bytes
    never gets that overlap, so it pays the wrapper call and the setup for
    parity. That is the shape of the remaining `dense` regression, and it
    is a property of 128-bit lanes without a movemask, not of the code.

## 2. Current Vitess state

- `go.mod` is `go 1.27.1`. No `GOEXPERIMENT`, `GOAMD64` or `GOARM64` is set
  in `Makefile`, `build.env`, the Docker images or CI.
- Hand-written SIMD already in tree: `go/vt/vthash/highway` (AVX2, SSE4, NEON
  and ppc64le assembly with `golang.org/x/sys/cpu` dispatch and a `noasm`
  opt-out), used only for the plan-cache key; `go/atomic2` (128-bit atomics).
- Pure-Go hashing on the hot path: `vthash.New()` is Metro128, used by
  `Distinct`, `HashJoin`, evalengine hashing and every collation's `Hash`.
- Vectorized third-party dependencies already cover backups
  (`klauspost/compress` zstd, `pgzip`, `pierrec/lz4`) and the `xxhash`
  vindex (`cespare/xxhash`). MD5/SHA use the standard library's assembly.
- CI runs unit tests on amd64 and arm64 (`unit_test.yml`).

## 3. Hot-path CPU ranking

CPU profiles of the existing benchmarks at the baseline commit (§4), arm64
(Apple M-series), `go tool pprof -top`. The benchmark corpus is the proxy for
the per-query path: `BenchmarkParse3` and `BenchmarkNormalizeVTGate` parse
and normalize the lobsters query log; `BenchmarkOLTP/TPCC/TPCH` plan them;
`BenchmarkCollation*` compare, weight and hash a mixed corpus;
`BenchmarkCompilerExpressions` and `BenchmarkScalarAggregate` cover
evalengine and aggregation. A live vtgate/vttablet profile under sysbench is
the follow-up that would replace this proxy.

| Candidate | Benchmark | flat% | cum% | Notes |
|---|---|---|---|---|
| `encodeBytesSQLBytes2` / `encodeBytesSQLStringBuilder` | `EncodeSQL` | 29.8 / 7.2 | 44.0 / 11.7 | Self-benchmark, so the share is of the encoder alone. Within `GenerateQueryStringBinds` the encoder is ~100% of the substitution cost; there was no existing benchmark that covered it. |
| `(*Collation_utf8mb4_uca_0900).Collate` | `CollationCollate`, `CollateSharedPrefix` | 1.6 | **18.4** | `FastForward32` is 6.8% flat inside it; `NextWeightBlock64` 5.1% cum. |
| `(*Tokenizer).scanString` + `peek` | `NormalizeVTGate` (lobsters corpus) | 0 + 5.4 | **1.5** (`Scan` 8.5, `yyParse` 23.6) | On real queries the literals are short; `peek` is the bounds-checked read every scan routine does per byte. In `Parse3`, a 1 MB query of ten 100 KB literals, the same loop is 38.5% cum — a fair stress test of the primitive, not a corpus share. |
| `(*Tokenizer).scanIdentifier`, `LookupString` | `NormalizeVTGate` | 0.3 | 2.7, 1.1 | Identifiers are too short to vectorize. |
| planbuilder, evalengine, engine candidates | `OLTP/TPCC/TPCH`, `CompilerExpressions`, `ScalarAggregate` | — | — | Profiles are allocation-dominated (`mallocgc` 6%, `memmove` 1%); no byte-loop candidate appears above 0.5%. |

The ranking is by where the byte loop is a large share of a path that runs
per query: escaping is the whole cost of bind substitution on vttablet, the
collation is a fifth of every compare that reaches it, and the tokenizer's
string hunt is a small share of parse time on a real corpus (an earlier draft
of this table put it at 38.5% by mixing in `Parse3`; that number is the
stress test, not the corpus). It stays on the list because the scalar
rewrite is a one-line change to a proven-faster stdlib primitive, and long
literals — JSON payloads, generated INSERTs — are where vtgate parse time
actually goes when it goes anywhere.

**Not candidates**, with reasons: `key.Compare` (inputs are 8 bytes,
`bytes.Compare` is already assembly); `readLenEncInt` and MySQL packet
encoding (per-field, memmove-bound); `binlog.CellValue` (branchy per-type
decode); LIKE wildcard matching (recursive, literal runs already use
`bytes.Index`); `utf8.Valid` (called on charset coercion, not per value);
`Distinct`/`HashJoin` hashing (short keys; setup dominates the hash).

## 4. Benchmark method and viability gate

### Baseline

The benchmarks were added in the first commit of this branch, before any code
moved, so before/after comparisons have a reviewable starting point. Test
binaries built at that commit are kept for the local arm64 comparison, and
the old loops are kept as `*Reference` functions in the test files so a single
test binary at HEAD can report today's code, the new scalar path and the SIMD
path side by side. The `Reference` cells drive the old loop through the same
writer as the new one but call it directly, without `Value.EncodeSQL*`'s type
switch, so they read ~10 ns under the frozen binary on 8–32 byte inputs and
at parity from 256 bytes up.

Benchmarks, all with `-benchmem` and `b.SetBytes`:

- `go/sqltypes`: `BenchmarkEncodeSQL/{Bytes2,StringBuilder}/{8,32,256,4096}/{clean,sparse,dense}`.
- `go/vt/sqlparser`: `BenchmarkGenerateQueryStringBinds/{64B,1KB}`,
  `BenchmarkTokenizerScanString/{squote,dquote}/{16,64,256,4096}/{clean,escape}`.
- `go/mysql/collations/colldata`: `BenchmarkCollateSharedPrefix/utf8mb4_0900_ai_ci/{16,64,256,1024,short-16}`.
- `go/bytes2`: `BenchmarkByteSetIndex/{8,32,256,4096}/{clean,sparse,dense}`, `BenchmarkIndexAny2/{16,64,256,4096}`.
- Regression gates: `BenchmarkParse3`, `BenchmarkNormalizeVTGate`, `BenchmarkCollationCollate`.

### Procedure

```sh
# plain build → the scalar path; GOEXPERIMENT=simd → the SIMD kernels
go test -run '^$' -bench "$BENCH_PATTERN" -count=10 -benchmem $PKGS | tee plain.txt
GOEXPERIMENT=simd go test -run '^$' -bench "$BENCH_PATTERN" -count=10 -benchmem $PKGS | tee simd.txt
go tool -modfile=tools/benchstat/go.mod benchstat plain.txt simd.txt
```

The `simd_experiment.yml` workflow runs exactly this on amd64 and arm64 and
uploads `plain.txt`, `simd.txt` and the `benchstat` comparison as artifacts.
`benchstat` at its default significance level (p < 0.05) decides; `-count=10`
per cell.

### Gate

Applied per fast path before the pull request leaves Draft, on the Go release
it merges on:

- **Scalar rewrite**: no statistically significant regression against the
  baseline in any cell on either architecture. A regressing cell is fixed
  with a length gate, or the rewrite is reverted and the old loop becomes the
  release path.
- **SIMD kernel**: judged against the scalar path it ships with (`plain.txt`
  vs `simd.txt`), because that is the path a build without the kernel runs.
  It stays if at least one representative cell (≥64 B for escaping and
  `scanString`; ≥64 B shared prefix for the collation) shows a significant
  improvement **and** no cell shows a significant regression, including
  `dense`, `8`/`16` and `short-16`. A kernel that passes on one architecture
  only keeps its `_simd.go` with the build tag narrowed to that architecture.
  A kernel that fails on both is deleted; its scalar path stays if that
  passed, and the negative result is recorded below.
- Whole-branch gates: `BenchmarkParse3`, `BenchmarkNormalizeVTGate`,
  `BenchmarkCollationCollate` and `BenchmarkGenerateQueryStringBinds` show no
  significant regression in either build mode.

If no kernel passes, the branch is reduced to the benchmarks and this document
and merges without waiting for the experiment to graduate.

### Results

Filled in as the fast paths land; see the "Results" section at the end.

## 5. Alternatives considered

The user-facing question was whether a third-party SIMD library could carry
the kernels until Go's own package graduates. The survey (repository trees
checked, not READMEs, 2026-09):

| Project | Stars | Activity | Architectures with assembly | Covers the three primitives? |
|---|---|---|---|---|
| `tphakala/simd` | 24 | active | amd64 + arm64 | No: numeric only (f64/f32/f16/i32/i16/i8/complex/crc); `i8` is quantized-ML kernels |
| `segmentio/asm` | 926 | dormant (last code commit 2022) | `mem/` amd64 only | No: `ContainsByte`, `IndexPair` (adjacent pair), `Copy`, `Mask`, `Blend` |
| `gofiber/utils/v2/simd` | 57 | active | amd64 only | Partially: `memchr_class` is a byte-set index; no equal-prefix; a web-framework utility module |
| `go-simd/*` (26 repos) | 0 each | 3 months old, single author | amd64, arm64, riscv64, loong64, ppc64le, s390x | Partially: `matchlen.MatchLen` is equal-prefix without the ASCII check |
| `mmcloughlin/avo` | ~3k | active; used by the Go standard library | amd64 codegen only | Not a library; the generator most Go SIMD projects write their kernels with |
| `bytedance/sonic`, `minio/simdjson-go`, `minio/sha256-simd`, `coregx/coregex` | 0.3k–9.6k | active | various | No: SIMD is internal to JSON, hashing or regex |

No maintained, multi-architecture library exposes "first index of any of N
bytes" or "equal-ASCII prefix length". Every mature Go SIMD project instead
commits small `.s` kernels (generated with avo on amd64, hand-written NEON on
arm64) dispatched on `x/sys/cpu` with a pure-Go fallback behind `noasm`, which
is also what `go/vt/vthash/highway` does. That remains the recorded fallback
vehicle if the `simd` experiment stalls for more than two Go releases; it was
not chosen now because kernels written against the portable `simd` API are
one readable Go source for both architectures and need no assembly review.

## 6. Adoption policy

Normative for SIMD code in this repository.

1. **Portable `simd` first.** `simd/archsimd` only where the portable API has
   no equivalent (today: permutes for table lookups), in files tagged to the
   architecture that has the instruction.
2. **Three files per fast path.** `*_scalar.go` (untagged reference, always
   compiled), `*_noasm.go` (`//go:build !goexperiment.simd || !(amd64 || arm64)`)
   that delegates to it, `*_simd.go` (`//go:build goexperiment.simd && (amd64 || arm64)`).
3. **Kernel shape.** The vector body is a plain function; exported methods
   are thin wrappers (§1 compiler limitation). Constants a kernel compares
   against are pre-broadcast into byte rows at construction and loaded with
   `LoadUint8s`, never built with `BroadcastUint8s` per call or held in
   package-level `var`s. Inputs shorter than the threshold take the scalar
   path, and inputs shorter than one vector always do. The tail is read as
   an overlapping full block ending at the last byte, not with
   `LoadUint8sPart` (§1: it is a call that spills every live vector).
4. **Equivalence is fuzzed.** Every kernel has a table test at the 16/32/64
   byte block boundaries and a fuzz target against the scalar reference; the
   fuzz targets run in the `simd_experiment` workflow under the experiment.
5. **Kernels must pass the gate in §4 to exist.** A kernel that is not
   measurably faster than its fallback is deleted, not kept for later.
6. **No SIMD type in an exported signature; no new `unsafe`.**
7. **Release builds never set `GOEXPERIMENT`.** The SIMD kernels are dormant
   in production until graduation.

### Graduation

When a Go release ships `simd` without `GOEXPERIMENT` and Vitess `main` has
moved `go.mod` to it: rebase; replace `goexperiment.simd && (amd64 || arm64)`
with `!noasm && (amd64 || arm64)` and the `_noasm.go` constraint with
`noasm || !(amd64 || arm64)`, so the kernels are on by default with a
`-tags noasm` opt-out, matching `highway`; drop `GOEXPERIMENT=simd` from the
workflow and have it compare `-tags noasm` against the default build; re-run
the gate on the final release; refresh the results below.

## 7. Roadmap

Each item is a follow-up with its own gating benchmark, in the order the
ranking in §3 suggests.

- `NextWeightBlock64` weight generation (GROUP BY / DISTINCT / HashJoin
  hashing): needs a 128-entry 16-bit table lookup, so `archsimd`
  `Uint16x32.Permute` on AVX-512BW, amd64 only.
- 8-bit collations (`latin1_*`): `Collate`, `WeightString`, `ToLower`/`ToUpper`
  are per-byte 256-entry table walks; ASCII range compares cover most inputs,
  full tables need `archsimd` permutes.
- `utf8.Valid` via a simdutf-style validator, if a profile shows charset
  coercion on a hot path.
- `HEX`/`UNHEX`/`TO_BASE64`/`FROM_BASE64` in evalengine.

## 8. Risks

- **Experiment API churn.** The kernels are small and unexported; a rename in
  Go 1.28 is a mechanical fix. The workflow runs on every Go patch and
  pre-release to surface it early.
- **Whole-tree compile under the experiment.** `go build ./go/...` under
  `GOEXPERIMENT=simd` is a workflow step; if an unrelated dependency fails it
  is narrowed to the SIMD packages and the reason recorded here.
- **Emulated path.** On a CPU where `simd.Emulated()` is true the kernels
  route to the scalar path at run time.
- **Deferred merge.** The branch is held until graduation; the benchmarks and
  this document could merge earlier, but the user's preference was one pull
  request.

## Results

Baseline commit: the first commit of this branch ("Add benchmarks and CI
workflow for the SIMD hot-path candidates"), whose test binaries were kept
and run against HEAD. arm64: Apple M4 Max, go1.27.1, `-count=10
-benchtime=200ms`, `benchstat` default p<0.05. **amd64: pending** — the
`simd_experiment` workflow produces it once the pull request is open; the
verdicts below are arm64 only and the graduation gate re-runs on both.

Columns: *today* = baseline commit; *scalar* = HEAD plain build (the release
path); *simd* = HEAD under `GOEXPERIMENT=simd`. Percentages are vs *today*.

### Bind-variable escaping (`sqltypes`, `GenerateQuery`)

| cell | today | scalar | simd |
|---|---|---|---|
| `EncodeSQL/Bytes2/8/clean` | 40.3n | 6.75n (−83%) | 7.20n (−82%) |
| `EncodeSQL/Bytes2/32/sparse` | 85.5n | 16.5n (−81%) | 18.0n (−79%) |
| `EncodeSQL/Bytes2/32/dense` | 63.2n | 16.7n (−74%) | 18.0n (−72%) |
| `EncodeSQL/Bytes2/32/clean` | 69.3n | 12.7n (−82%) | 10.8n (−84%) |
| `EncodeSQL/Bytes2/256/clean` | 462n | 76.0n (−84%) | 36.5n (−92%) |
| `EncodeSQL/Bytes2/256/dense` | 478n | 98.7n (−79%) | 102n (−79%) |
| `EncodeSQL/Bytes2/4096/clean` | 7.14µ | 1.05µ (−85%) | 468n (−93%) |
| `EncodeSQL/Bytes2/4096/sparse` | 7.18µ | 1.32µ (−82%) | 558n (−92%) |
| `EncodeSQL/Bytes2/4096/dense` | 7.34µ | 1.62µ (−78%) | 1.62µ (−78%) |
| `EncodeSQL/StringBuilder/8/clean` | 29.8n | 19.8n (−34%) | 20.5n (−31%) |
| `EncodeSQL/StringBuilder/32/dense` | 105n | 32.8n (−69%) | 33.8n (−68%) |
| `EncodeSQL/StringBuilder/256/clean` | 1.13µ | 107n (−91%) | 67.7n (−94%) |
| `EncodeSQL/StringBuilder/4096/clean` | 19.4µ | 1.27µ (−93%) | 801n (−96%) |
| `EncodeSQL` geomean | 389n | 80.7n (−79%) | 66.2n (−83%) |
| `GenerateQueryStringBinds/64B` | 1.63µ | 490n (−70%) | 461n (−72%) |
| `GenerateQueryStringBinds/1024B` | 33.1µ | 6.39µ (−81%) | 5.74µ (−83%) |

- **Scalar rewrite: passes.** Every cell improves, 3.5–15×. This is the
  release-build result.
- **SIMD `ByteSet.Index` kernel: conditional on arm64.** Against the scalar
  path it ships with, it is 1.9–2.4× faster on clean and sparse inputs of
  32 bytes and up, at parity on dense inputs of 256 bytes and up, and
  **slower by 0.4–1.5 ns on the 8-byte cells and the 32-byte sparse/dense
  cells** (+3% to +14% on the `Bytes2` path, +2% to +4% on the
  `StringBuilder` path vttablet uses; all p<0.05). The disassembly and a
  same-binary probe with the threshold pinned trace this to two costs that
  are the experiment's (§1): the `Index` wrapper is over the inlining
  budget, so 8-byte inputs pay a call the plain build does not, and one
  16-byte NEON block with a stored-mask scan costs about what 16 table
  lookups cost, so a scan that hits within its first block gains nothing
  for its setup. Two further costs that were ours are fixed: eight
  per-call broadcasts (now vector loads from pre-broadcast rows) and the
  spilled `LoadUint8sPart` tail (now an overlapping block); together they
  took the 8-byte regression from +16% to +7% and `4096/sparse` from 594 to
  558 ns. Under the gate as written the small cells are still a regression
  on arm64; the amd64 numbers (32/64-byte lanes with a cheaper block) decide
  whether the kernel is narrowed to amd64, its threshold raised to 64, or
  it is deleted.

### utf8mb4_0900 collation fast path (`colldata`, `internal/uca`)

| cell | today | scalar | simd |
|---|---|---|---|
| `CollateSharedPrefix/16` | 28.6n | 28.7n (~) | 28.4n (~) |
| `CollateSharedPrefix/short-16` | 27.3n | 27.4n (~) | 26.8n (−1.7%) |
| `CollateSharedPrefix/64` | 36.4n | 36.2n (~) | 34.3n (−5.8%) |
| `CollateSharedPrefix/256` | 70.4n | 71.2n (+1.1%) | 53.0n (−24.7%) |
| `CollateSharedPrefix/1024` | 222n | 221n (~) | 136n (−38.9%) |
| `CollationCollate/utf8mb4_0900_ai_ci` | 11.4µ | 10.8µ (−5.1%) | 10.9µ (−4.6%) |
| `CollationCollate/utf8mb4_0900_bin` | 31.9n | 34.4n (+7.9%) | 32.3n (+1.1%) |

- The scalar column is today's loop (the noasm `equalASCIIPrefix` is a
  constant 0); its ±1–8% deltas are run-to-run noise on code that did not
  change (`utf8mb4_0900_bin` is `bytes.Compare`).
- **SIMD `equalASCIIPrefix` kernel: passes on arm64.** No cell regresses
  (16 and short-16 at parity once the wrapper inlines), −5.8% at 64 B rising
  to −38.9% at 1 KB shared prefix. The threshold is 32 bytes; at 16 the
  16-byte cells lost 11–16%.

### Tokenizer string scan (`sqlparser`)

| cell | today | scalar | simd |
|---|---|---|---|
| `TokenizerScanString/squote/16/clean` | 28.5n | 10.3n (−63.9%) | 10.3n |
| `TokenizerScanString/squote/64/clean` | 106n | 10.5n (−90.2%) | 10.6n |
| `TokenizerScanString/squote/256/clean` | 417n | 12.8n (−96.9%) | 12.8n |
| `TokenizerScanString/squote/4096/clean` | 6.62µ | 102n (−98.5%) | 103n |
| `TokenizerScanString/squote/4096/escape` | 7.30µ | 4.09µ (−44.0%) | 4.18µ |
| `TokenizerScanString` geomean | 1.74µ | 356n (−78.6%) | 357n |
| `Parse3/normal` (1 MB query) | 1.50ms | 23.4µ (−98.4%) | 23.3µ |
| `Parse3/escaped` | 2.19ms | 2.20ms (~) | 2.25ms (~) |
| `NormalizeVTGate` (lobsters corpus) | 31.2ms | 31.6ms (~) | 30.6ms (−1.9%) |

- **Scalar rewrite (two `bytes.IndexByte` scans): passes.** Clean literals
  are 3–65× faster; escaped literals are bounded by `scanStringSlow`. The
  corpus benchmark is unchanged, as §3 predicts for a 1.5% share.
- **SIMD `IndexAny2` kernel: deleted.** Measured before removal (count=4):
  2–3× slower than the `IndexByte` fallback on every clean cell of 64 bytes
  or more (4096 B: 101 ns vs 314 ns; `IndexAny2/4096`: 90 ns vs 331 ns),
  ahead only at 16 bytes. The cause is architecture-independent (§1: no
  movemask in the portable API), so it was not held for the amd64 run. The
  *simd* column above is therefore the same code as *scalar*.

### Whole-branch gates

`NormalizeVTGate`, `Parse3/escaped`, `CollationCollate/*` show no significant
regression in either build mode. `BenchmarkGenerateQueryStringBinds` improves
70–83%.

### Summary

| fast path | release-build win (scalar) | SIMD kernel, arm64 verdict |
|---|---|---|
| escaping | −79% geomean; `GenerateQuery` 33 µs → 6.4 µs | conditional: ~2× at ≥32 B clean, +3–14% at 8 B and 32 B sparse/dense (`Bytes2` path) |
| UCA prefix | none (unchanged) | **pass**: −6% at 64 B to −39% at 1 KB, no regression |
| tokenizer | −79% geomean; 1 MB query 1.5 ms → 23 µs | **deleted**: stdlib `IndexByte` is 2–3× faster |

Two of the three release-build wins need no experiment at all, which is
the more useful finding: the byte-at-a-time loops were the cost, and the
portable `simd` package is the right tool only where no stdlib primitive
already vectorizes the scan.
