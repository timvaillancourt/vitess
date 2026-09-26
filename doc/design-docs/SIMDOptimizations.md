# SIMD fast paths on the Vitess hot query path

## Summary

Go is growing native SIMD support: the `simd` and `simd/archsimd` packages,
behind `GOEXPERIMENT=simd` since Go 1.26. This document ranks the Vitess code
paths that could use it, by measured CPU share on the per-query path, and
records a benchmark-gated prototype of the top three:

1. **Bind-variable escaping** (`sqltypes.encodeBytesSQL*`): vttablet runs it
   through `ParsedQuery.GenerateQuery` on every query with a string or binary
   bind variable.
2. **Tokenizer string-literal scanning** (`sqlparser.(*Tokenizer).scanString`):
   vtgate runs it for every literal in every query it parses.
3. **utf8mb4_0900 collation fast path**
   (`uca.(*FastIterator900).FastForward32`): vtgate runs it for ORDER BY,
   GROUP BY, DISTINCT and hash joins once the tiny-weight comparison ties.

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
| `(*Tokenizer).scanString` + `peek` | `Parse3`, `NormalizeVTGate` | 8.7 + 23.9 | **38.5** | `peek` is the bounds-checked one-byte read `scanString` does per byte. `scanStringSlow` (escapes) is a further 7.0% cum. |
| `(*Collation_utf8mb4_uca_0900).Collate` | `CollationCollate`, `CollateSharedPrefix` | 1.6 | **18.4** | `FastForward32` is 6.8% flat inside it; `NextWeightBlock64` 5.1% cum. |
| `encodeBytesSQLBytes2` / `encodeBytesSQLStringBuilder` | `EncodeSQL` | 29.8 / 7.2 | 44.0 / 11.7 | Self-benchmark, so the share is of the encoder alone. Within `GenerateQueryStringBinds` the encoder is ~100% of the substitution cost. |
| `(*Tokenizer).scanIdentifier`, `LookupString` | `Parse3`, `NormalizeVTGate` | 0 | 0.5, 1.1 | Identifiers are too short to vectorize. |
| planbuilder, evalengine, engine candidates | `OLTP/TPCC/TPCH`, `CompilerExpressions`, `ScalarAggregate` | — | — | Profiles are allocation-dominated (`mallocgc` 6%, `memmove` 1%); no byte-loop candidate appears above 0.5%. |

The ranking puts the tokenizer first, not third as estimated before profiling:
on a real query corpus the byte-at-a-time hunt for the closing quote is the
single largest leaf in parsing. Bind-variable escaping is ranked by call
frequency (every vttablet query with string binds) rather than by share of a
parse benchmark, since no existing benchmark covered it.

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
path side by side.

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
   are thin wrappers (§1 compiler limitation). Broadcasts happen inside the
   function, never in package-level `var`s. Inputs shorter than 16 bytes take
   the scalar path. Partial loads (`LoadUint8sPart`) zero-fill, so a hit in a
   lane at or past the returned count is discarded: `0x00` is a member of the
   SQL escape set.
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
workflow for the SIMD hot-path candidates"). arm64 numbers are local (Apple
M-series, `-count=10`); amd64 numbers come from the `simd_experiment`
workflow artifacts and are filled in when that run exists.

_(Filled in by the final commit of the branch.)_
