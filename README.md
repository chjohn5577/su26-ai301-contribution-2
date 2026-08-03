# Contribution #2: PySpark DataFrame Doctest: randomSplit

**Contribution Number:** 2
**Student:** John Nonye
**Issue:** [PySpark DataFrame Doctest: randomSplit · Issue #483 · lakehq/sail](https://github.com/lakehq/sail/issues/483)
**Status:** Phase III — In Progress

---

## Why I Chose This Issue

I chose this issue because it closely mirrors the type of contribution I made in my first cycle — adding missing functionality that the codebase acknowledges but hasn't yet implemented. Just as `ivre` had a TODO comment marking missing Nmap fields, this issue involves adding doctest coverage for a PySpark DataFrame function (`randomSplit`) that exists in the API but lacks verified test documentation in Sail's test suite.

This issue also gives me an opportunity to learn a new codebase (Sail — a Rust-native Spark replacement) while working in Python, which is my strongest language. I'm interested in understanding how Sail achieves PySpark compatibility and what it takes to verify behavioral parity between Sail and the original Spark implementation.

---

## Understanding the Issue

### Problem Description

The `randomSplit` method on PySpark DataFrames is part of the standard PySpark API. Sail aims for PySpark compatibility, and its own copy of the PySpark source includes a doctest for `randomSplit` (in `pyspark/sql/dataframe.py`) with a fixed seed and expected output. The issue tracks getting Sail's engine to actually produce output matching that doctest.

**Note on scope (updated after investigation):** this turned out not to be a missing-doctest problem — the doctest already exists in the PySpark source Sail ships. The real problem is that **Sail's sampling engine produces incorrect results** against that doctest, so the fix is a behavioral/engine bug, not a documentation gap.

**Second update, after deeper investigation (see "Root Cause" below):** the discrepancy turned out **not** to be in the sampling/RNG algorithm at all — it's that Sail always creates exactly one partition for locally-created DataFrames, while real Spark spreads them across multiple partitions by default. See the full corrected root-cause writeup further down.

### Expected Behavior

Per the doctest in `pyspark/sql/dataframe.py::DataFrame.randomSplit`:

```python
>>> df = spark.createDataFrame([
...     Row(age=10, height=80, name="Alice"),
...     Row(age=5, height=None, name="Bob"),
...     Row(age=None, height=None, name="Tom"),
...     Row(age=None, height=None, name=None),
... ])
>>> splits = df.randomSplit([1.0, 2.0], 24)
>>> splits[0].count()
2
>>> splits[1].count()
2
```

### Current Behavior

Running this exact example against Sail's engine produces `splits[0].count() == 1` and `splits[1].count() == 3` instead of the expected `2` and `2`.

### Affected Components (updated)

- `crates/sail-plan/src/resolver/query/misc.rs`, function `resolve_query_local_relation` — **this is the actual root cause.** `createDataFrame` always builds a single-partition `MemTable`.
- `crates/sail-physical-plan/src/bernoulli_sample.rs` — new physical operator (built this cycle) that will replace the current row-filter sampling approach; needed once multiple partitions exist, since partition-aware RNG seeding only matters at that point.
- `crates/sail-plan/src/resolver/query/sample.rs` — logical plan resolution for `Sample`; will be updated to call the new operator instead of the current `filter(rand_column between bounds)` approach.
- `crates/sail-function/src/scalar/math/xorshift.rs` — `SparkXorShiftRandom`, the underlying RNG (confirmed correct, reused as-is).
- `crates/sail-function/src/scalar/math/random.rs` — the original `Random` scalar UDF; not the actual bug, but architecturally can't be partition-aware (see analysis below).

---

## Reproduction Process

### Environment Setup

- **Environment:** GitHub Codespaces, Windows host, VS Code (browser)
- Sail includes a `devcontainer.json`, so setup used the Dev Containers workflow.
- Ran `npm install` initially — succeeded, but the project actually specifies `pnpm` (`"packageManager": "pnpm@10.12.3"` in `package.json`). Reinstalled correctly with `pnpm install`.
- Confirmed the docs site (VitePress) built and ran via `pnpm run docs:dev` — served at `localhost:5173/sail/main/`.
- Built the Rust engine with `hatch run maturin develop`.

**Setup issues encountered:**

| Problem | Fix |
|---|---|
| `package.json` scripts suggested `npm`, but project uses `pnpm` | Confirmed via `"packageManager"` field; reinstalled with `pnpm install` |
| No `.env.example` file present | Not needed for this project's dev setup |
| No `test` script in `package.json` | Project is a VitePress docs site; `docs:dev` is the real verification step |
| `maturin develop` killed with `SIGTERM` (exit 101) compiling `aws-sdk-glue` | Root cause: Codespace machine was 2-core/8GB — insufficient RAM for this crate. `CARGO_BUILD_JOBS=1` did **not** fix it; this was a genuine RAM ceiling, not a parallelism issue |
| Rust build repeatedly OOM-killed | Upgraded Codespace machine type (GitHub Codespaces → Change machine type) to 16-core/62GB. Build then completed successfully in ~7m32s |
| Codespace stopped mid-session after machine upgrade | Reopening the Codespace resumed the environment with all files/branch state intact — no data lost |
| `target/` grew to 104GB after multiple full rebuilds, disk hit 99% full mid-Phase-III | Ran `cargo clean` (freed ~103GB). **Lesson for future contributors:** watch `df -h` during heavy Rust rebuild cycles in Codespaces — this can happen fast with a large multi-crate workspace |

**Takeaway for future contributors:** a `SIGTERM`/exit 101 error compiling `aws-sdk-glue` (or similar large crates) is very likely a memory ceiling, not a code issue. Recommend at least 16GB RAM for a full Sail build, and keep an eye on disk usage across repeated rebuilds.

### Steps to Reproduce

1. Start Sail's Spark Connect server (built from source):
   ```bash
   hatch run scripts/spark-tests/run-server.sh
   ```
   Confirms it's listening on `127.0.0.1:50051`.

2. In a separate terminal, connect via `pyspark` and run the doctest's exact code:
   ```python
   from pyspark.sql import SparkSession, Row

   spark = SparkSession.builder.remote("sc://localhost:50051").getOrCreate()

   df = spark.createDataFrame([
       Row(age=10, height=80, name="Alice"),
       Row(age=5, height=None, name="Bob"),
       Row(age=None, height=None, name="Tom"),
       Row(age=None, height=None, name=None),
   ])

   splits = df.randomSplit([1.0, 2.0], 24)
   print("Split 0 count:", splits[0].count())
   print("Split 1 count:", splits[1].count())
   ```

3. Repeated the run twice more to confirm the result is deterministic, not flaky.

### Reproduction Evidence

- **Branch:** [`fix-issue-483`](https://github.com/chjohn5577/sail/tree/fix-issue-483)
- **Results:**

  | | Expected (per doctest) | Actual (Sail) — 3 runs, identical |
  |---|---|---|
  | Split 0 count | 2 | **1** |
  | Split 1 count | 2 | **3** |

- **My findings:** Server debug logs show the `Sample` plan nodes have correct normalized bounds (`0.0–0.333...` and `0.333...–1.0` for weights `[1.0, 2.0]`), which rules out a weight-normalization bug. Checking partition assignment directly (via `spark_partition_id()`), all 4 rows landed on `pid=0` — confirming Sail runs this scenario on a **single partition**.

---

## Solution Approach

### Analysis (fully revised after ground-truth verification)

The issue's maintainer (`SparkApplicationMaster`) left a detailed comment tracing Spark's real algorithm for `randomSplit`:
1. Spark Connect builds a `Sample` plan per split interval (✅ Sail has this)
2. Applies `sortWithinPartitions` for deterministic row order within each partition
3. Physical `SampleExec` calls `randomSampleWithRange`, which creates a **`BernoulliCellSampler`** seeded with `seed + partitionIndex` per partition
4. That sampler uses **`XORShiftRandom`** to generate a per-row keep/drop decision
5. `XORShiftRandom` hashes its seed with **MurmurHash3** (✅ Sail already has a compatible implementation)

**Initial hypothesis (later disproven):** I first assumed the bug was that Sail's `Random` UDF (`crates/sail-function/src/scalar/math/random.rs`) seeds `SparkXorShiftRandom` with only the literal seed, with no partition index at all — and that a scalar UDF architecturally can't access partition index anyway (confirmed via DataFusion's `ScalarFunctionArgs` struct, which exposes only `args`, `arg_fields`, `number_rows`, `return_field`, `config_options`). This led me to build `BernoulliSampleExec`, a new physical operator (modeled after `shuffle_read.rs`/`shuffle_write.rs`/`RelaxedTzCastExec`), since only `ExecutionPlan::execute()` receives `partition: usize` directly.

**Dead end encountered and corrected:** Testing `BernoulliSampleExec` with `seed + partition_index` seeding still produced `1`/`3`, identical to the original bug — even though the operator itself worked correctly. I hypothesized Spark's *real* per-partition seed derivation used sequential `java.util.Random(seed).nextLong()` draws (the algorithm behind `RDD.sample()` / `PartitionwiseSampledRDD`, a *different* Spark code path than `randomSplit`). I implemented and independently verified a `next_i64()` method on `crates/sail-function/src/scalar/math/java_random.rs` (matching real Java's `nextLong()` bit-for-bit, confirmed against a known reference value: `new java.util.Random(0).nextLong() == -4962768465676381896`). Wiring this in produced the **exact same** `1`/`3` result.

**Resolution — went back to ground truth:** Rather than continuing to theorize, I built patched real Apache Spark 4.1.1 from source (`scripts/spark-tests/build-pyspark.sh`) and ran the identical doctest scenario against it directly:
- Forcing `master("local[1]")` (single partition, to isolate the seeding math): **real Spark also produced `1`/`3`** — identical to Sail's original, unmodified behavior. This proved the per-partition seed *derivation* was never the actual bug; the `java_random.rs` detour was solving the wrong Spark code path.
- Running with Spark's natural default (no forced master): **16 partitions** (matching this machine's CPU core count), with the 4 rows landing on 4 different partitions (`pid` 3, 7, 11, 15) — and produced the doctest's expected **`2`/`2`**.

**Actual root cause, confirmed:** `crates/sail-plan/src/resolver/query/misc.rs`, function `resolve_query_local_relation`:
```rust
let table_provider = Arc::new(MemTable::try_new(schema, vec![batches])?);
```
`MemTable::try_new`'s second argument is `Vec<Vec<RecordBatch>>`, where the **outer** `Vec` represents partitions. Wrapping everything in `vec![batches]` means `createDataFrame` **always** produces exactly one partition, regardless of row count or the `target_partitions`/`default_parallelism` session config (which does exist and is wired up in `crates/sail-session/src/session_factory/server.rs`, but never consulted here). Real Spark spreads locally-created data across `defaultParallelism` partitions immediately at creation. With only 1 partition, the fixed 4-value RNG sequence for seed 24 can only ever produce a `1`/`3` split, mathematically, regardless of row order or exact seed-derivation formula — which is why every seeding variant I tried produced the identical wrong answer. The real lever was partition **count**, not partition **seed derivation**.

### Proposed Solution (corrected)

Two-part fix:
1. Fix `resolve_query_local_relation` to distribute `batches` across multiple partitions (matching `target_partitions`/`default_parallelism`, sensibly capped by row count) instead of the current hardcoded single partition.
2. Revert `BernoulliSampleExec`'s per-partition seed to the simple `seed.wrapping_add(partition as i64)` (matching `randomSampleWithRange`'s real algorithm, per the maintainer's comment — the `java_random.rs` detour, while independently correct, isn't the right formula for this code path), then wire it into `sample.rs`'s `resolve_sample_without_replacement`, replacing the current single-partition row-filter approach. Partition-aware seeding only matters once multiple partitions genuinely exist.

### Implementation Plan

Using UMPIRE framework:

**Understand:** `DataFrame.randomSplit([1.0, 2.0], seed=24)` should deterministically assign rows to buckets matching real Spark's behavior. The actual gap is that Sail's `createDataFrame` always produces a single partition, while real Spark distributes across `defaultParallelism` partitions — and this partition count, not the RNG seeding formula, is what determines the split outcome for small datasets like the doctest's 4 rows.

**Match:**
- `crates/sail-plan/src/resolver/query/misc.rs` (`resolve_query_local_relation`) — the actual root cause; hardcodes `vec![batches]` (one partition) regardless of config.
- `crates/sail-session/src/session_factory/server.rs` (line 212-214) — `target_partitions` config already exists and is applied to DataFusion's `SessionConfig`, but not consulted by `resolve_query_local_relation`.
- `crates/sail-function/src/scalar/math/xorshift.rs` — reusable, correct RNG.
- `crates/sail-physical-plan/src/bernoulli_sample.rs` (built this cycle) — new physical operator, correctly structured (modeled after `RelaxedTzCastExec`/`SparkPartitionIdExec`), seed formula needs reverting to `seed + partition_index`.
- `crates/sail-function/src/scalar/math/java_random.rs` — `next_i64()` added and independently verified correct, but not needed for this specific fix (solves a different Spark code path). Kept in the codebase as a correct, tested utility.

**Plan:**
1. Fix `resolve_query_local_relation` to split `batches` across multiple partitions based on `target_partitions` (capped by row count — e.g. `min(target_partitions, num_rows)`, matching Spark's own capping behavior for small datasets).
2. Revert `BernoulliSampleExec`'s seed logic to `seed.wrapping_add(partition as i64)`.
3. Wire `BernoulliSampleExec` into `resolve_sample_without_replacement` in `sample.rs`, replacing the current row-filter (`filter(rand_column between bounds)`) approach.
4. Leave `resolve_sample_with_replacement` (Poisson/with-replacement sampling) untouched — out of scope for this issue.
5. Re-verify against both the reproduction script and real Spark's actual behavior (already confirmed as ground truth this session).

**Implement:** Branch: [`fix-issue-483`](https://github.com/chjohn5577/sail/tree/fix-issue-483). `BernoulliSampleExec` skeleton and filtering logic implemented; seed formula and `resolve_query_local_relation` partition-splitting fix remain, targeted for next session.

**Review:** Checked `CONTRIBUTING.md` and `.github/workflows/pull-request-validation.yml`. Sail enforces Conventional Commits-style PR titles via CI (`amannn/action-semantic-pull-request`):
- Format: `type(scope): subject`
- Type for this fix: `fix`; Scope: `cargo`
- Subject must not start with an uppercase letter
- Planned PR title: `fix(cargo): distribute local relations across multiple partitions to match spark parallelism`
- Code style: `cargo +nightly fmt -- --check` (nightly toolchain required) and `cargo clippy --all-targets --all-features -- -D warnings` (all warnings are errors), enforced via `.github/workflows/rust-lint.yml`.
- Testing: project uses `cargo nextest run --no-fail-fast`, not plain `cargo test`. Baseline confirmed clean: 1002/1002 tests passing before starting Phase III changes.

**Evaluate:** Re-run the reproduction script — should print `2` and `2` after the fix. Run the project's doctest suite targeting this test specifically:
```bash
hatch run test-spark.spark-4.1.1:env \
  TEST_RUN_NAME=selected \
  scripts/spark-tests/run-tests.sh \
  --pyargs pyspark.sql.dataframe -v -k randomSplit
```
Also re-run the full `cargo nextest run --no-fail-fast` suite to check for regressions, since changing `resolve_query_local_relation`'s partition count could affect other tests relying on single-partition behavior for local DataFrames.

---

## Testing Strategy

### Unit Tests

- [ ] Test case 1: `randomSplit` with weights summing to 1.0 produces correct split sizes matching Spark's doctest exactly, when rows are distributed across multiple partitions
- [ ] Test case 2: Fixed seed produces deterministic output across repeated runs
- [ ] Test case 3: Resulting DataFrames together contain all rows from the original, with no duplicates or omissions
- [ ] Test case 4: `resolve_query_local_relation` produces the correct number of partitions for varying row counts and `target_partitions` settings (including edge cases: 0 rows, 1 row, more partitions requested than rows)

### Integration Tests

- [ ] Confirm the `randomSplit` doctest passes within Sail's full `doctest-dataframe` test suite run
- [ ] Confirm no regressions in existing tests that depend on `createDataFrame`'s current single-partition behavior (if any)
- [ ] Confirm no regressions in existing `Random`/`RandPoisson`/sampling-related tests

### Manual Testing

Using `/tmp/repro_483.py` as the manual verification script — confirmed it currently reproduces `1`/`3`; will re-run after the fix to confirm `2`/`2`.

---

## Implementation Notes

### Week 5 Progress

- Selected issue #483 in `lakehq/sail`
- Confirmed issue is unclaimed (no assignee, no linked PR)
- Completed Phase I: issue selected, README created
- Completed environment setup (Step 1) — including a real infrastructure blocker (Rust OOM compiling `aws-sdk-glue`), resolved by upgrading Codespace machine size
- Created working branch `fix-issue-483` (Step 2)
- Reproduced the bug consistently across 3 runs (Step 3): Sail produces `1`/`3` split counts instead of the expected `2`/`2`
- Initial (later corrected) root-cause analysis pointed to missing partition-aware RNG seeding (Step 4)

### Phase III Progress — Session: Root Cause Investigation & Correction

- Synced branch with upstream (`git fetch origin && git rebase origin/main`) — clean, no conflicts.
- Reviewed contribution requirements before writing code: code style (`cargo +nightly fmt`, `cargo clippy -D warnings`), testing (`cargo nextest run --no-fail-fast`), commit/PR conventions (Conventional Commits via `pull-request-validation.yml`). Confirmed clean baseline: 1002/1002 tests passing.
- Scaffolded `BernoulliSampleExec` in `crates/sail-physical-plan/src/bernoulli_sample.rs`, modeled after `RelaxedTzCastExec`, `SparkPartitionIdExec`, and `MapPartitionsExec`. Added `sail-function` as a new dependency of `sail-physical-plan` (verified no circular dependency first).
- **Dead end:** naive `seed + partition_index` seeding, then a `java.util.Random`-based per-partition seed derivation (`next_i64()`, added to `java_random.rs` and independently verified against a real Java reference value) — both produced the identical `1`/`3` result as the original bug.
- Ran out of disk space mid-session (`target/` grew to 104GB); resolved with `cargo clean` (freed ~103GB).
- Built patched real Apache Spark 4.1.1 from source to get ground truth. Confirmed: with single partition forced, real Spark also produces `1`/`3` (proving the seed-derivation detour was solving the wrong problem). With Spark's natural default (16 partitions on this machine), real Spark produces the doctest's expected `2`/`2`.
- **Found the actual root cause:** `resolve_query_local_relation` in `crates/sail-plan/src/resolver/query/misc.rs` hardcodes single-partition `MemTable` construction (`vec![batches]`), regardless of `target_partitions` config.
- Corrected the plan accordingly (see "Solution Approach" above). Remaining work: implement the partition-splitting fix, revert `BernoulliSampleExec`'s seed formula, and wire the operator into `sample.rs`.

### Code Changes

- **Files modified so far:**
  - `crates/sail-physical-plan/src/bernoulli_sample.rs` (new file — `BernoulliSampleExec` operator, seed formula to be reverted)
  - `crates/sail-physical-plan/src/lib.rs` (module registration)
  - `crates/sail-physical-plan/Cargo.toml` (added `sail-function` dependency)
  - `crates/sail-function/src/scalar/math/java_random.rs` (added `next_i64()` — correct and tested, though not needed for the final fix)
- **Key commits:** [ To be added ]
- **Approach decisions:**
  - Chose to implement sampling as a custom physical operator rather than modifying the existing scalar UDF, since only `ExecutionPlan::execute()` exposes the partition index.
  - Chose to verify against real, patched Apache Spark rather than continuing to reason from documentation/code comments alone, once two independent seeding hypotheses both failed to reproduce the doctest's expected output — this was the decision that actually resolved the investigation.

---

## Pull Request

**PR Link:** [ To be added when submitted ]

**PR Description:** [ To be drafted in Phase IV ]

**Maintainer Feedback:**
- [ To be added as feedback is received ]

**Status:** Root cause fully confirmed via ground-truth verification against real Apache Spark. Implementation in progress — partition-splitting fix and `BernoulliSampleExec` wiring remain for next session.

---

## Learnings & Reflections

### Technical Skills Gained

- Practical experience implementing a custom DataFusion `ExecutionPlan` operator from scratch, following existing codebase patterns.
- Learned to verify a hypothesis against ground truth (building and running real Apache Spark from source) rather than continuing to reason indefinitely from documentation and code comments — this was the single action that actually resolved a multi-hour investigation.

### Challenges Overcome

- Diagnosed and resolved a genuine infrastructure limit (Rust compilation OOM-killed on a memory-constrained Codespace) by isolating the cause methodically — confirming it wasn't a job-count/parallelism issue before escalating to a machine upgrade.
- Navigated an unfamiliar Rust workspace (multi-crate, Hatch + Maturin + pnpm) to trace a bug from a Python-level symptom down to a specific line of code.
- Followed a plausible, well-evidenced hypothesis (per-partition RNG seed derivation) into a genuine dead end — twice — and recognized when to stop theorizing and instead build real Spark to get a ground-truth answer, which is what actually cracked the investigation.
- Managed a disk-space crisis mid-session (104GB of build artifacts) without losing progress.

### What I'd Do Differently Next Time

- Would build and check against real Spark earlier in the investigation, rather than after two failed seeding hypotheses — it's a slower first step (~15-30 min build) but would have saved significant time overall by establishing ground truth before theorizing about Sail's internals.

---

## Resources Used

- [lakehq/sail repository](https://github.com/lakehq/sail)
- [Issue #483](https://github.com/lakehq/sail/issues/483)
- [PySpark DataFrame.randomSplit() docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.randomSplit.html)
- [Sail Development Guide](https://docs.lakesail.com/sail/main/development/)
- [Sail Spark Setup docs](https://docs.lakesail.com/sail/main/development/spark-tests/spark-setup.html)