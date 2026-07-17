# Contribution #2: PySpark DataFrame Doctest: randomSplit

**Contribution Number:** 2
**Student:** John Nonye
**Issue:** [PySpark DataFrame Doctest: randomSplit · Issue #483 · lakehq/sail](https://github.com/lakehq/sail/issues/483)
**Status:** Phase II — In Progress

---

## Why I Chose This Issue

I chose this issue because it closely mirrors the type of contribution I made in my first cycle — adding missing functionality that the codebase acknowledges but hasn't yet implemented. Just as `ivre` had a TODO comment marking missing Nmap fields, this issue involves adding doctest coverage for a PySpark DataFrame function (`randomSplit`) that exists in the API but lacks verified test documentation in Sail's test suite.

This issue also gives me an opportunity to learn a new codebase (Sail — a Rust-native Spark replacement) while working in Python, which is my strongest language. I'm interested in understanding how Sail achieves PySpark compatibility and what it takes to verify behavioral parity between Sail and the original Spark implementation.

---

## Understanding the Issue

### Problem Description

The `randomSplit` method on PySpark DataFrames is part of the standard PySpark API. Sail aims for PySpark compatibility, and its own copy of the PySpark source includes a doctest for `randomSplit` (in `pyspark/sql/dataframe.py`) with a fixed seed and expected output. The issue tracks getting Sail's engine to actually produce output matching that doctest.

**Note on scope (updated after investigation):** this turned out not to be a missing-doctest problem — the doctest already exists in the PySpark source Sail ships. The real problem is that **Sail's sampling engine produces incorrect results** against that doctest, so the fix is a behavioral/engine bug, not a documentation gap.

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

Running this exact example against Sail's engine produces `splits[0].count() == 1` and `splits[1].count() == 2` → **actually `1` and `3`** (see Reproduction Evidence below) instead of the expected `2` and `2`. The split *boundaries* are computed correctly, but individual rows are assigned to the wrong bucket.

### Affected Components

- `crates/sail-plan/src/resolver/query/sample.rs` — logical plan resolution for `Sample`
- `crates/sail-function/src/scalar/math/random.rs` — the `Random` scalar UDF used to generate per-row random values
- `crates/sail-function/src/scalar/math/xorshift.rs` — `SparkXorShiftRandom`, the underlying RNG (confirmed correct, not the bug)
- Likely needs a new file for a custom physical operator (e.g. `sample_exec.rs`) in `crates/sail-execution/src/plan/`

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

**Takeaway for future contributors:** a `SIGTERM`/exit 101 error compiling `aws-sdk-glue` (or similar large crates) is very likely a memory ceiling, not a code issue. Recommend at least 16GB RAM for a full Sail build.

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

- **My findings:** Server debug logs show the `Sample` plan nodes have correct normalized bounds (`0.0–0.333...` and `0.333...–1.0` for weights `[1.0, 2.0]`), which rules out a weight-normalization bug. The discrepancy is specifically in *which rows* get assigned to which bucket — pointing to the row-level random-sampling logic rather than the plan/boundary math.

---

## Solution Approach

### Analysis

The issue's maintainer (`SparkApplicationMaster`) left a detailed comment tracing Spark's actual algorithm for `randomSplit`:
1. Spark Connect builds a `Sample` plan per split interval (✅ Sail has this)
2. Applies `sortWithinPartitions` for deterministic row order (❌ not fully wired up)
3. Physical `SampleExec` calls a **`BernoulliCellSampler`**, seeded with `seed + partitionIndex` (❌ missing in Sail)
4. That sampler uses **`XORShiftRandom`** to generate a per-row keep/drop decision (RNG exists in Sail, but isn't used this way)
5. `XORShiftRandom` hashes its seed with **MurmurHash3** (✅ Sail already has a compatible implementation)

I verified this against the current codebase (the maintainer's linked commit was outdated):

- `crates/sail-function/src/scalar/math/xorshift.rs` — `SparkXorShiftRandom` is a correct, already-implemented port of Spark's RNG.
- `crates/sail-function/src/scalar/math/random.rs` (line 69) — the `Random` UDF seeds `SparkXorShiftRandom::new(seed)` using **only the literal seed**, with no partition index at all.
- `crates/sail-plan/src/resolver/query/sample.rs` — builds sampling as a logical-plan **row filter** (`filter(rand_column between bounds)`), which has no way to know which partition a row belongs to.
- Confirmed directly in DataFusion's source (`datafusion-expr` v54.0.0, `udf.rs`): `ScalarFunctionArgs` (the struct passed into any scalar UDF) only exposes `args`, `arg_fields`, `number_rows`, `return_field`, `config_options` — **no partition index**. This rules out fixing this inside a plain scalar UDF.
- `crates/sail-execution/src/plan/shuffle_read.rs` (line 73–76) — Sail already has custom `ExecutionPlan` implementations where `fn execute(&self, partition: usize, ...)` receives the partition index directly. This is the correct pattern to follow.

**Conclusion:** this requires a new physical operator, not a UDF tweak, because only a physical `ExecutionPlan` gets access to `partition: usize` during execution.

### Proposed Solution

Implement a new physical operator (e.g. `BernoulliSampleExec`) that receives the partition index directly, seeds `SparkXorShiftRandom` with `seed + partition_index` (matching Spark's `BernoulliCellSampler`), and makes a sequential per-row keep/drop decision as each partition streams through — replacing the current row-filter approach in `sample.rs` for the without-replacement sampling path used by `randomSplit`.

### Implementation Plan

Using UMPIRE framework:

**Understand:** `DataFrame.randomSplit([1.0, 2.0], seed=24)` should deterministically assign rows to buckets using a per-partition, per-row seeded random sequence identical to Spark's `BernoulliCellSampler` + `XORShiftRandom`. Sail currently diverges from Spark's row assignment (reproduced: `1`/`3` instead of `2`/`2`), even though split boundaries are correct.

**Match:**
- `crates/sail-function/src/scalar/math/xorshift.rs` — reusable, correct RNG
- `crates/sail-function/src/scalar/math/random.rs` — shows the bug (seed-only, no partition awareness)
- `crates/sail-plan/src/resolver/query/sample.rs` — current row-filter architecture, structurally unable to access partition index
- `crates/sail-execution/src/plan/shuffle_read.rs` / `shuffle_write.rs` — existing custom `ExecutionPlan` implementations to model the fix after; confirmed `execute(&self, partition: usize, ...)` gives direct partition access

**Plan:**
1. Design a new physical operator (e.g. `BernoulliSampleExec`) implementing `ExecutionPlan`, modeled after `shuffle_read.rs`/`shuffle_write.rs`
2. Seed `SparkXorShiftRandom::new(seed + partition_index as i64)` once per partition inside `execute()`
3. Generate a sequential per-row keep/drop decision as each `RecordBatch` streams through the partition — RNG state must persist across batches within a partition, not reset per batch
4. Replace the `filter(rand_column between bounds)` logic in `resolve_sample_without_replacement` (`sample.rs`) with a call building this new operator
5. Leave `resolve_sample_with_replacement` (Poisson/with-replacement sampling) untouched — out of scope for this issue

**Implement:** Branch created: [`fix-issue-483`](https://github.com/chjohn5577/sail/tree/fix-issue-483). Code changes pending — to be completed in Phase III.

**Review:** Checked `CONTRIBUTING.md` and `.github/workflows/pull-request-validation.yml`. Sail enforces Conventional Commits-style PR titles via CI (`amannn/action-semantic-pull-request`):
- Format: `type(scope): subject`
- Type for this fix: `fix`; Scope: `cargo`
- Subject must not start with an uppercase letter
- Planned PR title: `fix(cargo): match spark's per-partition bernoulli sampling in randomSplit`

**Evaluate:** Re-run the reproduction script — should print `2` and `2` after the fix. Run the project's doctest suite targeting this test specifically:
```bash
hatch run test-spark.spark-4.1.1:env \
  TEST_RUN_NAME=selected \
  scripts/spark-tests/run-tests.sh \
  --pyargs pyspark.sql.dataframe -v -k randomSplit
```
Also check for regressions in any other tests referencing `Random`, sampling, or `randomSplit`.

---

## Testing Strategy

### Unit Tests

- [ ] Test case 1: `randomSplit` with weights summing to 1.0 produces correct split sizes matching Spark's doctest exactly
- [ ] Test case 2: Fixed seed produces deterministic output across repeated runs
- [ ] Test case 3: Resulting DataFrames together contain all rows from the original, with no duplicates or omissions

### Integration Tests

- [ ] Confirm the `randomSplit` doctest passes within Sail's full `doctest-dataframe` test suite run
- [ ] Confirm no regressions in existing `Random`/`RandPoisson`/sampling-related tests

### Manual Testing

[ To be completed in Phase III, using `/tmp/repro_483.py` as the manual verification script ]

---

## Implementation Notes

### Week 5 Progress

- Selected issue #483 in `lakehq/sail`
- Confirmed issue is unclaimed (no assignee, no linked PR)
- Completed Phase I: issue selected, README created
- Completed environment setup (Step 1) — including a real infrastructure blocker (Rust OOM compiling `aws-sdk-glue`), resolved by upgrading Codespace machine size
- Created working branch `fix-issue-483` (Step 2)
- Reproduced the bug consistently across 3 runs (Step 3): Sail produces `1`/`3` split counts instead of the expected `2`/`2`
- Traced root cause to `crates/sail-function/src/scalar/math/random.rs` and confirmed (via DataFusion's `ScalarFunctionArgs` source) that a scalar-UDF-only fix is architecturally impossible; the correct fix requires a new physical `ExecutionPlan` operator, following the pattern in `shuffle_read.rs`/`shuffle_write.rs` (Step 4)

### Code Changes

- **Files modified:** [ To be added in Phase III ]
- **Key commits:** [ To be added ]
- **Approach decisions:** Chose to implement sampling as a custom physical operator rather than modifying the existing scalar UDF, since only `ExecutionPlan::execute()` exposes the partition index needed to match Spark's per-partition seeding behavior.

---

## Pull Request

**PR Link:** [ To be added when submitted ]

**PR Description:** [ To be drafted in Phase IV ]

**Maintainer Feedback:**
- [ To be added as feedback is received ]

**Status:** Phase II complete (setup, branch, reproduction, root-cause analysis, and solution plan) — awaiting Phase III implementation

---

## Learnings & Reflections

### Technical Skills Gained

[ To be completed at end of cycle ]

### Challenges Overcome

- Diagnosed and resolved a genuine infrastructure limit (Rust compilation OOM-killed on a memory-constrained Codespace) by isolating the cause methodically — confirming it wasn't a job-count/parallelism issue before escalating to a machine upgrade
- Navigated an unfamiliar Rust workspace (multi-crate, Hatch + Maturin + pnpm) to trace a bug from a Python-level symptom down to a specific missing Rust struct/trait implementation

### What I'd Do Differently Next Time

[ To be completed at end of cycle ]

---

## Resources Used

- [lakehq/sail repository](https://github.com/lakehq/sail)
- [Issue #483](https://github.com/lakehq/sail/issues/483)
- [PySpark DataFrame.randomSplit() docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.randomSplit.html)
- [Sail Development Guide](https://docs.lakesail.com/sail/main/development/)
- [Sail Spark Setup docs](https://docs.lakesail.com/sail/main/development/spark-tests/spark-setup.html)