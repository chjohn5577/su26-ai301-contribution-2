# Contribution #2: PySpark DataFrame Doctest: randomSplit

**Contribution Number:** 2  
**Student:** John Nonye  
**Issue:** [PySpark DataFrame Doctest: randomSplit · Issue #483 · lakehq/sail](https://github.com/lakehq/sail/issues/483)  
**Status:** Phase I — In Progress

---

## Why I Chose This Issue

I chose this issue because it closely mirrors the type of contribution I made in my first cycle — adding missing functionality that the codebase acknowledges but hasn't yet implemented. Just as `ivre` had a TODO comment marking missing Nmap fields, this issue involves adding doctest coverage for a PySpark DataFrame function (`randomSplit`) that exists in the API but lacks verified test documentation in Sail's test suite.

This issue also gives me an opportunity to learn a new codebase (Sail — a Rust-native Spark replacement) while working in Python, which is my strongest language. I'm interested in understanding how Sail achieves PySpark compatibility and what it takes to verify behavioral parity between Sail and the original Spark implementation.

---

## Understanding the Issue

### Problem Description

The `randomSplit` method on PySpark DataFrames is part of the standard PySpark API, but Sail currently lacks a doctest verifying that this method works correctly and produces output consistent with PySpark's behavior.

### Expected Behavior

A doctest for `randomSplit` should exist that demonstrates the function splitting a DataFrame into multiple parts according to provided weights, and the output should match what PySpark produces.

### Current Behavior

No doctest exists for `randomSplit` in Sail's test suite — the function may or may not be implemented, but its behavior is unverified and undocumented at the doctest level.

### Affected Components

- The PySpark DataFrame doctest files in the Sail test suite
- Likely under `python/pyspark/` or a `tests/` directory containing DataFrame API doctests

---

## Reproduction Process

### Environment Setup

[ To be completed in Phase II ]

### Steps to Reproduce

[ To be completed in Phase II ]

### Reproduction Evidence

- **Branch:** [ To be added ]
- **Screenshots/logs:** [ To be added ]
- **My findings:** [ To be added ]

---

## Solution Approach

### Analysis

[ To be completed after exploring the codebase in Phase II ]

### Proposed Solution

At a high level: locate where existing PySpark DataFrame doctests live in the Sail repo, study the pattern used for similar doctests (e.g. `sameSemantics`, `randomSplit`'s neighbors in the API), and add a doctest for `randomSplit` that demonstrates correct behavior with a fixed seed and known weights.

### Implementation Plan

Using UMPIRE framework:

**Understand:** Sail needs a doctest for `DataFrame.randomSplit()` to verify that the method works correctly and matches PySpark's expected output.

**Match:** [ To be completed after exploring the codebase — will find existing DataFrame doctests to use as a model ]

**Plan:**
1. Fork and clone `lakehq/sail`, open in GitHub Codespace
2. Locate existing PySpark DataFrame doctest files
3. Find the pattern used for similar doctests
4. Write a doctest for `randomSplit` using a fixed seed and known weights
5. Run the test suite to confirm the new doctest passes
6. Submit PR

**Implement:** [ Branch link to be added as work progresses ]

**Review:** [ To be completed — will check CONTRIBUTING.md and existing PR conventions ]

**Evaluate:** Run the doctest and confirm output matches PySpark's expected behavior for the same inputs.

---

## Testing Strategy

### Unit Tests

- [ ] Test case 1: `randomSplit` with two weights sums to 1.0 produces correct split sizes
- [ ] Test case 2: Fixed seed produces deterministic output
- [ ] Test case 3: Resulting DataFrames together contain all rows from the original

### Integration Tests

- [ ] Confirm doctest passes within Sail's full test suite run
- [ ] Confirm no regressions in existing DataFrame doctest suite

### Manual Testing

[ To be completed in Phase III ]

---

## Implementation Notes

### Week 5 Progress

- Selected issue #483 in `lakehq/sail`
- Confirmed issue is unclaimed (no assignee, no linked PR, no comments)
- Left a comment on the issue claiming it
- Completed Phase I: issue selected, README created

### Code Changes

- **Files modified:** [ To be added ]
- **Key commits:** [ To be added ]
- **Approach decisions:** [ To be added ]

---

## Pull Request

**PR Link:** [ To be added when submitted ]

**PR Description:** [ To be drafted in Phase IV ]

**Maintainer Feedback:**
- [ To be added as feedback is received ]

**Status:** Awaiting Phase II

---

## Learnings & Reflections

### Technical Skills Gained

[ To be completed at end of cycle ]

### Challenges Overcome

[ To be completed at end of cycle ]

### What I'd Do Differently Next Time

[ To be completed at end of cycle ]

---

## Resources Used

- [lakehq/sail repository](https://github.com/lakehq/sail)
- [Issue #483](https://github.com/lakehq/sail/issues/483)
- [PySpark DataFrame.randomSplit() docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.randomSplit.html)
