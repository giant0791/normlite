# ADR-0008: The `JoinExecution` seam — a stateful object spanning the two-phase dispatch

**Status:** Accepted
**Date:** 2026-06-14

---

## Context

`Select.join()` (PRD #302, user story 16) runs as a two-phase pipeline: phase 1 queries the
left table, then the engine's EXECUTEMANY dispatch fires a batched `pages.retrieve` for the
right side, and phase 2 merges the two. Today that choreography is spread across three places
with no single owner:

- the `Select._setup_execution` / `_finalize_execution` hooks in `sql/dml.py`;
- five module-level free functions — `build_phase_two_batch`, `merge_inner_join_rows`,
  `_project_join_row`, `_merge_rows_with_right_side_filter`, `_right_side_passes`;
- mutable `ExecutionContext` attributes whose contract is implicit in assignment order.

PRD #302 bet on a deep `JoinExecution` module ("left rows + join node in, merged rows out")
but the implementation never built it. This ADR fixes the **shape of that seam** before any
code moves (#314–#317). It is purely a structural decision — no behaviour changes.

It does not revisit the join *semantics* already fixed by
[ADR-0005](./0005-outer-join-phantom-null-semantics.md) (phantom-NULL drop),
[ADR-0006](./0006-join-result-row-shape.md) (row shape), or
[ADR-0007](./0007-join-dangling-fk-propagation.md) (dangling-FK cardinality); those are
preserved bit-for-bit.

## Decision

**Introduce `JoinExecution`: a stateful object, constructed once per execution, that owns all
join-domain state and computation across both phases. Configuration enters through the
constructor; the two phase methods carry only row data.**

```
JoinExecution(join, projection, right_filter)
    .left_schema   # read-only, derived from join.left
    .right_schema  # read-only, derived from join.right
    .prepare(left_rows)  -> bulk_params
    .assemble(right_rows) -> (merged_schema, merged_rows)
```

- **Stateful, two methods, spanning the dispatch boundary.** A single synchronous call is
  impossible: the engine's EXECUTEMANY dispatch (`base.py`) sits *between* phase 1 (build the
  `pages.retrieve` batch) and phase 2 (assemble the merged result). So the object is
  constructed and `prepare`d in `_setup_execution`, survives the dispatch, and is `assemble`d
  in `_finalize_execution`. It holds `left_rows`, both schemas, the onclause, `isouter`, the
  projection, and the right-side filter internally between the two calls.

- **Config to the constructor.** The `join` node, `projection` (statement-level,
  `Select._projection`), and `right_filter` (bound in `ctx.pre_exec()` *before*
  `_setup_execution`, so available at construction) are fixed for the whole join and enter via
  the constructor. `prepare`/`assemble` take only the phase-1 / phase-2 row batches. This
  keeps the two-method interface honest — methods take phase data, construction takes config —
  rather than smuggling `projection`/`right_filter` in as hidden parameters.

- **Owns both schemas; exposes them read-only.** `left_schema` and `right_schema` are pure
  functions of `join.left` / `join.right`, so `JoinExecution` builds both and exposes them so
  the hooks can inject each cursor's description.

- **I/O stays in the hooks.** `JoinExecution` never touches a cursor or the engine. The
  `Select` hooks drive all I/O: run the phase-1 query and drain `left_rows`, inject the schema
  descriptions, wire the errorhandler, write the returned `bulk_params` into the shared
  EXECUTEMANY channel, then drain `right_rows` and hand them to `assemble`. The object is pure
  compute + state.

- **The attribute collapse is 5 → 1, not 8 → 1.** The five genuinely join-only context
  attributes — `_join`, `_join_left_schema`, `_join_left_rows`, `_join_right_schema`,
  `join_right_filter` — collapse into a single `context._join_execution` holding the instance.
  Nothing outside `sql/dml.py` reads these five, so the collapse is private to the
  Select↔context interaction.

## Alternatives Considered

**A. Collapse all eight attributes (incl. `_staged_result_cursor`, `bulk_operation`,
`bulk_parameters`) into `JoinExecution`.**
Rejected. Those three are *not* join-only — they are the shared EXECUTEMANY dispatch channel,
also written by DELETE / UPDATE / INSERT…RETURNING (`dml.py:661,713,721`) and read by
`ExecutionContext._get_exec_cursor`. If `JoinExecution` owned them it would fork a parallel
dispatch path and break the non-join statements. Instead `JoinExecution` *cooperates with* the
channel: `prepare` returns `bulk_params` and the hook adapts them onto the existing rail.

**B. A fatter object that drives phase-1/phase-2 I/O itself (takes the cursor).**
Rejected. It would entangle join-domain logic with engine choreography and cursor lifecycle,
duplicating dispatch concerns the engine already owns. Keeping I/O in the hooks leaves a small,
pure, testable object — the "rows in, merged rows out" seam PRD #302 named.

**C. Keep the issue's literal `prepare(left_rows, left_schema, join)` / one-shot free
function.** Rejected. `assemble` unavoidably needs `projection` and `right_filter`, which that
signature omits; and a free function cannot hold state across the dispatch boundary. The
constructor-config shape makes the real dependencies explicit.

## Consequences

- **Behaviour-preserving, verified by the existing suite.** Inner/outer cardinality (ADR-0007),
  row shape (ADR-0006), and phantom-NULL drop (ADR-0005) are unchanged; `test_join_compilation`,
  `test_join_strategy`, and `test_join_pipeline` stay green without edits through #314–#316.
- **One `Join` per instance.** `JoinExecution` wraps exactly one join (`_joins[0]`). The
  existing multi-join silent-drop (chaining `.join().join()` drops `_joins[1:]`) and untested
  self-join boundary (ADR-0007 consequences) are **unchanged known limitations** — this seam
  neither fixes nor guards them. Multi-join composition (iterating/chaining instances) is
  explicit future work.
- **Compiler and cursor contracts untouched.** `visit_join` / `compiled_dict` shape (incl.
  `join_right_filter`, `compiler.py:627`) and the `CursorResult` surface are unchanged; this is
  a runtime/execution seam only.
- **Internals migrate incrementally.** #314 stands up the seam *delegating* to the five free
  functions (tracer bullet, zero behaviour change); #315 folds in batch + merge/projection;
  #316 folds in right-side filtering; #317 deletes the now-dead functions and adds seam-level
  tests. This ADR records the *target* interface, not the intermediate delegation.
