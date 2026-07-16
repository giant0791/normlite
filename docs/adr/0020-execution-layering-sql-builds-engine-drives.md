# ADR-0020: Execution layering — SQL builds, ENGINE drives, DBAPI performs I/O

**Status:** Accepted
**Date:** 2026-07-16
**Applied by:** [ADR-0018](./0018-query-plan-operator-tree.md) (the Query plan is the mechanism that
restores this layering for `SELECT`).

---

## Context

normlite is intended as a strict three-layer architecture with a single dependency direction:

- **DBAPI** — the I/O adaptation layer (`notiondbapi/`). Owns the PEP-249 `Cursor`, pagination
  mechanics (`PageIterator`, `start_cursor` threading, the eager-vs-lazy page pull), and the
  Notion-client transport. Knows nothing about SQL or the engine.
- **SQL** — the AST and semantics layer (`sql/`). Owns `ClauseElement`s, compilation, and the
  *description* of what a statement means and needs. Knows nothing about the engine.
- **ENGINE** — the orchestration layer (`engine/`). Owns `Connection._execute_context`, the
  execution-style dispatch, and cursor routing. Depends *downward* on SQL (to compile) and DBAPI
  (to execute).

The invariant: **ENGINE → SQL, ENGINE → DBAPI, SQL → DBAPI. Nobody calls upward.**

The execution pipeline violates it. `Select._setup_execution` reaches *up* into ENGINE:
`context.engine.do_execute(...)` (`dml.py:832`), mints its own cursor (`dml.py:843`), and wires an
errorhandler (`dml.py:847`). The SQL layer does not merely *know* what I/O it needs — it *performs*
that I/O, driving the engine through a handle it was handed.

The distinction is the whole point:

- **Knowing** what I/O a statement needs — a join needs a left drain then a bulk retrieve; a plain
  select needs one query — legitimately belongs to SQL. A `Join` *is* the fact that there are two
  round-trips.
- **Performing** that I/O — calling `do_execute`, minting cursors, driving a loop — belongs to
  ENGINE.

The hooks fuse the two, so a description that should be inert *data* is instead live *behaviour*
executed from the wrong layer. The precise shape is a Law-of-Demeter reach: `context.engine.<call>`
grabs the engine *through* the ENGINE-layer mediator and operates it directly.

The tension is not confined to the query path. `CheckEnforcement` runs client-side above the DBAPI
boundary yet raises a DBAPI-layer error (CONTEXT.md §CheckConstraint, "deliberate layering
tension"); reusing the fake client's `_Filter` in the SQL layer is called out as "a layering
inversion" (CONTEXT.md §is_null). The layers are operative throughout the codebase but were never
stated as a principle, so a reader meets the violations before the intent and cannot tell whether
layering was designed or accidental.

## Decision

**State the three-layer invariant as foundational, and restore it for `SELECT` by making the
statement's I/O a description the engine drives rather than behaviour the statement performs.**

Three roles, one per layer:

- **SQL builds the plan.** The `Planner` turns a `Select` AST into a **Query plan** — a tree of
  `Operator`s (ADR-0018). The plan is *data*: a description of the I/O and computation, carrying no
  engine handle. Building it is the SQL layer's whole job; it then steps back.
- **ENGINE drives the plan.** The drive loop (`open` → `next` → `close`) lives in
  `Connection._execute_context`, not in `_setup_execution`. The engine consumes the plan
  *polymorphically* — it never inspects what kind of operators are inside.
- **`CursorResult` is the pull consumer.** For results that stream, the engine does not drain; the
  `CursorResult` façade pulls batches through the plan root on demand, and the plan's leaves pull
  through the DBAPI `Cursor`'s existing lazy `PageIterator`.

Knowledge crosses the layer boundary as **the plan (data)**. It must never cross as an engine-side
type switch (`if isinstance(stmt, Select) and stmt._joins:`), which would pull SQL knowledge *up*
into ENGINE — a worse inversion than the one being fixed.

### Staging

The restoration is deliberately staged so that streaming is never regressed and each move has one
reason to fail. The staging mirrors ADR-0018's slices.

- **Baseline — #361 (tracer bullet).** The plan is *built* and unit-tested but **inert**: nothing
  drives it in production. `Scan` takes the DBAPI `Cursor` directly (no port). The plain select's
  cursor flows out of the pipeline exactly as today. The inversion is unchanged — deliberately, so
  slice 1 stays behaviour-preserving and the existing suite remains a true oracle. See
  [ADR-0018](./0018-query-plan-operator-tree.md).

- **Move A — #364 (drop EXECUTEMANY), blocking plans only.** The engine drives **blocking** plans
  (`Join`, `Aggregate`), which drain their child in `open()` *by nature*, so eager driving is
  already correct for them. The `QueryIO` dispatch port arrives here — this is its first real
  caller — and the borrowed `ExecutionStyle.EXECUTEMANY` channel, `_join_errorhandler` and the
  staged-cursor wiring go. **The plain streaming path is left as cursor-passthrough**: it is not
  driven through the plan yet, so `stream_results` / `yield_per` are untouched. Move A restores the
  layering for the join/aggregate paths only.

- **Move B — a separate PRD.** `CursorResult` becomes the lazy pull consumer, and the plain
  streaming path finally runs *through* the plan. This completes the invariant — SQL builds, ENGINE
  drives, `CursorResult` pulls — for *every* select. **Pagination mechanics stay in the DBAPI
  `Cursor`; only the drive/pull relocates.** Do not move `PageIterator` or `start_cursor` handling
  upward: the DBAPI layer owns transport mechanics; ENGINE owns only the *policy* (eager vs lazy,
  `yield_per`) and the *drive*.

The split matters because driving a **plain** plan to exhaustion would kill `yield_per` — draining
is the opposite of streaming. Only blocking plans can be engine-drained safely before Move B exists.

## Alternatives Considered

**A. Move the hook branching into `Connection` as a type switch.** Rejected. Relocating
`if self._joins:` into the engine makes ENGINE depend on SQL *internals* — the opposite inversion.
The knowledge must cross as the plan (data), driven polymorphically.

**B. Keep the double-dispatch hooks; let `_setup_execution` drive the plan.** Rejected. The plan
would own its I/O but still be *driven* from the SQL layer, so `SQL → ENGINE` persists through the
injected port. Half-fixing the inversion is not fixing it.

**C. Restore the layering in one move.** Rejected. Blocking and streaming plans have opposite drive
semantics (drain vs. pull-lazily); fusing them forces the `CursorResult` pull-consumer rework
(Move B) into the EXECUTEMANY-removal slice, making a streaming regression and an I/O-ownership bug
indistinguishable in one diff.

**D. A separate foundational ADR vs. amending ADR-0018.** This ADR is foundational: the invariant
governs DDL, DML writes, and reflection, not only `SELECT` execution. ADR-0018 *applies* it to the
query path and references it; the principle stands on its own so Move B's future PRD and other
layering work (CheckEnforcement's error boundary, the `_Filter` reuse) have a record to cite.

## Consequences

- The three-layer invariant is now stated and citable; the known violations (the execution hooks,
  CheckEnforcement's DBAPI error, the `_Filter` reuse) can be named as *violations* rather than read
  as accidents.
- `SELECT` layering is restored in stages: inert (#361) → blocking-only (#364/Move A) → all selects
  (Move B). The plain streaming path is the last to move and is protected until Move B ships.
- `CursorResult` grows a new responsibility (pull consumer) in Move B; the DBAPI `Cursor` keeps all
  pagination mechanics.
- The mutation statements' `bulk_operation` / `bulk_parameters` channel is untouched throughout;
  DELETE / UPDATE / INSERT…RETURNING carry no risk from this work (see ADR-0018).
- Move B is **not yet filed**. It should become its own PRD when Move A lands.
