# ADR-0018: The Query plan — an Operator tree that owns its own I/O

**Status:** Accepted — supersedes [ADR-0008](./0008-joinexecution-seam.md)
**Date:** 2026-07-15

---

## Context

`Select` executes joins and aggregates by branching in its execution hooks — `if self._joins:` /
`if self._is_aggregate:` in `_setup_execution` / `_finalize_execution` — and delegating to two
sibling seams, `JoinExecution` and `AggregateExecution`. Three capabilities are wanted:
**multi-join composition**, **GROUP BY / HAVING**, and a real home for **filter pushdown**. Each
would add another branch and another seam.

[ADR-0008](./0008-joinexecution-seam.md) fixed the shape of the join seam and explicitly rejected
its "Alternative B — a fatter object that drives phase-1/phase-2 I/O itself", holding that
**"a single synchronous call is impossible: the engine's EXECUTEMANY dispatch (`base.py`) sits
between phase 1 and phase 2."** That premise does not survive inspection:

- The boundary is **self-inflicted**. `context.py:413` opts a join `Select` into
  `ExecutionStyle.EXECUTEMANY` purely as a *vehicle* for dispatching the phase-2 batch — it
  borrows the **mutation** statements' bulk channel.
- The hook is **already an I/O driver**. `Select._setup_execution` calls
  `context.engine.do_execute(...)` directly for phase 1 (`dml.py:832`) and mints its own cursor
  (`dml.py:843`). Only phase 2 goes through the channel. The split is an accident, not a design.
- The channel is **single-shot**: `_execute_many` (`base.py:277`) fires `do_executemany` exactly
  once. Chained multi-join (`a JOIN b ON a.b_ref`, then `b JOIN c ON b.c_ref`) needs **N sequential
  round-trips**, because `c`'s page IDs are unknowable until `b`'s pages return. Under the channel
  chained multi-join is not unimplemented — it is **inexpressible**.

Separately, `NotionCompiler.visit_select` is **already a planner** and nobody named it: it splits
`left_conjuncts` / `right_conjuncts` (predicate pushdown, `compiler.py:660-676`), computes the
pushable *leading run* of ORDER BY keys (sort pushdown, `compiler.py:734-750`), and derives
`filter_properties` (projection pushdown). The leftovers — **residuals** — have no home, so they
ride `compiled_dict` as ad-hoc keys (`join_right_filter`, `join_right_sorts`) which
`ExecutionContext` re-exposes as attributes which `Select._setup_execution` feeds to
`JoinExecution.__init__`. That path is the actual obstacle to a third seam.

## Decision

**Replace the hook branching with a `Query plan`: a tree of `Operator`s, built at execution time by
a `Planner`, that drives its own I/O. Joins stop using `ExecutionStyle.EXECUTEMANY`.**

- **Operators are batch-at-a-time** (`open(io)` / `next() -> list[tuple] | None` / `close()`) —
  vectorized, **not** classic row-at-a-time Volcano. Notion's transport grain *is* a batch
  (`page_size` ≤ 100, `PageIterator` already yields whole pages, `pages.retrieve` is inherently
  bulk); a row-at-a-time `next()` would re-split batches the layer below just produced. There is
  no `LIMIT` in normlite, so lazy pipelining has no consumer to pay for it.

- **Every SELECT gets a plan; `Scan` delegates to today's path.** `Scan` is a thin adapter over the
  existing cursor / `PageIterator` machinery rather than a reimplementation, so the plain-select
  path — the most-used and most-tested — keeps its behaviour and its
  [ADR-0010](./0010-streaming-result-token-pagination.md) streaming.

- **Blocking operators replace the drain-all flag.** `Join` and `Aggregate` drain their child in
  `open()`. ADR-0010's "joins and aggregates force drain-all" becomes **structural** rather than
  enforced — today it holds only because `dml.py:832` omits the `streamable` flag that
  `base.py:272` passes on the normal path.

- **The plan owns its I/O through an injected port.** A join `Select` runs as
  `ExecutionStyle.EXECUTE`; `context.py:413` is deleted. The `bulk_operation` / `bulk_parameters`
  channel is **untouched** and continues to serve DELETE / UPDATE / INSERT…RETURNING — joins simply
  stop borrowing it.

- **One `Scan` per plan; every right side is a `Retrieve`.** normlite joins through a `Relation` FK,
  so the right side is always reached *by page ID*, and Notion has no `id IN (...)` filter that
  could make it a query. `Scan` is the only Operator carrying a compiled payload.

- **`Retrieve` is the `Join`'s second, parameterized child**, bound **once per open with the whole
  deduplicated oid set** — *not* Graefe's per-outer-tuple rebinding. normlite conforms to Volcano
  structurally while deviating on binding cardinality: `Join` blocks, so it knows every oid before
  `Retrieve` opens, and one bulk call is one round-trip where per-tuple rebinding would be
  thousands.

- **`PlanningContext` carries compile-time decisions to run time.** Authored by the compiler,
  harvested onto `Compiled`, read by the `Planner`. `join_right_filter` / `join_right_sorts` are
  deleted; the residual rides the `PlanningContext` as an **AST**. `compiled_dict` stays pure
  JSON-like data.

## Alternatives Considered

**A. Keep the channel; a tree of `prepare`/`assemble` pairs.** Rejected. It preserves ADR-0008
verbatim but leaves the single-shot dispatch in place, so chained multi-join stays inexpressible —
it would deliver star-joins at best, i.e. not the motivating capability.

**B. An engine-level multi-round dispatch loop.** Rejected. It keeps I/O in the engine at the cost
of turning `Connection._execute_context`'s linear step 5→6→8 into a loop affecting *every*
statement type, to serve one statement type.

**C. Extend `ExecutionContext` as the messenger.** Rejected on two counts. It walks back ADR-0008's
headline win (the 5 → 1 attribute collapse); and `ExecutionContext` is constructed at `base.py:226`
(step 3) — **after** compile (step 2) — so it can only ever be the messenger's *destination*, never
its author. That timeline is precisely why `join_right_filter` had to ride `compiled_dict`.

**D. Extend `CompilerState` instead of a distinct `PlanningContext`.** Rejected. `CompilerState`
mixes transient compiler *scratch* (`in_where`, `compile_state`, `stmt`) with durable planning
*outputs* (`execution_binds`, `result_columns`, `fetch_columns`). Only outputs may survive
compilation; a distinct object names that boundary and lets `Compiled` harvest **one object**
rather than cherry-pick fields it must know the lifetime of.

**E. `Retrieve` internal to `Join`.** Rejected in favour of Volcano's shape. Notion offers exactly
one right-side access path, so the extension point is speculative today — but the parameterized-inner
structure is the honest expression of what the algorithm is, and the coupling cost is low.

**F. Planning at compile time (the plan *is* the compiled artifact).** Deferred, not rejected. It is
the architecturally cleaner end-state — pushdown decisions are static, so they belong at compile —
but it changes the `Compiled` contract, the `.compile()` public surface, and every compiler test.
Execution-time planning reads the same decisions off the `PlanningContext` for a fraction of the
blast radius; moving them later is a local change.

**G. A compilation stack (`_compiler_state` as push/pop).** Out of scope. It fixes a real,
documented footgun (`CONTEXT.md` §Compiler entry points: calling `.compile()` inside a `visit_*`
silently clobbers in-flight state — its canonical example is literally
`[j.compile(self) for j in select._joins]`). But the plan turns out to create **no** new nesting
pressure: one `Scan` per plan means one payload per plan, however many joins. Filed separately.

## Consequences

- **`Select`'s hook branching disappears**; `_setup_execution` builds a plan and drives it.
  `JoinExecution` / `AggregateExecution` become `Operator`s, keeping ADR-0008's and
  [ADR-0011](./0011-aggregate-execution-seam.md)'s config-to-constructor discipline intact.
- **ADR-0008 is superseded, not contradicted in spirit.** Its diagnosis (choreography with no
  single owner) and its discipline (config to the constructor; a small, testable, pure-compute
  object) both survive. What changes is one premise — that the dispatch boundary was immovable —
  and therefore its "I/O stays in the hooks" conclusion.
- **Multi-join becomes expressible** by nesting: `Join(Join(Scan(a), Retrieve(b)), Retrieve(c))`,
  N sequential round-trips, each bulk-deduped. The global dedup survives *because* `Join` blocks.
  The [multi-join silent-drop boundary] (chaining `.join()` drops `_joins[1:]`) is closed by the
  feature slice that follows, not by this ADR.
- **GROUP BY becomes an ordinary blocking Operator** rather than the third seam ADR-0011
  anticipated ("a sibling to the ADR-0008 prepare/assemble pattern").
- **The join algorithm gets a name**: a **batched index nested-loop join** — retrieve-by-ID is a
  primary-key index seek, batched across the whole left side, with the hash map (`dml.py:1295`)
  serving *reassembly*, not matching. It is not a hash join and not a classic nested loop.
- **`_join_errorhandler` and the staged-cursor wiring are rewritten** as part of the I/O port.
- **Semantics are unchanged by this ADR.** The `Filter` Operator wraps today's `_Filter` and the
  `all(None)` guard verbatim, so the existing suite remains a true oracle. The right-side filtering
  semantics change is [ADR-0019](./0019-sql-null-semantics-pushdown-soundness.md), a separate,
  deliberate slice.
- **This ADR lands in two behaviour-preserving slices**, not one. Slice 1 stands up the tree,
  `Planner`, `PlanningContext` and the `QueryIO` port while joins **still ride EXECUTEMANY** and
  `Join` keeps `prepare`/`assemble` internally — a failure there is a *tree* bug. Slice 2 gives the
  plan its I/O, deletes `context.py:413` and the staged-cursor wiring, and collapses
  `prepare`/`assemble` into `Join.open()` — a failure there is an *I/O ownership* bug. Fusing them
  would make the two indistinguishable in one diff.
