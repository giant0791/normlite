# ADR-0018: The Query plan — an Operator tree that owns its own I/O

**Status:** Accepted — supersedes [ADR-0008](./0008-joinexecution-seam.md)
**Date:** 2026-07-15
**Applies:** [ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md) — the Operator tree
is the mechanism that restores the DBAPI/SQL/ENGINE layering for `SELECT`.

> **Correction (2026-07-16).** Two claims below were walked back while slicing the work; see
> [ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md) for the reasoning. (1) The SQL
> layer does **not** drive the plan — the engine does; `_setup_execution` only *builds* it, and in
> slice 1 the plan is **inert** (built, unit-tested, not driven). (2) The `QueryIO` port is **not**
> part of slice 1; it arrives in slice 2 (drop-EXECUTEMANY, #364) where it gets its first real
> caller. In slice 1 `Scan` takes the DBAPI `Cursor` directly. The corrected sentences are marked
> **[corrected]** inline.
>
> **(3) The slice-1 `Scan`'s raw-cursor coupling is more compromised than "takes the `Cursor`
> directly" makes it sound, and both compromises are provisional shims.** (a) `Scan.next()` reads a
> page via `fetchmany(NOTION_MAX_PAGE_SIZE)` — a **row-count stand-in** for a page pull. This
> re-splits by a row count the whole pages `PageIterator` already produced (the very thing the
> batch-at-a-time rationale below, lines 55-58, argues against) and lifts the page-size constant
> **into the SQL layer**, where [ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md)
> says pagination *mechanics* must not live. It matches only because `fetchmany(100)` = one Notion
> page by coincidence of Notion's page size. (b) `Scan.open()` sets `stream_results=True` — an I/O
> *policy* (eager vs lazy) ADR-0020 assigns to ENGINE, expressed through a `stream_results` /
> `yield_per` `execute()` extension that is **not** part of PEP-249. The honest contract is a
> page-granular pull on the DBAPI `Cursor` (a `fetchnextpage()`-style primitive that surfaces the
> `PageIterator`'s grain) reached **through the `QueryIO` port** in slice 2 (#364): the page size
> returns to the `Cursor`, the streaming policy moves to ENGINE, and `next()` stops re-splitting
> pages. Slice 1 keeps the shim so the tracer bullet stays inert and behaviour-preserving.
>
> **Correction (2026-07-17) — scope of the issues vs. this ADR.**
>
> **(4) The issues (#361–#369, authored 2026-07-15) predate Corrections (1)–(3) above (2026-07-16)
> and their acceptance criteria were never re-cut.** Three of them still demand the `QueryIO` port
> and a fake ("`Scan`/`Aggregate`/`Join` unit tests against the fake `QueryIO`") — #361's did too,
> and slice 1 shipped without it, deliberately and correctly. **This ADR, not the issue text, is
> authoritative:** the port arrives in **#364**, its first real caller. Introducing it in #363 would
> shape it around the `bulk_operation`/staged-cursor choreography that #364 deletes — a port
> fossilising the channel it exists to escape. #363's `Join`/`Filter` keep `prepare`/`assemble` and
> stay pure compute, unit-testable exactly as `JoinExecution` already is (config to the constructor,
> rows fed directly). For the same reason #363 cannot yet satisfy "the `if self._joins:` branch is
> gone from `Select`'s hooks": while the hooks still drive phase-1/phase-2, removing the branch
> would force the plan to own the choreography — which *is* #364.
>
> **(5) Slice 1 shipped narrower than #361's criteria.** Only `Scan` landed; the `Operator`
> protocol, `Planner` and `PlanningContext` did not. **#363 carries them**, as its own criteria
> already presuppose.
>
> **(6) Carrying the residual as AST while `Filter` wraps `_Filter` verbatim implies a bridge.**
> `_Filter` eats Notion JSON, so the `Planner` compiles the residual AST to JSON at plan-build time
> with bind values **inlined** — forced, because deleting `join_right_filter` from `compiled_dict`
> deletes the only thing that bound its `:key` placeholders (`ExecutionContext._bind_params`), and
> `BindParameter` already carries `.value`. Consequently the residual's binds must **stop being
> registered** in `execution_binds`: `_bind_params` *pops* what it binds and
> `_assert_all_params_consumed` raises on leftovers, so a residual compiled to JSON *and* still
> registered would fail every join with a parameterised right-side WHERE
> (`ArgumentError: Unused bind parameters`) — breaking the "existing join suite green without edits"
> invariant. A residual never `_compiler_dispatch`-ed never reaches `visit_bindparam`, so the
> accounting works out. The bridge is throwaway: ADR-0019's three-valued evaluator reads the AST
> directly and removes it. It is **not** the target contract.
>
> **Correction (2026-07-20) — there is no `QueryIO` port (#364).**
>
> **(7)** #364's acceptance text ("I/O flows through the `QueryIO` port; Operators never touch a
> cursor or the engine directly"; "unit tests against the fake `QueryIO`") is more pre-Correction
> boilerplate (Correction (4)). It **overshoots** [ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md):
> the invariant forbids SQL touching the **engine**, not the DBAPI `Cursor`, which is the *downward*
> boundary object (`SQL → DBAPI` is legal). The port arrives as **nothing new**: the plan's `open()`
> takes the **DBAPI `Connection`** the engine hands down while driving (`Connection._execute_context`
> mints it via `Engine.raw_connection()`), and each leaf mints its own `conn.cursor()` — one cursor
> per result set, two for a `Join`. The plan carries **no engine handle** (the Decision's rule
> holds), so `open(conn: Connection)`, not `open(io: QueryIO)`. A cursor-hiding `QueryIO` wrapper
> would add an abstraction the layering does not require *and* prematurely force the
> `stream_results`/page-size relocation of Correction (3), which the **blocking** Move-A path never
> exercises. `Table.create(bind=engine)` (`schema.py:829`) is not a counter-precedent: it holds
> `bind` only to call the **public** `bind.connect().execute(stmt)` — an entry-point facade, never an
> operator reaching `raw_connection().cursor()`.

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

- **`Select`'s hook branching disappears**; `_setup_execution` **builds** a plan and the engine
  drives it. **[corrected]** The drive loop lives in the engine, not the hook — restoring the
  layering ([ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md)); a plain select's
  plan is inert in slice 1.
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
  `Planner` and `PlanningContext` while joins **still ride EXECUTEMANY** and `Join` keeps
  `prepare`/`assemble` internally — a failure there is a *tree* bug. **[corrected]** The `QueryIO`
  port is **not** part of slice 1: the plan owns no I/O yet, so the port would have no caller. In
  slice 1 `Scan` takes the DBAPI `Cursor` directly and the plan is inert. Slice 2 gives the plan
  its I/O **through the `QueryIO` port** (its first caller), deletes `context.py:413` and the
  staged-cursor wiring, and collapses `prepare`/`assemble` into `Join.open()` — a failure there is
  an *I/O ownership* bug. Slice 2 drives **blocking** plans (join/aggregate) from the engine; the
  plain streaming path stays cursor-passthrough until Move B (see
  [ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md)). Fusing the slices would make
  a tree bug and an I/O-ownership bug indistinguishable in one diff.
