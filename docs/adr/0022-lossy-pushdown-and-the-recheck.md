# ADR-0022: Notion's filter is a lossy probe — every pushed conjunct is re-checked

**Status:** Proposed — extends [ADR-0019](./0019-sql-null-semantics-pushdown-soundness.md), builds on
[ADR-0018](./0018-query-plan-operator-tree.md) and [ADR-0021](./0021-scan-both-hash-join.md)
**Date:** 2026-07-31

## Context

[ADR-0019](./0019-sql-null-semantics-pushdown-soundness.md), as amended by its 2026-07-30
Correction, states pushdown soundness as a direction:

```
{rows the pushed filter keeps}  ⊇  {rows the residual keeps}
```

with the residual *"always re-applied client-side, so it is the residual that decides the answer."*

**That sentence describes a system that was never built.** The compiler routes a WHERE conjunct to
exactly one side: left-table conjuncts are compiled into `payload['filter']` and pushed
(`compiler.py`), right-table conjuncts are held as AST on `PlanningContext.residual_where` and
evaluated client-side by `eval3` in the `Filter` Operator (`queryplan.py`). Never both. A non-join
`SELECT` produces a bare `Scan` with no `Filter` above it at all.

So for a left-side predicate **the push decides alone**, and #384 is live: `WHERE effort != 5`
returns the `{"number": null}` row, because Notion's `does_not_equal` is the boolean complement of
its `equals` (measured against the live API) while SQL's `<>` against NULL is UNKNOWN and drops the
row.

**And ⊇ had no referent.** "Re-applied" presupposes the same conjunct evaluated twice. With the
push/residual split exclusive, the two sides of ⊇ ranged over *different predicates*. The invariant
was a property of a **predicate** — which is what `test_pushdown_soundness.py` measures, by handing
one generated filter to both evaluators — and not a property of any **execution**. Asserting it
while the left side re-checks nothing formally blesses the wrong answer.

**The general shape, and it is not novel.** A Notion filter is a **lossy probe**: it over-matches
relative to SQL semantics. This is the same situation as a lossy bitmap index scan in Postgres,
whose plans carry a `Recheck Cond` re-applying the predicate on the heap tuple — the index narrows,
the recheck decides. Framing #384 as "one weird operator" understates it; the correct frame is that
**a pushed filter is a hint** and normlite had no recheck.

The scale is measured, not assumed: across 24 000 leaf evaluations over all 36 declared
`<type>.<operator>` pairs, exactly one pair diverges (`number.does_not_equal` over `{"number":
null}`), in one direction, on one cell shape, iff the cell is valueless (396 valued cells agree, 104
valueless diverge). One divergence today — but the recheck is what makes *any* future lossy operator
safe, including the ones #383 measured and #382 exposed.

## Decision

**Every WHERE conjunct is evaluated client-side over raw cells, and that evaluation decides the
answer. Pushing is a transfer optimisation that never decides.**

- **Three terms, answering different questions** (glossary in `CONTEXT.md` §Pushdown / Recheck /
  Residual):
  - **Pushdown** — Notion-side pre-filter. A *hint*. May over-keep; never decides.
  - **Recheck** — the client-side re-application of a conjunct that *was* pushed. Always present,
    always decides. Carried as AST on `PlanningContext.recheck_where`.
  - **Residual** — a conjunct or sort key with **no pushed form**, evaluated client-side *once*:
    `is_null()` (#366) and the constructs the API rejects (#383).

- **`residual_where` is renamed `recheck_where`.** `residual_sorts` and `compile_residual_sorts`
  **keep their names**: a held-back sort key is never pushed and never re-applied — it is the
  genuine leftover, and "recheck" would assert a re-application that does not happen.

- **A `Project` Operator trims execution-only columns.** The recheck must read columns the user did
  not project (`SELECT name WHERE effort != 5`), but `SchemaInfo` cannot express "fetch, don't
  return": `_merge_names` flattens execution and projected names into one list and `ResultColumn`
  has no `projected` flag, so a widened `fetch_columns` reaches the user's `Row`. The plan becomes
  `Scan(superset) → Filter(recheck) → Project(projection)`, with `Project` owning the result schema.

  **The widening itself lives in `Planner.plan`, not in the compiler** — worth stating, because the
  reasoning above is about `_merge_names` and `ResultColumn`, which are compile-time concerns, and a
  reader will look for it there. `Planner.plan` appends the predicate's unfetched columns to
  `execution_names` and widens the `Scan`'s `filter_properties` to match (stripping
  `SpecialColumns`, ADR-0006), while `Project` trims back to the **pre-widening** `fetch_columns`,
  carried on `PlanningContext.pre_widening_fetch_columns`. `Project` must **not** trim to
  `result_columns()`: that drops the special columns (`object_id`, `is_archived`, …) the row still
  needs. The cost of this placement is that the planner rewrites already-compiled DBAPI parameters,
  which inverts the layering; it is confined to the widening case and left as known debt.

- **The `all(None)` phantom guard is deleted, and ADR-0005's outcome is derived.** An outer join
  fills right-owned columns with literal `None`; `eval3` returns UNKNOWN for a `None` cell; the
  WHERE policy drops UNKNOWN. This finally makes ADR-0019's Decision bullet true. It is also
  *strictly more correct*: `all(None)` is a **row-level guess** at what `eval3` knows **per cell**,
  and the guess is what mislabels a real all-empty right row as a phantom.

- **Existence and passage stay separate.** `HashJoin` decides which rows **exist** (only it knows,
  per left row, whether that row matched); `Filter` decides which rows **pass**. A `Filter` cannot
  create rows, so outer-join NULL-fill cannot move into it.

## Alternatives Considered

**A. Do not push `does_not_equal` over a nullable column.** Rejected as the general answer, though
it is sound (it is the degenerate case, pre-filter = `true`). It fixes one operator by giving up
pushdown for it, leaves the next lossy operator to be discovered the same way, and gives up transfer
reduction permanently. #383's construct 1 reaches this same fork for `date.does_not_equal` and its
body prefers exactly this remedy *there* — correctly, because the API **rejects** that filter, so
there is no pre-filter to be had. Where the API accepts the filter, the recheck is strictly better.

**B. Assert ⊇ and leave the left side unmediated (C1 only).** Rejected, and it is worse than doing
nothing: the invariant reads as satisfied while the push decides alone.

**C. A `projected: bool` flag on `ResultColumn`.** Rejected in favour of `Project`. It puts a
presentation concern into the schema and changes the ADR-0009 provenance record that several call
sites construct, to avoid one operator that ADR-0018's tree already invites — `HashJoin` re-derives
a trimmed schema informally today, so `Project` generalises an existing move.

**D. Move inner-vs-outer into `Filter` as semantic logic.** Investigated and rejected *for now*. The
identity is real (`A INNER JOIN B` ≡ `σ(B.key IS NOT NULL)(A LEFT JOIN B)`) and would delete the
`isouter` mode flag, but it needs `is_not_null()` (**#366**), needs a `Filter` present on joins with
no WHERE, and pays materialisation for every inner-join row it then discards. If `Filter` identified
phantoms by `all(c is None)` rather than a key test it would reintroduce the guess this ADR removes.
Revisit under #366.

**E. Delete the `all(None)` guard as part of #366 instead.** Rejected as unnecessary sequencing: the
deletion is measured to change nothing today (872 passed, on a covered path), so holding it back
buys no safety and leaves `Filter` with a join-only branch as it generalises to the scan path.

## Consequences

- **Transfer cost is unchanged.** Pushing continues exactly as today; the recheck runs over rows
  already in hand. The only new fetching is predicate columns not in the projection, which
  `filter_properties` would otherwise have excluded.
- **`Filter` becomes uniform.** One code path for scan and join, no left/right asymmetry, no
  structural guard above the predicate. Its `table=` argument and right-slice selection generalise
  to "the columns this predicate reads".
- **The `[unreachable empty-title]` boundary closes** — a real all-empty right row is no longer
  mislabelled a phantom.
- **⊇ acquires a referent.** `test_pushdown_soundness.py` keeps measuring it over predicates; the
  execution now honours it. The two are no longer the same claim.
- **A silent failure mode becomes possible and must be tested**: if a predicate column is not added
  to `fetch_columns`, `eval3` sees an absent cell → UNKNOWN → **every row dropped**.

  **Corrected 2026-08-15, C2 step 4.** This bullet originally predicted the mode would fail
  *loudly* — `resultset.py` raising `AttributeError` on the absent property first (the #388 shape)
  — hedged as "an accident of the decode path, not a guarantee". **Measured, and the prediction is
  wrong: it fails SILENTLY, returning zero rows.** On `select(t.c.object_id).where(t.c.is_active.is_(True))`:
  `fetch_columns == ['object_id']`, the conjunct held, `rows == []`, no exception. The loss happens
  one stage **earlier** than this reasoned — `filter_properties` is not even sent for that shape, so
  Notion returns the complete page and the cell is lost in the **schema**, which is built from
  `fetch_columns` and never contained `is_active`. The decode path never gets the chance to raise.
  "Deserves a test" is answered by `tests/unit/engine/test_select_pipeline.py`, which is also the
  **only** observer of the widening anywhere in the suite: the widening lives in `Planner.plan`, so
  no compile-level test in `tests/unit/sql/` can see it.
- **Right-side pushdown is unblocked but deliberately out of scope.** The right `Scan` is currently
  given `path_params` only — no payload — so a right-table WHERE is not pushed even though
  `join.right.get_data_source_id()` makes it perfectly expressible. That is a separate slice, and it
  **must land after this one**: pushing more without a recheck repeats #384 on a second axis. It
  also carries a hazard of its own — pushing into the right `Scan` changes the join's *input*, so
  under an outer join it can manufacture phantoms that then satisfy an `is_null()` predicate.
- **A plain `SELECT` did not reach the query planner at all, and that was a prerequisite this ADR
  did not name — now closed (step 2b).** `context.py:402` used to route a statement to
  `ExecutionStyle.EXECUTEQUERYPLAN` only `if stmt.is_select and (stmt._joins or
  stmt._is_aggregate)`; every other `SELECT` took `ExecutionStyle.EXECUTE` and `_execute_single`,
  never constructing a `Planner`. Measured with a spy on `Planner.plan`: `select(t)` and
  `select(t).where(t.c.effort != 5)` — **#384's own repro shape** — yielded zero invocations,
  `select(func.count())` one. So "wire the recheck on the scan path" was blocked, and the `Project`
  stage of the non-join branch was correct but unreachable in production, exercised only by direct
  `Planner(ctx).plan()` calls in `tests/unit/sql/test_query_plan.py`. **The routing is now
  `if stmt.is_select`**, pinned by `test_a_plain_select_is_driven_through_the_query_plan`
  (`tests/unit/engine/test_context.py`) — which exists because the streaming tests are green on
  **both** sides of the flip and therefore cannot pin it.

- **The routing change is IN SCOPE for C2, and it comes before the recheck — DONE.** A first draft of
  this ADR proposed deferring it to its own slice; that was overruled deliberately — deferring it
  would land steps 3 and 4 on a code path no plain `SELECT` executes, i.e. ship the recheck without
  rechecking anything for the statement shape #384 actually reports. Its cost was measured, not
  estimated: flipping `context.py:402` to `if stmt.is_select` redded **24** tests, but **19 of those
  were one test-harness gap** — `tests/utils/execution.py:run_context` duplicates
  `Connection._execute_context`'s dispatch and had never gained an `EXECUTEQUERYPLAN` branch, so a
  routed `SELECT` fell through to `do_executemany` with a `None` `bulk_operation`. Giving the harness
  that branch left **5 genuine failures spanning TWO gaps**: `_execute_query_plan` drained the plan
  eagerly and never read `context.execution_options`, so `stream_results` / `yield_per` (ADR-0010)
  did not survive the plan path (4 streaming tests); and `post_exec` read the wrong cursor's
  `rowcount` (**#392**, which reproduced on `main` with no routing change and was fixed separately).

  **Resolution: make the plan path lazy.** The two rejected alternatives are recorded because both
  were measured, not guessed. *Cascade the options into `Scan`'s page size while keeping the eager
  drain* costs **4 reds, not 1**, and regresses ADR-0010 in a specific way: a mid-stream backend
  failure moves out of the iteration boundary into `conn.execute()`, so page-1 rows the contract
  promises "stay pulled" are never handed over. *Route only the `SELECT`s that need a recheck*
  measured **0 reds against a suite with a hole in it** — nothing combined streaming with a WHERE
  until `test_streaming_survives_a_where_clause` was written; with it, that option costs 1. It also
  gives one statement kind two execution paths keyed on its predicate, which is the shape of thing
  that produced #384.

  What landed, as one commit: the routing flip; `yield_per` cascaded into the plain-`SELECT` `Scan`
  (**not** into the aggregate or join `Scan`s, whose parents drain by nature, where a smaller page
  only multiplies requests); `Scan.next()` batching by that page size instead of the constant 100;
  and a **batch source** seam — `Cursor` refills from anything answering
  `next() -> Optional[list[tuple]]`, satisfied by a new `JsonPageSource` adapter over `PageIterator`
  and by **a plan root unwrapped**, since `Project.next()` already has that shape. Laziness is gated
  on `stream_results` / `yield_per`, mirroring `_execute_single`'s `streamable=` gate; without it a
  non-streaming plan path would report `rowcount == -1`, because `post_exec` memoizes
  `self.cursor.rowcount` immediately after execution.

- **This ADR is Proposed, not Accepted.** ADR-0019 was marked Accepted while its `all(None)` bullet
  described code that was never written; that drift cost a session to discover. This one flips to
  Accepted when the code lands.
