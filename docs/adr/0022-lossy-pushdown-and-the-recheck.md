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
  to `fetch_columns`, `eval3` sees an absent cell → UNKNOWN → **every row dropped**. In practice
  `resultset.py` raises `AttributeError` on the absent property first (the #388 shape), so it fails
  loudly — but that is an accident of the decode path, not a guarantee, and deserves a test.
- **Right-side pushdown is unblocked but deliberately out of scope.** The right `Scan` is currently
  given `path_params` only — no payload — so a right-table WHERE is not pushed even though
  `join.right.get_data_source_id()` makes it perfectly expressible. That is a separate slice, and it
  **must land after this one**: pushing more without a recheck repeats #384 on a second axis. It
  also carries a hazard of its own — pushing into the right `Scan` changes the join's *input*, so
  under an outer join it can manufacture phantoms that then satisfy an `is_null()` predicate.
- **This ADR is Proposed, not Accepted.** ADR-0019 was marked Accepted while its `all(None)` bullet
  described code that was never written; that drift cost a session to discover. This one flips to
  Accepted when the code lands.
