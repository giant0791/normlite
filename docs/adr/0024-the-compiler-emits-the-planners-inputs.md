# ADR-0024: The compiler emits the planner's inputs

**Status:** Accepted — supersedes the widening placement in
[ADR-0022](./0022-lossy-pushdown-and-the-recheck.md) (the paragraph "The widening itself lives in
`Planner.plan`"); builds on [ADR-0018](./0018-query-plan-operator-tree.md),
[ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md) and
[ADR-0023](./0023-dml-executes-through-the-query-plan.md)
**Date:** 2026-10-16

---

## Context

The compiler started as a transpiler: one SQL statement in, one Notion request out. That model no
longer holds. A SELECT runs as a **Query plan** (ADR-0018), and DELETE and UPDATE run through the
plan too (ADR-0023). One SQL statement becomes an orchestrated flow of several Notion calls: a
`data_sources.query` per side, a recheck, one `pages.update` per row. The **Planner** builds that
flow. The planner, not the Notion client, is now the compiler's user.

The compiler output already grew in that direction. `<kind>_operation` and `<kind>_params` carry
the **ModifyTable** write for DELETE and UPDATE (ADR-0023). The **PlanningContext** carries the
recheck WHERE and the residual sorts as AST.

One decision went the other way. ADR-0022 put the **widening** in `Planner.plan`: the planner adds
the columns that the recheck reads to `execution_names` and rewrites the compiled
`filter_properties`. ADR-0023 then added the title-only fetch for a DELETE/UPDATE with no WHERE, in
the same place. ADR-0022 already named the cost: *"the planner rewrites already-compiled DBAPI
parameters, which inverts the layering … left as known debt."* After #425 the same widening code
lives twice, in `_plan_select()` and `_plan_modify()`.

## Decision

**The compiler emits every input that the statement decides. The planner builds operators from
those inputs and adds only what the execution decides.**

1. **Two output channels, unchanged in kind.** `compiled_dict` carries JSON-like data only: Notion
   request pieces such as `filter_properties`, the write operation and the parameter templates. The
   **PlanningContext** carries AST decisions: `recheck_where`, `residual_sorts` and the
   `pre_widening_fetch_columns` snapshot. The JSON-purity invariant of `compiled_dict` stays
   (`CONTEXT.md` §Compiler). `compiled_dict` may grow, but only with JSON data.
2. **The compiler emits names. The planner builds schemas.** `SchemaInfo` and the Operators belong to
   the planner. The compiler gives it `fetch_columns`, `result_columns` and the snapshot.
   `compiler.py` imports no Operator type.
3. **Run-time inputs stay with the planner.** `yield_per` comes from `execution_options`, not from
   the statement, so the compiler cannot know it.
4. **The compiler owns the Scan fetch decision.** It fetches every column the recheck reads. One
   private compiler helper does the widening. It reads `recheck_where` and `fetch_columns`. It
   writes `fetch_columns` and `query_params['filter_properties']`, and only when it adds a column.
   `visit_select()` (plain SELECT path only), `visit_delete()` and `visit_update()` call it.
   The title-only step is not in the helper. `visit_delete()` and `visit_update()` do it
   themselves. The helper cannot choose it: a `select(table.c.object_id)` with no WHERE and a
   DELETE with no WHERE give it the same inputs, but only the DELETE fetches the title. For a
   SELECT, the projection decides the fetch, and the WHERE only adds to it. A DELETE or UPDATE
   reads no property, so with no WHERE its fetch is empty, and Notion reads an empty
   `filter_properties` as "all properties".
5. **`Compiled.fetch_columns()` is the widened list.** "Fetch" means what the `Scan` fetches.
   `PlanningContext.pre_widening_fetch_columns` holds what the user's `Row` needs, and `Project`
   trims to it.
6. **The snapshot comes before the widening.** `visit_select()` takes the
   `pre_widening_fetch_columns` snapshot first and widens after. In the other order `Project` keeps
   the widened columns, and they reach the user's `Row`.
7. **The move is a pure move.** The compiler reproduces today's Notion requests exactly. It keeps
   the `visit_select()` gate for the unwidened projection (send `filter_properties` only for a
   non-empty strict subset of the user columns). The widening and the title-only step then send
   `filter_properties` whenever they apply, as the planner's `narrows` gate did.

## Alternatives Considered

- **Keep the widening in the planner (ADR-0022 as written).** Rejected: the planner keeps rewriting
  compiled parameters, and the rule lives in two builders.
- **Widen in `visit_where_clause()`.** Rejected: the visitors call it only when the WHERE is
  pushable, and it returns a *pruned* filter. The widening must read the whole `recheck_where`,
  pushable or not.
- **The helper also does the title-only step, behind a flag from the caller (e.g. `mutation=True`).**
  Rejected: the helper gets a branch that SELECT never takes. The step belongs to the statements
  that need it.
- **Move only DML.** Rejected: the same rule would then live in two layers.
- **A new name for the widened list (e.g. `scan_columns`).** Rejected: it adds a third column list
  next to `fetch_columns` and `result_columns`. ADR-0022 already says "the *pre-widening*
  `fetch_columns`", which presumes that `fetch_columns` is the widened list.
- **Unify the two `filter_properties` rules.** Deferred, not rejected. Three cases send a different
  request (a DELETE/UPDATE with no WHERE on a title-only or title-less table, and a widening that
  reaches every user column). The rows are the same, but the request is not. That is a wire change
  and needs its own issue.
- **The compiler builds `SchemaInfo`.** Rejected: it couples the compiler to the Operators, the same
  reason #425 rejected planning on the statements.

## Consequences

- **The planner stops writing `query_params`.** `_plan_select()` and `_plan_modify()` lose the
  `narrows` gate, the `scan_params` copy and the title-only step. They read `fetch_columns()` and
  `ctx.parameters` as they are. The remaining shared code (schema, `Scan`, optional `Filter`) is
  short and differs in `yield_per`, so it gets no shared helper.
- **The engine needs no change.** `ExecutionContext.pre_exec()` already copies any
  `compiled_dict['query_params']` into the parameters the `Scan` sends. DML can now carry that key.
- **The cursor description widens.** `ExecutionContext` builds it from `fetch_columns()` and
  `result_columns()`. Every planned statement's `Scan` shapes the cursor again with its own schema,
  which already holds the widened columns, so nothing visible changes.
- **The compiler re-reads `_is_aggregate` and `_joins`** to pick the plain SELECT path. It did so
  already. #425's rule ("one classify step reads the statement-kind flags") applies to the planner.
- **Compile-level tests can now see the widening.** ADR-0022 noted that no test in `tests/unit/sql/`
  could, because the widening lived in the planner.
- **The join shape is not covered yet, and that is a known defect.** `_plan_join()` never widened,
  so a join WHERE on a column that the projection does not name returns no rows on either side
  (#433). The rule of item 4 applies to every shape with a recheck. The join fix applies it per
  side, through the same helper.
- **The aggregate shape has no recheck (#399),** so the helper does not run for it. *(Superseded
  by #399: the aggregate shape now has a recheck `Filter`, and the helper widens its `Scan`. The
  compiler widens the left `Scan` of every SELECT shape the same way. It reads `_is_aggregate` and
  `_joins` only to widen the right `Scan` of a join.)*
