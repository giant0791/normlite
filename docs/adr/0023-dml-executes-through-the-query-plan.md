# ADR-0023: DELETE and UPDATE execute through the Query plan (ModifyTable)

**Status:** Accepted — supersedes the DML parts of
[ADR-0020](./0020-execution-layering-sql-builds-engine-drives.md) Consequences and extends
[ADR-0018](./0018-query-plan-operator-tree.md); required by
[ADR-0022](./0022-lossy-pushdown-and-the-recheck.md)
**Date:** 2026-10-11

> Accepted in #413, on the commit that deletes `ExecutionStyle.EXECUTEMANY`. DELETE moved in #411,
> UPDATE in #413.

---

## Context

ADR-0022 decides that **every** WHERE conjunct is re-checked client-side, and that the recheck
decides the answer. Only `SELECT` complies. `DELETE` and `UPDATE` use **Two-Phase Execution**:
`_setup_execution` sends the compiled `data_sources.query`, calls `fetchall()`, and fills
`bulk_parameters`; `Connection._execute_many` then sends one `pages.update` per page. No recheck
runs, so `SELECT ... WHERE p` and `DELETE ... WHERE p` can select different rows (#397).

The recheck lives in the **Filter** Operator, so a DML statement must run through a **Query plan**
to get it. But a plan only reads: no Operator writes. The engine drives a plan *or* the bulk channel,
never both, so a DML plan needs a place for its writes.

Two facts constrain where the writes go:

- **The Halloween problem.** The fake client paginates by offset over a result it computes again on
  every request, and it drops trashed pages (`notion_sdk/client.py:170-186`, `:234`). A DELETE that
  trashes batch 1 and then asks for offset 100 skips the next 100 rows, with no error. An UPDATE
  that changes a filtered or sorted column can skip or repeat rows. Today's code is safe only
  because `fetchall()` drains the scan before the first write. Real Notion cursors are not
  measured under concurrent writes.
- **RETURNING reads the write response.** `pages.update` returns the full page. Today RETURNING
  rows come from those responses, not from the scan. The DELETE Scan fetches only what the recheck
  reads, or only the title (#410), so the scan rows cannot serve RETURNING.

## Decision

**DELETE and UPDATE execute through the Query plan. A new Operator, `ModifyTable`, sits at the
root, above `Scan -> [Filter]`, and performs the writes. Two-Phase Execution is retired statement by
statement: DELETE in #411, UPDATE in #413.**

1. **One operator for both statements.** `ModifyTable` does not know which statement it serves.
   The name avoids a collision with the `Delete` / `Update` DML constructs.
2. **The writes are complete when `open()` returns.** `open()` drains the child, sends every
   `pages.update`, and keeps the responses. `next()` only gives back the kept responses. Draining
   first prevents the Halloween problem. Writing in `open()` means no consumer (streaming, early
   close) can leave a DELETE or UPDATE half done. `ModifyTable` is a **Blocking operator** that also
   completes its side effects before its first batch.
3. **RETURNING reads the write response, never the scan.** `ModifyTable` yields rows built from the
   `pages.update` responses, in the RETURNING schema. This gives the **new** values for UPDATE and
   the last values for DELETE. The Scan never widens for RETURNING.
4. **One row per written page, always.** With no RETURNING the operator still yields the rows, so
   `rowcount` stays the number of written pages. The statement's `_finalize_execution`, not the
   operator, decides if the caller sees them: it soft-closes when there is no RETURNING, and it
   reads implicit returning from the `object_id` column of the rows that `ModifyTable` yields.
   - **With no RETURNING**, each row is keyed by `object_id` at column 0, because `ResultSet`
     reads the ids by position (`itemgetter(0)`).
   - **With RETURNING**, each row holds exactly the RETURNING columns, in their order, as for
     INSERT … RETURNING. Implicit returning does not run, so nothing reads column 0, and
     `rowcount` still counts the rows.

   `ModifyTable` reads the `object_id` for each write from its **child's** schema, never from the
   rows it yields. The engine never reads the `ModifyTable` cursor: the plan closes it before
   `_finalize_execution` runs.
5. **The request is data.** The compiler emits the write operation and the payload template for
   both statements (operation: `pages.update`; DELETE payload: `{"in_trash": True}`; UPDATE
   payload: the existing `update_payload`, ADR-0001). The operation belongs with the payload. The Planner binds
   it **once** and gives `ModifyTable` the operation and the bound payload. `ModifyTable` adds only
   `{"page_id": object_id}` per row, and it holds no `ExecutionContext` — the same shape as `Scan`.
6. **Not atomic.** A failed write raises at once through the default `errorhandler`, and the writes
   before it stay. This is today's behaviour. Atomicity belongs to transaction support
   (`Connection.commit()` / `rollback()`), not to an Operator.
7. **Mutations never stream.** The engine streams only when `elem.is_select` — the flag
   `_execute_single` already reads, not an inspection of the plan. The DELETE/UPDATE Scan uses the
   default page size and does not take `yield_per`, as the aggregate and join Scans do.
8. **The hooks shrink.** `_setup_execution` becomes a no-op, as for `Select`.
   `_finalize_execution` keeps only result interpretation (item 4); `_staged_result_cursor` routing
   goes. `ExecutionStyle.EXECUTEMANY` and `Connection._execute_many` are deleted in #413, when their
   last user moves.

## Alternatives Considered

**A. Hybrid: the plan reads, the bulk channel writes, driven from `_setup_execution`.** Rejected.
`_setup_execution` would drive the plan and then fill `bulk_parameters`. That is ADR-0020
Alternative B: SQL drives the plan, so the `SQL → ENGINE` reach stays.

**B. Hybrid: the engine drains the plan, then calls `_execute_many`.** Rejected. The engine would
have to ask which statement it runs and convert the rows to bulk parameters. That is ADR-0020
Alternative A: an engine-side type switch on SQL knowledge.

**C. A lazy `ModifyTable` that writes one batch per `next()`; Halloween in a separate issue.**
Rejected. It skips rows as soon as a table has more rows than one page (Context), which is a silent
wrong answer that today's code does not have. Deferring the fix ships a regression.

**D. Drain in `open()`, write in `next()`.** Rejected. It avoids the skip, but the side effects then
depend on the consumer: a caller that stops reading a streamed `DELETE ... RETURNING` deletes only
part of the table.

**E. Yield the child's rows.** Rejected. The Scan does not fetch the RETURNING columns, and for
UPDATE the child holds the **old** values where SQL returns the new ones.

**F. Separate `Delete` and `Update` operators.** Rejected. The names collide with the DML
constructs, and both would repeat the drain-first rule, which is the part that matters most.

**G. Compensate on failure (send `in_trash: False` to undo).** Rejected. It is a fake transaction:
the undo can fail too, and for UPDATE it needs old values that a narrowed Scan never fetches.

## Consequences

- DML gets the recheck. #412 (DELETE) and #413 (UPDATE) only need the `Filter` that the Planner
  already places under `ModifyTable`; the DML pushability `CompileError` can then go (#414, #417).
- **The request is row-independent only because `SET` takes literal values** (`.values()` holds
  only `BindParameter`s). If normlite supports `SET col = col + 1`, the payload depends on the row
  and item 5 must be revisited.
- A DELETE plan now has side effects when it is opened. A test that opens a DELETE plan to inspect
  the scan rows trashes pages; such a test must read the `ModifyTable` child.
- `ModifyTable` keeps every response in memory. Today's code keeps every page too (`fetchall()`
  and one result set per response), so the cost does not change.
- The `bulk_operation` / `bulk_parameters` channel stays for INSERT…RETURNING
  (`Insert._post_fetch_inserted_row` calls `context.engine.do_executemany` itself). That is a known
  ADR-0020 violation, out of scope here: it has no WHERE and no scan, so neither the recheck nor the
  Halloween problem applies.
- ADR-0020's Consequences said DELETE / UPDATE "carry no risk from this work" and that the bulk
  channel is "untouched throughout". That held for the `SELECT` work and is superseded here.
