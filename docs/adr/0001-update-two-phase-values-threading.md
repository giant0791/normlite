# ADR-0001: Threading UPDATE values through the two-phase execution pipeline

**Status:** Accepted  
**Date:** 2026-05-03

---

## Context

`Update` follows the same two-phase execution pattern as `Delete`:

1. `_setup_execution` calls `databases.query` to collect the Notion page IDs of affected rows.
2. `_execute_many` calls `pages.update` on each collected page ID.

Unlike `Delete`, which sends a hardcoded `{"in_trash": True}` payload, `Update` must send the
user-supplied column VALUES as the `properties` payload for each `pages.update` call. These values
are known at compile + bind time, but the page IDs are only known after the query runs. This
creates a tension: the VALUES need to be type-processed (via `bind_processor`) before execution,
yet they cannot be merged into the query-filter binding step that runs in `pre_exec`.

## Decision

**`visit_update` emits the VALUES template under a separate `'update_payload'` key in `compiled_dict`.**

`pre_exec` binds the query-filter params normally (populating `context.payload` and
`context.path_params`). The remaining unbound params — the column VALUES `BindParameter` objects —
are stored in a new `context.resolved_params` attribute after the query-filter params are consumed.
The `_assert_all_params_consumed` guard is skipped for `is_update` statements because VALUES params
are intentionally left for `_setup_execution`.

`Update._setup_execution` then:
1. Executes the `databases.query` and collects matching pages.
2. For each page, shallow-copies `context.resolved_params`, injects the `page_id`, and calls
   `context._bind_params(compiled_dict['update_payload'], params_copy)` to produce the bound
   properties payload.
3. Builds `bulk_parameters` as `[{"path_params": {"page_id": ...}, "payload": {"properties": ...}}]`.

## Alternatives Considered

**A. Bind VALUES in `pre_exec`, store in `context.bulk_parameters` template.**  
Rejected: `bulk_parameters` requires page IDs, which are only available after the query. Pre-populating
it with a template would require a second mutation pass in `_setup_execution`, obscuring
ownership of the binding step.

**B. Bind VALUES lazily inside `_setup_execution` by accessing `invoked_stmt._values` directly.**  
Rejected: This bypasses the compile/bind pipeline and duplicates type-processing logic that belongs
in the compiler and `_bind_params`. It also makes `_setup_execution` responsible for value binding,
violating the separation between `pre_exec` (what to send) and `_setup_execution` (to whom).

**C. Add a second `'update_payload'` key and bind it fully in `pre_exec`, storing in a `context.update_payload` attribute.**  
Viable but unnecessary: `pre_exec` would bind the template into a ready-to-use dict, avoiding the
`resolved_params` pattern. Rejected because it still cannot inject the `page_id` (a DBAPI_PARAM
with `_NoArg.NO_ARG` at bind time), requiring `_setup_execution` to patch it in anyway. The
`resolved_params` approach handles both VALUES and `page_id` uniformly.

## Consequences

- `compiled_dict` for `Update` has two payload keys: `'payload'` (query filter) and `'update_payload'` (VALUES template).
- `ExecutionContext` gains a `resolved_params` attribute (populated for `Update`; unused by other statements).
- `_assert_all_params_consumed` is gated on `not is_update`.
- `_compile_update_values` is a new compiler method that processes only the columns present in `_values` (no completeness check), distinct from `_compile_insert_update_values`.
