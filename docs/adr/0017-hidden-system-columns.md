# Hidden system columns are excluded from the public column surface

Some system columns are captured for routing/metadata but must never be user-visible.
`data_source_id` (the two-ID identity's operational id, per
[ADR-0014](0014-data-source-two-id-identity.md)) and `table_name` are carried in
`Table._sys_columns` and read by name (`get_data_source_id()`), yet a SQL user never selects them and
a Notion *page* has no such key. We therefore treat them as **hidden system columns**: kept in
`_sys_columns` for value capture, but **excluded from the public `table.c` view** and from every query
projection and page-result description.

This consolidates the exclusion at its source (`table.c`), replacing the earlier per-consumer filter
in the SELECT projection (`e0647cd`). It keeps one coherent invariant — `data_source_id ∉ table.c` —
so that `select(table)`, `returning(*table.c)`, page-result descriptions, and the primary key
(`primary_key=False`, per the sibling decision) all agree without scattered special-casing.

## Considered options

- **Keep `data_source_id` as a full public column in `table.c`** and filter it out at each consumer.
  Rejected: it leaks routing plumbing into the user-facing column surface and forces every consumer to
  remember the exclusion.
- **Make `data_source_id` a bare private attribute** (not a system column at all). Rejected: it would
  special-case the value-capture path that uniformly populates `_sys_columns` during CREATE
  finalization; the hidden-system-column category already exists for `table_name`.
