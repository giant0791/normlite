# ADR-0005: Outer-join phantom rows obey SQL NULL semantics, not Notion empty-property semantics

**Status:** Accepted
**Date:** 2026-06-06

---

## Context

`Select.outerjoin(...)` preserves a left row that matched zero right rows by emitting a
single **phantom row**: the left columns carry real values, every right column is
None-filled (`merge_inner_join_rows` / `_project_join_row` in `sql/dml.py`). Phantoms are
common because the fake client validates relation *shape* but not FK *targets*
([ADR-0002](./0002-fake-client-lax-fk-validation.md)), so dangling references resolve to
"no matching right row" rather than an error.

Slice 5 (#307) routes a right-side `WHERE` predicate *out* of the phase-1
`databases.query` (the phantom is fabricated client-side and never exists in the right
database, so no server query can see it). Slice 6 (#308) must therefore answer the
right-side predicate **client-side, after the merge** — which forces a decision about what
a right-side predicate *means* when evaluated against a phantom.

Two empty-value models collide at this exact point:

1. **SQL NULL + three-valued logic.** An outer join's unmatched right columns are NULL.
   Any predicate on a NULL operand evaluates to UNKNOWN, and `WHERE` keeps only rows that
   are TRUE — so a right-side predicate drops the phantom. This is *type-independent*: NULL
   poisons `title`, `number`, `date`, `checkbox` identically.

2. **Notion empty-property.** Notion has **no NULL**. Every property has a concrete empty
   shape (`[]` for `title`/`rich_text`/`relation`, absent/`{}` for `date`) and Notion's own
   `is_empty` / `is_not_empty` operate on those concrete shapes. There is no honest empty
   representation for some types: a **checkbox is never empty** (always concretely
   `true`/`false`, default `false`), and the fake's `number` filter exposes no `is_empty`
   operator at all.

The first implementation attempt took model (2): a per-type `empty_value` table mapped each
None-filled cell to a fabricated Notion-empty shape, rebuilt a page, and fed it to the
fake's `_Filter` (`notion_sdk/client.py`). This works for `title`/`rich_text`/`relation`
(their empty shapes make `is_not_empty` → False) and *accidentally* for `date` (the
sentinel is rejected by `normalize_page_date`'s `isinstance(_, dict)` guard → None). It
**fails** for the other two:

- **number** — `EMPTY_NUMBER` (an `_EmptyType` sentinel) flows unguarded into
  `number.greater_than` / `number.less_than` (`lambda a, b: a > b`) and raises `TypeError`.
  The comparison lambdas have no empty-guard, unlike `title`/`rich_text` which already do
  (`False if a is EMPTY_TEXT else ...`).
- **checkbox** — `EMPTY_CHECKBOX` under `does_not_equal True` evaluates
  `EMPTY_CHECKBOX is not True` → **True**, so the phantom is **kept**. SQL says `NULL != True`
  is UNKNOWN → drop. Fabricating an empty checkbox invents a value Notion never has and
  produces the wrong answer.

## Decision

**A right-side predicate on an outer join is answered with SQL NULL semantics. A phantom
fails every right-side predicate by construction, decided structurally — not by
fabricating per-type Notion-empty values.**

- In the post-merge filter (`_right_side_passes` in `sql/dml.py`), a merged row whose right
  slice is entirely None-filled **is** the phantom and is dropped without building a page or
  invoking `_Filter`:

  ```python
  right_slice = merged_row[left_width:]
  if all(c is None for c in right_slice):
      return False          # phantom: NULL fails every right-side predicate
  # else: real right row, concrete on-wire shapes — hand to _Filter unchanged
  ```

- This rests on the invariant that **a Python `None` cell in a merged tuple originates only
  from the phantom None-fill** — real right rows carry concrete on-wire dicts
  (`{"title": []}`, `{"checkbox": false}`), never `None`. So `all(None)` ⟺ phantom ⟺
  "right entity absent" ⟺ drop. This invariant must be pinned by a test so a future
  result-processor that emits `None` for a present-but-empty property cannot silently
  re-classify a real row as a phantom.

- The per-type `empty_value` table and the `if cell is None` page-rebuild branch are
  **removed**. No empty values are fabricated. Only genuine right rows reach `_Filter`,
  with their real shapes, so the existing `_Filter` evaluation path is untouched.

This keeps the engine's reliance on the underscore-private `_Filter` confined to genuine
right rows, respecting the boundary flagged for a future shared-evaluator decision.

## Alternatives Considered

**A. Harden `_Filter`'s operator lambdas to treat `EMPTY_*` sentinels as NULL-like.**
Guard every comparison (`number.greater_than: lambda a, b: False if a is EMPTY_NUMBER else
a > b`; `checkbox.does_not_equal: False if a is EMPTY_CHECKBOX else a is not b`) so empties
poison predicates to False uniformly, mirroring the `EMPTY_TEXT` guards `title`/`rich_text`
already carry. **Deferred, not rejected** — but as a *separate* concern. The `number`
comparison `TypeError` is a pre-existing latent bug in the fake's own `databases.query`
(an empty number compared server-side crashes today, join or no join). Fixing it belongs to
its own slice with a red test against `databases.query` directly, and touches the shared
`_Filter` evaluator — which is ADR-worthy in its own right (see Consequences). Folding it
into the join slice would couple two unrelated decisions and pull the engine deeper into
underscore-private SDK internals.

**B. Fabricate per-type Notion-empty values (the first attempt).**
Rejected. It reconstructs model (2) to answer a model (1) question, invents values Notion
never has (empty checkbox), crashes for number, and works for date only by accident. It
also makes correctness *type-dependent* when the underlying semantics (NULL poisons
everything) are type-independent — so every new right-column type would need a new, easily
wrong, table entry.

**C. Pre-filter raw right pages before the merge.**
Rejected. Pre-filtering keeps phantoms (their None-fill is fabricated *after* the merge),
giving the wrong answer — which is the whole reason slice 5 routed the right-side predicate
out of phase-1 first. The predicate must be applied **post-merge**.

## Consequences

- `_right_side_passes` becomes type-independent and shrinks: no `empty_value` /
  `type_mapper` empty branch, no fabricated page for phantoms. The phantom drop is one
  `all(... is None)` check.
- A new invariant — "`None` in a merged tuple ⟺ phantom" — becomes load-bearing and must be
  guarded by a test. If a future `result_processor` emits Python `None` for a present
  property, that test fails loudly instead of the join silently dropping a real row.
- `is_empty` on a right column now distinguishes **absent entity** (phantom → UNKNOWN →
  drop) from **present entity with an empty property** (real row → `_Filter` → may match).
  These land on opposite sides of `is_empty`, matching SQL. Worth an explicit test.
- The fake's `_Filter` still cannot evaluate a comparison against a genuinely-empty
  `number` without `TypeError`, and treats checkbox/number empties inconsistently. That
  debt is now *named and isolated* (Alternative A) rather than hidden inside the join. A
  follow-up ADR should decide whether to promote `_Filter` to a shared, backend-agnostic
  evaluator with documented NULL/empty semantics before the engine leans on it further.
- Real Notion filters server-side and never sees a phantom; this decision only governs the
  client-side post-merge step that the outer join introduces, so it does not drift from
  real-API behaviour for genuine rows.
