# ADR-0019: SQL NULL semantics, `is_null()` vs `is_empty()`, and pushdown soundness

**Status:** Accepted — supersedes [ADR-0005](./0005-outer-join-phantom-null-semantics.md)
**Date:** 2026-07-15

---

## Context

[ADR-0005](./0005-outer-join-phantom-null-semantics.md) ruled that an outer join's **phantom** (an
all-`None` right slice) **fails every right-side predicate**, enforced *structurally* — `if all(c is
None for c in right_slice): return False` (`dml.py:1404`) — before the predicate is evaluated at
all. Two documented boundaries fell out of it:

- **[Anti-join inexpressible]** — `LEFT JOIN ... WHERE right IS NULL` cannot be written, because
  there is no `IS NULL`, `is_empty ≠ IS NULL`, and *any* right-side WHERE drops the phantom.
- **[Unreachable empty-title]** — a real `title=None` row is **mislabelled a phantom** by
  `all(None)`. ADR-0005's own consequences call this "the first crack in the `None ⟺ phantom`
  invariant".

That rule was never a considered semantic position; it was a **workaround for the evaluator
available at the time**. `_Filter` returns `bool` and speaks Notion's filter language, which has no
way to say "there is no row here" — so the drop was hard-coded outside the predicate logic.

Notion tolerates value-less properties: an omitted `rich_text` stores `[]`, an omitted number stores
`null`. So under SQL semantics nearly every column is nullable, and normlite has been modelling
NULL's absence as a Notion fact when it is really an evaluator limitation.

**The constraint that shapes the answer.** [ADR-0018](./0018-query-plan-operator-tree.md) gives
residual predicates a real home (the `Filter` Operator), which raises a soundness question ADR-0005
never had to face: a `Planner` decides *per predicate* whether it is pushed (evaluated Notion-side)
or residual (evaluated client-side). **If those two evaluations disagree, the result depends on a
planner decision the user cannot see.** This invariant holds today only because both sides are
Notion-semantic over **raw cells** — `_right_side_passes` re-wraps raw cells into a synthetic page
precisely to preserve that fidelity.

And the fidelity is necessary, because **the decode is lossy**:

| Notion raw cell | `is_empty` (Notion) | decodes to |
|---|---|---|
| `{"rich_text": []}` | **TRUE** | `""` |
| `{"rich_text": [{"text": {"content": ""}}]}` | **FALSE** | `""` |

Both collapse to `""` via `rich_text_to_plain_text` (`getters.py:93`). A client-side evaluator over
**decoded** values could not reproduce `is_empty`, and pushdown parity would break.

## Decision

**Adopt full SQL three-valued semantics, and split the overloaded notion of "empty" into two
operators — one Notion-semantic and pushable, one SQL-semantic and never pushed.**

- **`is_empty()` — Notion-semantic, PUSHABLE.** Unchanged meaning: "the property holds no value"
  (`{"rich_text": []}`, `{"number": null}`). Maps to Notion's `is_empty` filter op
  (`type_api.py:383`) and rides into the `Scan` payload.

- **`is_null()` — SQL-semantic, NEVER PUSHED.** "There is no value *here*": the cell is literally
  `None` — an outer join's unmatched right slice, or an absent property. Notion cannot express "this
  row had no join partner", so `is_null()` is **always residual**. Being unpushable *by
  construction*, it has no pushdown parity to violate.

  This is implementable because the two are distinguishable **at raw level**: a real empty cell is a
  **dict** (`{"rich_text": []}`, `{"number": None}`); an unmatched right slice is **literally
  `None`**.

- **The Filter Operator evaluates raw Notion cells**, not decoded values — the shape the rows carry
  through the plan (decoding happens later, at `Row` / `CursorResult`). Preserving raw cells is what
  keeps pushdown sound.

- **The evaluator returns TRUE / FALSE / UNKNOWN — never `bool`.** Its callers apply **opposite**,
  both-SQL-correct policies: `CheckConstraint` rejects only on FALSE (UNKNOWN → accept);
  `WHERE` / `Filter` keeps only on TRUE (UNKNOWN → drop). A `bool` silently bakes in one caller's
  policy and breaks the other. `is_null()` is not a comparison: it returns TRUE/FALSE, never
  UNKNOWN.

- **The `all(None)` structural guard is deleted.** ADR-0005's outcome is now *derived*: a phantom's
  cells are `None`, so any comparison on them is UNKNOWN → dropped by WHERE.

- **There are two client-side evaluators, and that is correct.**
  [ADR-0012](./0012-checkconstraint-client-side-enforcement.md)'s works on **pre-bind Python
  values** *before* a write; the plan's `Filter` works on **raw cells** *after* a read. They share
  the backend-agnostic `Operator` enum and the three-valued logic — not an implementation. An
  earlier draft of this design proposed unifying them; that was wrong, because the shapes and the
  times differ.

## Alternatives Considered

**A. `is_empty()` simply becomes SQL `IS NULL`.** Rejected. One verb is simpler, but it silently
changes what every existing `is_empty()` call means, **un-pushes** a predicate that currently
narrows Notion-side (a real performance regression: full scan, then filter client-side), and
removes any way to express Notion-emptiness.

**B. Stay Notion-semantic everywhere; no SQL NULL.** Rejected. It preserves pushdown parity
trivially and risks nothing — but it abandons the ADR-0005 revisit, leaves anti-join inexpressible,
and reduces the `Filter` Operator to today's code in a new box.

**C. Evaluate residuals over decoded Python values** (as ADR-0012's evaluator does). Rejected: the
decode is lossy (above), so `is_empty` becomes unreproducible client-side and pushdown parity
breaks. This is the reason the two evaluators stay separate.

**D. Preserve ADR-0005 bit-for-bit and keep the `all(None)` guard.** Rejected as the *end state*,
but adopted as the **slice-1 checkpoint**: ADR-0018 lands the structure with `_Filter` and the guard
wrapped verbatim, so the existing suite stays a true oracle; this ADR is slice 2.

## Consequences

- **Anti-join becomes expressible** — `select(s, c).outerjoin(...).where(c.c.title.is_null())` —
  closing the [anti-join inexpressible] boundary that ADR-0005 declared closed for good.
- **The [unreachable empty-title] boundary closes**: with the `all(None)` guard gone, a real
  `title=None` row is no longer mislabelled a phantom. It is still dropped by a comparison — but
  now for the correct reason (UNKNOWN), and `is_null()` can now find it.
- **This is a deliberate behaviour change.** The existing join suite stops being a bit-for-bit
  oracle for right-side filtering; the semantics need their own tests. This is why it is a separate
  slice from ADR-0018 — a structural bug and a semantic bug must not be indistinguishable in one
  diff.
- **`dml.py` stops importing the fake client's internals.** `_Filter` lives in
  `notion_sdk/client.py` and exists *only* to simulate Notion-side filtering; a real Notion
  integration would have deleted the join's evaluator out from under it. normlite now owns a
  raw-cell evaluator. Note the fix was **not** to abandon raw cells — the re-wrapping in
  `_right_side_passes` was preserving fidelity, not being lazy.
- **`title` NOT NULL is noted but out of scope.** The `title` property is the one non-nullable
  Notion property, so the SQL-faithful move is for `Insert` to reject `None` for the `is_title=True`
  column. That is an `Insert`-side constraint concern — a sibling of the deferred NOT-NULL
  constraint under ADR-0012 — not a query-planning one. Partial enforcement already exists at the
  fake-client level (`dml.py:1252`).
- **Pushdown soundness is now a named invariant** any future pushable operator must satisfy: if a
  predicate's Notion-side and client-side evaluations can disagree, it **must not be pushable**.
