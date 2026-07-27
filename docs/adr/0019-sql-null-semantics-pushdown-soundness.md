# ADR-0019: SQL NULL semantics, `is_null()` vs `is_empty()`, and pushdown soundness

**Status:** Accepted — supersedes [ADR-0005](./0005-outer-join-phantom-null-semantics.md)
**Date:** 2026-07-15

> **Correction (2026-07-27) — the motivating example is factually wrong; the decision stands.**
>
> **(1) Real Notion answers `is_empty` TRUE on `{"rich_text": [{"text": {"content": ""}}]}`.** The
> Context table below said FALSE. That cell was never measured — it was inferred from the fake
> client's `_Filter`, which is a model, not evidence. A read-only probe against the live API
> (`src/tools/notion_probe.py`, `Notion-Version: 2026-03-11`,
> `POST /v1/data_sources/{id}/query`) matched **all six** rows holding blank *and* empty-array
> cells with `is_empty`, and **none** with `is_not_empty`. Notion's `is_empty` is a test on the
> **content** the cell spells out, not on the length of the array spelling it. `eval3` was wrong in
> the same way and is fixed (#365); `_Filter` still carries the bug (**#381** family).
>
> **(2) So the "the decode is lossy" argument does not hold as written.** Its two cells do *not*
> differ under `is_empty` — they agree, and both decode to `""`. A `rich_text`-only evaluator over
> decoded text could in fact reproduce `is_empty` for these two cells. The argument is replaced,
> not repaired; see (3).
>
> **(3) The raw cell is required for a structural reason instead: it carries the Notion type tag.**
> Emptiness is **per-type** — a number is empty when it is `null`, a text when its plain text is
> `""`, a relation when it holds no items — and there is no single expression over a decoded value
> that answers all three (`0` and `False` are falsy but *not* empty; `[]` and `""` are). The
> evaluator dispatches on `"<col_spec>.<op>"`, and `<col_spec>` **is** the raw cell's key. Decoding
> to a Python scalar erases which rule applies before the rule is chosen. This does not rest on any
> single disputed cell, and it is the argument the Decision below actually needs.
>
> **(4) The `None` vs `{...}` distinction — the "distinguishable **at raw level**" bullet under
> `is_null()` in **Decision** — is untouched and still load-bearing.** It is a
> different claim from the lossy-decode one — a phantom's cell is literally `None` while a real
> empty cell is a dict — and the probe confirms its premise rather than denying it: **no
> `absent-property` cell ever arrives from Notion**, since a query response always carries every
> property. `eval3`'s absent-cell UNKNOWN guard is therefore reachable *only* from normlite's own
> outer-join phantom.
>
> **(5) Never cite Notion's formula surface as evidence about its filter surface.** The formula
> `.equal("")` *does* separate a blank cell from `[]`; the filter `equals("")` separates nothing —
> it is discarded and returns the data source unfiltered (**#382**, a live silent wrong-rows bug).
> Reading formula behaviour as filter behaviour produced a wrong conclusion here once already.
>
> The corrected sentences are marked **[corrected]** inline. Nothing in **Decision** changes: the
> Filter Operator still evaluates raw cells, `is_empty()` stays Notion-semantic and pushable, and
> `is_null()` stays SQL-semantic and never pushed.

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

And the fidelity is necessary, because **the decode erases the type tag** — **[corrected]**, see
Correction (1)–(3); this paragraph and its table originally argued from a lossy *value* decode and a
cell whose verdict was measured to be the opposite:

| Notion raw cell | `is_empty` (Notion) | provenance | decodes to |
|---|---|---|---|
| `{"rich_text": []}` | **TRUE** | measured | `""` |
| `{"rich_text": [{"text": {"content": ""}}]}` | **TRUE** **[corrected]**, was FALSE | measured | `""` |
| `{"number": null}` | **TRUE** | measured | `None` |
| `{"relation": []}` | **TRUE** | measured | `[]` |

Emptiness is **per-type**: a number is empty when it is `null`, a text when the plain text it spells
out is `""`, a relation when it holds no items, a date when it has no start instant. Those are four
different rules, and picking the right one requires knowing the property's Notion type. The raw cell
is where that type is written — it *is* the key — and the evaluator's dispatch is literally
`"<col_spec>.<op>"`. A client-side evaluator over **decoded** values has already thrown the key away
by the time it must choose a rule, so it could not reproduce `is_empty`, and pushdown parity would
break. Note that the decoded column alone cannot stand in for the type either: `""` above could have
come from a `rich_text` or a `title`, which are distinct Notion operators.

> **Unsettled — `{"number": 0}` and `{"checkbox": false}`.** Notion's documentation describes an
> empty value as one "equating to empty — `0`, `false`, `""`, `[]`", which read literally makes a
> zero **empty**. normlite models both as NOT empty (`number.is_empty` is `a is None`, pinned by
> `test_is_empty_on_zero_cell_is_false`). **Neither was probed** — the number probe carried a
> `null`, never a `0`. This is the same shape as the bug Correction (1) fixed, so treat the FALSE as
> normlite's model, not as Notion's answer, until a probe settles it.

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
