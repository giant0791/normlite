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

> **Correction (2026-07-29) — `{"<col_spec>": {}}` is not a cell. The decision stands.**
>
> **(6) A valueless cell is `{"<col_spec>": null}`, and that is the only shape it takes.** The
> evaluator additionally counted `{}` as valueless (`_has_no_value(val) = val is None or val == {}`).
> That arm was never grounded in Notion. It was added so the evaluator would agree with a **page
> generator** that emitted `{"date": {}}` as its unset-date shape — and the generator had, in turn,
> modelled a cell on a **property definition**.
>
> **(7) The two positions are different objects that share a shape.** In a *data source's*
> `properties`, `{"date": {}}` is a **column declaration** — "this column is a date, with empty
> configuration" — and is canonical, real and load-bearing (`TypeEngine.get_notion_spec()`, the DDL
> compiler, the system catalog). In a *page's* `properties` it is a **cell**, and there it is
> **unproducible**. Both halves measured 2026-07-29: the API **rejects** `{"date": {}}` on
> `POST /v1/pages` with a 400, and clearing a date through the Notion **UI** — the more permissive
> path — stores and emits `"date": null`, read back from a live data source
> (`src/tools/notion_probe.py`). Nothing can produce the shape.
>
> **(8) So the `== {}` arm is removed, together with the fiction that motivated it.** This is a
> deletion that is **semantics-preserving over the reachable domain** — every cell that can actually
> arrive gets the identical verdict — and it is *not* a reopening of the Decision below. The
> generator is changed to emit `null`, and the evaluator tests that pinned `{}` are rewritten onto
> `null` rather than deleted: the behaviour they protect (a valueless date is UNKNOWN, so
> `does_not_equal` does not match it) is the heart of **#384**. *(Decision recorded ahead of the
> code; both land on `bug/issue-384/notion-does-not-equal`.)*
>
> **(9) The rule this cost us, stated once: never model a cell on a schema object.** An empty
> *config* is not an empty *value*. The two are distinguished by **position**, never by shape, so a
> shape copied out of a `CREATE TABLE` payload carries no evidence about what a query returns. This
> is the same failure as Correction (1) and (5) — a **model** read as **evidence** — and it is now
> the third instance in this ADR's area.
>
> **(10) Corollary, and it is the invariant to hold onto:** a raw cell decodes to Python `None`
> **iff** the evaluator calls it valueless. `None` in a decoded `Row` *is* how a user observes SQL
> NULL, so a disagreement between the decode layer and the evaluator is user-visible — `WHERE col =
> x` dropping a row as UNKNOWN while `SELECT col` yields a value from that same cell. The two stay
> separate functions (`TypeEngine._is_valueless_cell` takes the whole cell, `eval3._has_no_value`
> takes the inner value); this is their contract, not an argument to merge them.

> **Correction (2026-07-30) — the pushdown-soundness invariant is `⊇`, not equality. The decision
> stands; the last Consequences bullet does not.**
>
> **(11) "If a predicate's Notion-side and client-side evaluations can disagree, it must not be
> pushable" is too strong, and taken literally it forbids something correct.** Written when the
> residual was `_Filter` — Notion-semantic on both sides, so equality was free. This ADR itself
> replaced the residual with `eval3`, which is **SQL** three-valued. The two sides are now
> deliberately different semantics, so they *will* disagree, and the rule as stated would make
> `number.does_not_equal` unpushable — a real narrowing, for no gain.
>
> **(12) The invariant that actually holds is directional**, because the residual is **always
> re-applied** and therefore decides the answer:
> `{rows the pushed filter keeps} ⊇ {rows the residual keeps}`. A row the push keeps and the
> residual drops is **slack** — safe, paid for in transfer. A row the push *drops* that the residual
> would have kept is unrecoverable. Only the second is a violation. The revised rule: **a pushable
> predicate's pushed form must never be *narrower* than its residual form.** Disagreement in the
> other direction is permitted and expected.
>
> **(13) This is now measured, not argued.** With the generator widened to all 36 declared pairs
> (#381), across 24 000 leaf evaluations: **exactly one** pair diverges — `number.does_not_equal`
> over `{"number": null}` — in **exactly one** direction (Notion keeps, `eval3` drops), on **exactly
> one** cell shape, and within that pair divergence holds **iff** the cell is valueless (396 valued
> cells agree, 104 valueless cells diverge, no exceptions either way). Zero ⊇ violations at leaf or
> compound level. That is #384, and under the corrected invariant it is slack rather than a defect.
>
> **(14) Text does not join it, and the reason is load-bearing.** `title`/`rich_text`
> `does_not_equal` agree exactly, because `[]` is a **present** value (`4abe0d1`) rather than a
> valueless cell, so `eval3` reaches its rule and answers TRUE just as Notion does. Correction (9)'s
> ruling and the complement law arrive at the same place independently.

> **Correction (2026-07-31) — two bullets in Decision/Consequences describe code that was never
> written. See [ADR-0022](./0022-lossy-pushdown-and-the-recheck.md).**
>
> **(15) "The `all(None)` structural guard is deleted" is false.** The guard is live in
> `Filter._right_side_passes` (`sql/queryplan.py`), comment and all, and slice 2 never removed it.
> This ADR has been **Accepted** while asserting the opposite, which cost a session to discover.
> The claim is *achievable* — measured 2026-07-31, deleting it leaves the suite unchanged at 872
> passed, on a path that **is** covered (an outer join with a dangling FK plus a right-side
> `is_empty()`), because a phantom's cells are literally `None`, `eval3` returns UNKNOWN, and the
> WHERE policy drops UNKNOWN. ADR-0022 does the deletion.
>
> > **Closed 2026-07-31.** The guard is gone — ADR-0022 step 1 deleted it from
> > `Filter._right_side_passes`, suite unmoved at 908 (872 plus the 36-pair safety net written
> > first). This ADR's Decision bullet is now true of the code. Correction (15) is kept as the
> > record that it was asserted for two weeks before it was.
>
> **(16) "The residual is always re-applied" was never true either, and it is what makes ⊇
> meaningful.** A conjunct is pushed **xor** evaluated client-side today, so the two sides of ⊇
> ranged over *different predicates*: it is a property of a **predicate** (what the fuzz measures)
> and not of an **execution**. ADR-0022 adds the client-side re-application — the **Recheck** — that
> the wording already assumed, and renames `residual_where` → `recheck_where` accordingly.
> `residual_sorts` keeps its name: a held-back sort key is never pushed and never re-applied.
>
> **(17) The lesson is the one this ADR keeps re-learning.** Corrections (1), (5) and (9) record a
> *model* read as *evidence*. This is the neighbouring failure: a **document** read as *code*. An
> ADR marked Accepted is evidence about a decision, never about an implementation.

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
| `{"number": 0}` | **FALSE** | measured | `0` |
| `{"relation": []}` | **TRUE** | measured | `[]` |

Emptiness is **per-type**: a number is empty when it is `null`, a text when the plain text it spells
out is `""`, a relation when it holds no items, a date when it has no start instant. Those are four
different rules, and picking the right one requires knowing the property's Notion type. The raw cell
is where that type is written — it *is* the key — and the evaluator's dispatch is literally
`"<col_spec>.<op>"`. A client-side evaluator over **decoded** values has already thrown the key away
by the time it must choose a rule, so it could not reproduce `is_empty`, and pushdown parity would
break. Note that the decoded column alone cannot stand in for the type either: `""` above could have
come from a `rich_text` or a `title`, which are distinct Notion operators.

**The `{"number": 0}` row is the sharpest of the five, and it was measured to settle a doubt.**
Notion's documentation describes an empty value as one "equating to empty — `0`, `false`, `""`,
`[]`", which read literally makes a zero empty — and since that same wording turned out **true** of
text (Correction (1)), there was every reason to expect it true of numbers too. It is not: probed
against a live `{"number": 0}` cell, `is_empty` matched **nothing** while `is_not_empty` matched it,
with `equals(0)` discriminating it from the non-zero rows to prove the condition ran.

So the "equating to empty" rule is itself **type-dependent on the filter surface**: `""` in a text
is empty, `0` in a number is not. There is no falsiness rule that spans the types — which is the
type-tag argument again, this time measured rather than reasoned.

> **Still unprobed: `{"checkbox": false}`.** The same doubt applies, but it is off normlite's path —
> `Boolean.supported_ops` declares only `equals` / `does_not_equal`, so there is no
> `checkbox.is_empty` for normlite to be wrong about.

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
- **Pushdown soundness is now a named invariant** any future pushable operator must satisfy: a
  pushable predicate's **pushed form must never be narrower than its residual form**
  (`pushed ⊇ residual`). Disagreement in the other direction — the push keeping rows the residual
  then drops — is **slack**, and is permitted. *(Superseded wording: this bullet originally said
  any disagreement made a predicate unpushable. See Correction (2026-07-30), items (11)–(12).)*
