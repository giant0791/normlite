# tests/unit/sql/test_eval3.py
# Copyright (C) 2026 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""Axioms for the three-valued raw-cell evaluator ``eval3`` (ADR-0019).

``eval3`` returns TRUE / FALSE / UNKNOWN — never ``bool``. Its callers apply
opposite, both-SQL-correct policies to UNKNOWN, so the third value must be a
distinct, first-class result no ``bool`` evaluator can produce.
"""
from datetime import date

import pytest

from normlite.sql.elements import Operator
from normlite.sql.eval3 import eval3, TRUE, FALSE, UNKNOWN, _has_no_value
from normlite.sql.schema import Column
from normlite.sql.type_api import (
    Boolean,
    Date,
    Float,
    Integer,
    Money,
    Number,
    Numeric,
    Relation,
    String,
    TypeEngine,
)

_DID_NOT_DECODE = object()
"""Marker for a cell the decoder refused outright.

Distinct from ``None`` on purpose: ``None`` is the decoded form of SQL NULL,
so collapsing "raised" into it would erase the very distinction under test.
"""


def test_comparison_against_null_cell_is_unknown():
    """SQL 3VL axiom: comparing a value against a NULL cell is UNKNOWN.

    A present-but-valueless raw cell (``{"number": None}`` — the shape Notion
    echoes for a property that holds no value) has nothing to compare, so
    ``a == 1`` is neither TRUE nor FALSE. No ``bool`` evaluator can express
    this, which is precisely why ``eval3`` must return a distinct UNKNOWN.
    """
    a = Column("a", Integer())
    predicate = a == 1
    cells = {"a": {"number": None}}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_comparison_against_matching_cell_is_true():
    """A comparison against a present, matching raw cell is TRUE.

    ``a == 1`` over ``{"number": 1}`` has a real value to compare and it
    satisfies the predicate, so ``eval3`` must return the TRUE singleton —
    never implicit ``None`` and never ``bool``.
    """
    a = Column("a", Integer())
    predicate = a == 1
    cells = {"a": {"number": 1}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_not_of_unknown_is_unknown():
    """Kleene NOT axiom: ``NOT UNKNOWN = UNKNOWN``.

    This is the row that separates three-valued logic from boolean: negating
    an UNKNOWN leaves it UNKNOWN, never flipping it to TRUE/FALSE the way
    ``not None`` would in a ``bool`` evaluator. ``~(a == 1)`` over a NULL cell
    evaluates its inner comparison to UNKNOWN, and NOT must preserve it.
    """
    a = Column("a", Integer())
    predicate = ~(a == 1)
    cells = {"a": {"number": None}}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_not_of_true_is_false():
    """Kleene NOT axiom: ``NOT TRUE = FALSE``.

    Where ``NOT UNKNOWN`` stays UNKNOWN, negating a *determinate* truth value
    must flip it. ``~(a == 1)`` over ``{"number": 1}`` evaluates its inner
    comparison to TRUE, so NOT must return the FALSE singleton. This is the row
    that forces NOT out of the UNKNOWN short-circuit into a complete rule that
    returns for all three inputs.
    """
    a = Column("a", Integer())
    predicate = ~(a == 1)
    cells = {"a": {"number": 1}}

    result = eval3(predicate, cells, schema=None)

    assert result is FALSE
    assert result is not TRUE
    assert result is not UNKNOWN


def test_not_of_false_is_true():
    """Kleene NOT axiom: ``NOT FALSE = TRUE``.

    The sibling of ``NOT TRUE = FALSE``: negating the other determinate truth
    value flips it the opposite way. ``~(a == 2)`` over ``{"number": 1}``
    evaluates its inner comparison to FALSE, so NOT must return the TRUE
    singleton. Together with ``NOT TRUE = FALSE`` and ``NOT UNKNOWN =
    UNKNOWN``, this pins every row of the Kleene NOT table.
    """
    a = Column("a", Integer())
    predicate = ~(a == 2)
    cells = {"a": {"number": 1}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_unknown_and_false_is_false():
    """Kleene AND axiom: ``UNKNOWN AND FALSE = FALSE``.

    This is the row that proves AND is not a naive fold over sub-results: a
    ``bool`` evaluator that treated the NULL comparison as false-ish would
    still land FALSE here, but one that propagated UNKNOWN through an
    ``all(...)`` would wrongly return UNKNOWN. FALSE *dominates* AND — one
    determinate FALSE operand makes the whole conjunction FALSE regardless of
    the UNKNOWN sibling. ``a == 1`` over ``{"number": None}`` is UNKNOWN and
    ``b == 2`` over ``{"number": 3}`` is FALSE, so the conjunction is FALSE.
    """
    a = Column("a", Integer())
    b = Column("b", Integer())
    predicate = (a == 1) & (b == 2)
    cells = {"a": {"number": None}, "b": {"number": 3}}

    result = eval3(predicate, cells, schema=None)

    assert result is FALSE
    assert result is not TRUE
    assert result is not UNKNOWN


def test_unknown_and_true_is_unknown():
    """Kleene AND axiom: ``UNKNOWN AND TRUE = UNKNOWN``.

    The direct contrast to ``UNKNOWN AND FALSE = FALSE``: same UNKNOWN operand,
    but a TRUE sibling instead of FALSE. TRUE does *not* dominate AND, so the
    UNKNOWN survives — the conjunction is UNKNOWN, not TRUE. This pins that it
    is specifically FALSE (not just "any determinate value") that absorbs an
    UNKNOWN in a conjunction. ``a == 1`` over ``{"number": None}`` is UNKNOWN
    and ``b == 2`` over ``{"number": 2}`` is TRUE, so the conjunction is
    UNKNOWN.
    """
    a = Column("a", Integer())
    b = Column("b", Integer())
    predicate = (a == 1) & (b == 2)
    cells = {"a": {"number": None}, "b": {"number": 2}}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_ne_against_matching_cell_is_false():
    """``!=`` against a present, equal raw cell is FALSE.

    The first leaf comparison beyond ``equals``. ``eval3`` dispatches on the
    backend-agnostic :class:`Operator` enum through the type's
    ``supported_ops`` mapping, where ``Operator.NE`` maps to
    ``"does_not_equal"`` — a token the ``equals`` branch cannot match. So
    ``a != 1`` over ``{"number": 1}`` has a real value to compare and fails
    the predicate, and ``eval3`` must return the FALSE singleton rather than
    falling off the end of the dispatch and yielding implicit ``None``.

    This is the row that forces the evaluator to grow a second leaf operator:
    the Kleene compounds already dispatch over whatever the leaves return, so
    a leaf that returns nothing poisons every predicate built on it.
    """
    a = Column("a", Integer())
    predicate = a != 1
    cells = {"a": {"number": 1}}

    result = eval3(predicate, cells, schema=None)

    assert result is FALSE
    assert result is not TRUE
    assert result is not UNKNOWN


def test_ne_against_mismatching_cell_is_true():
    """``!=`` against a present, unequal raw cell is TRUE.

    The sibling outcome of ``test_ne_against_matching_cell_is_false``: same
    operator, same cell shape, opposite verdict. ``a != 2`` over
    ``{"number": 1}`` has a real value to compare and satisfies the predicate,
    so the result is TRUE.

    Landed green — the ternary that closed the matching case answered this one
    with it. Kept as a truth-table pin: it fixes NE's *polarity*, so a future
    refactor that folded ``does_not_equal`` into the ``equals`` branch without
    negating would be caught here rather than in a differential run.
    """
    a = Column("a", Integer())
    predicate = a != 2
    cells = {"a": {"number": 1}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_ne_against_null_cell_is_unknown():
    """``!=`` against a NULL cell is UNKNOWN — the 3VL guard is per-operator.

    The counterpart of ``test_comparison_against_null_cell_is_unknown`` for the
    second leaf operator. It pins the rule that the NULL guard belongs to every
    *comparison*, not just to ``equals``: without it, ``effective_val != value``
    would evaluate ``1 != None`` as Python truth and return TRUE, silently
    turning a valueless cell into a match — the exact inversion ADR-0019's
    UNKNOWN exists to prevent, and the one a ``bool`` evaluator cannot avoid.

    Landed green. This is the row that makes the guard duplication real: two
    operators now repeat it, which is the trigger to hoist it over the
    comparison operators (but *not* over ``is_empty``/``is_not_empty``, whose
    verdict on a NULL cell is determinate).
    """
    a = Column("a", Integer())
    predicate = a != 1
    cells = {"a": {"number": None}}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_gt_against_greater_cell_is_true():
    """``>`` is TRUE when the *cell* exceeds the literal — order matters.

    ``Operator.GT`` maps to ``"greater_than"``, so like every operator before
    it this needs its own dispatch branch. But GT is the first *asymmetric*
    leaf: ``equals`` and ``does_not_equal`` give the same answer whichever way
    their operands are written, so neither pins which side is which. GT does.

    ``a > 1`` asks whether the value stored in the row exceeds the literal from
    the predicate — ``value > effective_val``, not the reverse. Over
    ``{"number": 2}`` that is ``2 > 1`` = TRUE. An implementation that copied
    the existing branches' ``effective_val <op> value`` spelling would compute
    ``1 > 2`` and answer FALSE, so this test fails for the inversion as well as
    for the missing branch.
    """
    a = Column("a", Integer())
    predicate = a > 1
    cells = {"a": {"number": 2}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_gt_against_equal_cell_is_false():
    """``>`` is strict: a cell *equal* to the literal is FALSE, not TRUE.

    The mismatch half of GT's table, taken at its sharpest point. A cell of 0
    against ``a > 1`` would also be FALSE, but it stays FALSE under a ``>=``
    slip too, so it proves less. The equality boundary is the single row where
    ``>`` and ``>=`` disagree — pinning it fixes the operator's strictness, and
    with it the seam where GE (``greater_than_or_equal_to``) must behave
    differently rather than share a branch.
    """
    a = Column("a", Integer())
    predicate = a > 1
    cells = {"a": {"number": 1}}

    result = eval3(predicate, cells, schema=None)

    assert result is FALSE
    assert result is not TRUE
    assert result is not UNKNOWN


def test_gt_against_null_cell_is_unknown():
    """``>`` against a NULL cell is UNKNOWN — and must not raise.

    GT's row of the 3VL guard, and the first where the guard prevents a *crash*
    rather than a wrong answer: Python 3 refuses to order ``None`` against an
    ``int``, so an unguarded ordering comparison raises ``TypeError`` instead
    of quietly answering. That makes this test the canary for the hoisted
    allowlist — it fails the moment an ordering operator gains a dispatch
    branch without being registered as a comparison, which is exactly the
    two-place-registration slip the current shape allows.
    """
    a = Column("a", Integer())
    predicate = a > 1
    cells = {"a": {"number": None}}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_lt_against_lesser_cell_is_true():
    """``<`` is TRUE when the *cell* falls below the literal.

    ``Operator.LT`` maps to ``"less_than"``, GT's mirror: ``a < 2`` over
    ``{"number": 1}`` is ``1 < 2`` = TRUE, and an inverted branch would compute
    ``2 < 1`` and answer FALSE.

    This is the fourth near-identical leaf, and the one that makes the shape of
    the dispatch the real subject. Three more ordering operators (LE, GE) plus
    the string operators would each add a token to the comparison allowlist and
    an ``elif`` that differs from its neighbours only by a Python operator —
    two edits per operator, where forgetting the first turns a wrong answer
    into a ``TypeError``. Whether this test is satisfied by a fifth ``elif`` or
    by collapsing the branches into a single token-to-callable mapping is a
    production decision; the test pins the behaviour either way.
    """
    a = Column("a", Integer())
    predicate = a < 2
    cells = {"a": {"number": 1}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_comparison_against_absent_cell_is_unknown():
    """A comparison against an *absent* cell is UNKNOWN — ADR-0019's phantom.

    The raw shape here is not ``{"number": None}`` (a property present but
    holding no value) but no cell at all: the outer join's unmatched right
    slice, where ADR-0019 places SQL NULL. Comparing against nothing has no
    more truth value than comparing against a valueless cell, so both land
    UNKNOWN — and WHERE's UNKNOWN-drops policy then *derives* ADR-0005's
    outcome instead of enforcing it structurally.

    The two ``None``\\ s must not be collapsed into one check, though. They
    agree only for comparisons: at raw level one is a dict and the other is
    literally ``None``, which is precisely what makes ``is_null()`` (TRUE here,
    FALSE for a valueless cell) implementable at all. Keeping the checks
    separate is what lets those operators diverge later without revisiting
    this row.

    Today ``prop.get(name)`` returns ``None`` and the next line calls
    ``.get()`` on it, so this raises ``AttributeError`` rather than answering.
    """
    a = Column("a", Integer())
    predicate = a == 1
    cells = {}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_is_empty_on_valueless_cell_is_true():
    """``is_empty`` on a present-but-valueless cell is TRUE — determinate.

    The first operator that must live *outside* ``_COMP_OPERATORS``, and it
    inverts the rule every leaf before it obeyed. For a comparison, a cell
    holding no value is a reason to give up: there is nothing to compare, so
    the answer is UNKNOWN. For ``is_empty`` that same cell *is* the answer —
    ``{"number": None}`` is Notion for "this property holds no value", which is
    exactly what the operator asks about. Routing it through the comparison
    guard would return UNKNOWN and make ``is_empty`` unable to ever say TRUE.

    It also has no operand to compare against: ``a.is_empty()`` coerces
    ``None`` into the bind parameter, so ``value.effective_value`` is ``None``.
    ``is_empty`` is a unary predicate in a ``BinaryExpression`` shape and must
    not be dispatched through a two-argument callable.

    The determinate verdict is not a stylistic choice — it is ADR-0019's
    pushdown-soundness invariant. ``is_empty`` is Notion-semantic and
    **pushable**, so the same predicate may be evaluated Notion-side (in the
    ``Scan`` payload) or client-side here, depending on a planner decision the
    user never sees. Notion's ``is_empty`` filter matches this row; if the
    client-side evaluator answered UNKNOWN, WHERE would drop a row the pushed
    form keeps, and the result would depend on where the predicate landed.
    """
    a = Column("a", Integer())
    predicate = a.is_empty()
    cells = {"a": {"number": None}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_is_empty_on_zero_cell_is_false():
    """``is_empty`` on a cell holding ``0`` is FALSE — present beats falsy.

    Zero is a *value*. The property holds it, so the property is not empty,
    and the row must survive an ``is_empty`` predicate's negation just as it
    would in SQL.

    This is the row that separates "holds no value" from "holds something
    falsy", and it rules out implementing the operator as truthiness over the
    raw value: ``not 0`` is ``True``, so a truthiness test would call this cell
    empty and silently drop or admit rows on the strength of a zero. The same
    trap waits for ``0.0`` and ``False``.
    """
    a = Column("a", Integer())
    predicate = a.is_empty()
    cells = {"a": {"number": 0}}

    result = eval3(predicate, cells, schema=None)

    assert result is FALSE
    assert result is not TRUE
    assert result is not UNKNOWN


def test_is_empty_on_empty_rich_text_is_true():
    """``is_empty`` on ``{"rich_text": []}`` is TRUE — emptiness is per-type.

    The counterweight to the zero case, and the reason the operator cannot be
    a single expression over the raw value. A number is empty when it is
    ``None``; a rich_text is empty when its array has no elements — the value
    here is ``[]``, which is emphatically not ``None``. The reference
    evaluator encodes exactly this split, with a different test per Notion
    type (``date.is_empty`` on ``None``, ``rich_text.is_empty`` on the empty
    sentinel, ``relation.is_empty`` on length) rather than one rule.

    ADR-0019 used to lean on this very cell, claiming it differs under
    ``is_empty`` from ``{"rich_text": [{"text": {"content": ""}}]}`` though
    both decode to ``""``. They do **not** differ — the real API answers TRUE
    to both, as the test below asserts — and that argument is withdrawn (see
    ADR-0019 Correction 2026-07-27). The assertion here stands on its own: the
    empty array is empty by any reading.

    The evaluator still reads raw cells, for a structural reason rather than
    this one. Emptiness is per-type — ``null`` for a number, plain text ``""``
    for a text, no items for a relation — so a rule must be chosen before it
    can be applied, and the raw cell's key *is* the type the choice runs on:
    dispatch is ``"<col_spec>.<op>"``, which a decoded value cannot supply.
    """
    t = Column("t", String())
    predicate = t.is_empty()
    cells = {"t": {"rich_text": []}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_is_empty_on_blank_rich_text_is_true():
    """``is_empty`` on ``[{"text": {"content": ""}}]`` is TRUE — content, not length.

    Verified against real Notion pages: a rich_text property whose single item
    holds the empty string matches the ``is_empty`` filter. Notion's stated
    rule is that a value "equating to empty" — ``0``, ``false``, ``""``, ``[]``
    — is empty, so emptiness is a question about the text the cell spells out,
    not about how many items spell it.

    A length test cannot answer this: the array has one element. Whatever rule
    ``is_empty`` uses has to read through to the content, and it must keep
    answering TRUE for the empty array of its neighbour above, whose plain text
    is also the empty string.

    This is a pushdown-soundness bug, the class ADR-0019 exists to prevent.
    Notion keeps this row for a pushed ``is_empty``; a residual FALSE drops it,
    so the same predicate selects different rows depending on which side of the
    boundary it lands on. It also falsifies the neighbour's docstring, which
    reasons that these two cells *differ* under ``is_empty`` — they do not, and
    the raw-cell requirement needs its argument from elsewhere.
    """
    t = Column("t", String())
    predicate = t.is_empty()
    cells = {"t": {"rich_text": [{"text": {"content": ""}}]}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_is_not_empty_on_blank_rich_text_is_false():
    """``is_not_empty`` on ``[{"text": {"content": ""}}]`` is FALSE — the mirror.

    ``bool(a)`` is True for a one-item array whatever that item holds, so this
    rule reads the array's length exactly as ``is_empty`` did before the
    content test landed. Against the same live Notion pages, ``is_not_empty``
    matched **none** of the six rows the blank and empty-array cells sit in,
    while ``is_empty`` matched all six.

    The two operators are negations of each other, and on a determinate cell
    that has to hold: with ``is_empty`` now answering TRUE here and this rule
    still answering TRUE, ``eval3`` currently calls the same cell both empty
    and not empty. No composition of the two is trustworthy while that stands
    — ``NOT is_empty()`` and ``is_not_empty()`` are the same question, and a
    predicate that asks it twice would contradict itself mid-row.

    Emptiness stays a question about content, so the fix mirrors its
    counterpart. Like ``is_empty`` this is a presence test, bypassing the
    ``_has_no_value`` guard, so it must also stay determinate — FALSE, not
    UNKNOWN — on a cell that carries no value at all.
    """
    t = Column("t", String())
    predicate = t.is_not_empty()
    cells = {"t": {"rich_text": [{"text": {"content": ""}}]}}

    result = eval3(predicate, cells, schema=None)

    assert result is FALSE
    assert result is not TRUE
    assert result is not UNKNOWN


def test_equals_on_matching_rich_text_is_true():
    """``equals`` is per-type too: a rich_text cell matching its literal.

    ``equals`` looked type-generic while only ``Integer`` exercised it, but the
    two sides of the comparison have different shapes. The raw cell holds
    Notion's array — ``[{"text": {"content": "x"}}]`` — while the predicate's
    literal stays a plain Python ``str``: ``effective_value`` is the value as
    written, since ``bind_processor`` is not applied when building the bind
    parameter. Comparing them directly is comparing a list to a string, which
    is FALSE for every input and TRUE for none.

    So the operator needs the type's own reading of the cell (the array's
    plain text) before any comparison, exactly as ``is_empty`` needs the
    type's own notion of emptiness. This is the same lesson one operator
    earlier, and it says the type dimension belongs in the dispatch rather
    than inside individual branches.
    """
    t = Column("t", String())
    predicate = t == "x"
    cells = {"t": {"rich_text": [{"text": {"content": "x"}}]}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_is_empty_on_absent_cell_is_unknown():
    """``is_empty`` on an absent cell is UNKNOWN — not empty, *not there*.

    An outer join's unmatched right slice carries a literal ``None`` where a
    raw cell would be. That is not a property holding no value, which is what
    ``is_empty`` asks about; it is the absence of the property altogether, and
    ADR-0019 gives that its own operator (``is_null()``, #366).

    UNKNOWN rather than FALSE, and the deciding case is negation, not WHERE.
    Both verdicts drop the phantom under a bare ``WHERE``, so they look
    interchangeable — but under ``~col.is_empty()`` a FALSE flips to TRUE and
    *resurrects* the phantom, while UNKNOWN stays UNKNOWN and it stays
    dropped. ADR-0005's rule, which ADR-0019 preserves, is that a phantom
    fails every right-side predicate; only UNKNOWN keeps that true once the
    predicate is compounded.

    The guard must therefore sit above the operator dispatch, where it was
    before the per-type table, rather than inside the comparison allowlist —
    ``is_empty`` currently reaches ``prop_val.get(...)`` and raises
    ``AttributeError``.
    """
    a = Column("a", Integer())
    predicate = a.is_empty()
    cells = {"a": None}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_every_declared_operator_has_an_eval3_rule():
    """Every operator a type advertises must have a rule in ``_OPERATORS``.

    ``supported_ops`` is normlite's public promise about what a column of a
    given type can be filtered on, and ``Comparator.operate`` enforces it at
    construction: a predicate that reaches ``eval3`` has already been declared
    supported. If the dispatch table lacks the matching rule the evaluator
    fails with a bare ``KeyError`` from inside a WHERE evaluation — a promise
    made in one module and broken in another, which no per-operator test will
    catch because the missing ones are exactly those nobody wrote a test for.

    Walking ``type_mapper`` rather than a hand-written list is what makes this
    a guard rather than a snapshot: a newly registered type arrives here
    automatically instead of being quietly unsupported.

    Note this proves *registration*, not correctness — a rule can exist and
    still be wrong, as ``equals`` was for rich_text. Behavioural coverage comes
    from the per-operator tests and the differential suite.
    """
    from normlite.sql.eval3 import _OPERATORS
    from normlite.sql.type_api import type_mapper

    declared = {}
    for type_ in type_mapper.values():
        supported_ops = getattr(type_, "supported_ops", None)
        if not supported_ops:
            continue  # not filterable: ObjectId, PropertyId, TimeStampStringISO8601
        try:
            col_spec = type_.get_col_spec()
        except NotImplementedError:
            continue  # no Notion property shape of its own: ArchivalFlag
        for token in supported_ops.values():
            declared.setdefault(f"{col_spec}.{token}", set()).add(type(type_).__name__)

    missing = {key: sorted(types) for key, types in declared.items() if key not in _OPERATORS}

    assert not missing, (
        f"{len(missing)} of {len(declared)} declared operators have no eval3 rule: "
        + ", ".join(f"{key} ({'/'.join(types)})" for key, types in sorted(missing.items()))
    )


_NONE_CELL_LITERALS = {
    "number": 1,
    "title": "x",
    "rich_text": "x",
    "checkbox": True,
    "date": date(2024, 6, 1),
    "relation": "1429989f-e8ac-4eff-bc8f-57f56486db54",
}
"""One type-appropriate literal per ``col_spec``, so each predicate below is
one a user could really have written. Which literal it is cannot matter: the
cell under test holds no value to compare it against."""


def _declared_operator_pairs() -> list[tuple[str, TypeEngine, Operator]]:
    """Every declared ``<col_spec>.<operator>`` pair, with a type that declares it.

    Derived from ``type_mapper`` by the same walk as
    ``test_every_declared_operator_has_an_eval3_rule`` — a newly registered type
    or operator arrives here on its own rather than waiting to be added to a
    hand-written list. Deduplicated on the pair, which is the unit ``eval3``
    dispatches on: the four numeric types declare one ``number`` table between
    them, and re-running an identical dispatch under four names measures
    nothing.
    """
    from normlite.sql.type_api import type_mapper

    pairs: dict[str, tuple[TypeEngine, Operator]] = {}
    for type_ in type_mapper.values():
        supported_ops = getattr(type_, "supported_ops", None)
        if not supported_ops:
            continue  # not filterable: ObjectId, PropertyId, TimeStampStringISO8601
        try:
            col_spec = type_.get_col_spec()
        except NotImplementedError:
            continue  # no Notion property shape of its own: ArchivalFlag
        for op, token in supported_ops.items():
            pairs.setdefault(f"{col_spec}.{token}", (type_, op))
    return [(key, type_, op) for key, (type_, op) in sorted(pairs.items())]


_DECLARED_OPERATOR_PAIRS = _declared_operator_pairs()


@pytest.mark.parametrize(
    "pair, type_, op",
    _DECLARED_OPERATOR_PAIRS,
    ids=[pair for pair, _, _ in _DECLARED_OPERATOR_PAIRS],
)
def test_every_declared_operator_on_a_none_cell_is_unknown(pair, type_, op):
    """A ``None`` cell is UNKNOWN for *every* declared operator — no exceptions.

    ``test_is_empty_on_absent_cell_is_unknown`` pins one operator on this cell;
    this pins the whole declared surface, because the property about to become
    load-bearing is universal quantification, not one case.

    **Why it is worth pinning now.** An outer join fills right-owned columns
    with literal Python ``None`` (``HashJoin._project_join_row``), and today a
    phantom row is dropped by a structural guard *above* the predicate —
    ``Filter._right_side_passes`` returns ``False`` when the whole right slice
    is ``None``. That guard is deleted in ADR-0022 step 1, after which nothing
    is left between a phantom and the answer except this fact: every leaf over
    a ``None`` cell is UNKNOWN, the WHERE policy drops UNKNOWN, so the row goes.
    ADR-0005's outcome stops being hard-coded and starts being *derived*.

    So the fact is incidental before the deletion and load-bearing after it,
    and it should be asserted before the code begins to depend on it. The
    deletion itself is measured to change no test result (872 → 872), which is
    exactly why there is no failing test to write for it and why this safety net
    is written instead of a manufactured red.

    **UNKNOWN, not FALSE**, for the reason
    ``test_is_empty_on_absent_cell_is_unknown`` gives at length: under
    ``~col.is_empty()`` a FALSE flips to TRUE and resurrects the phantom, while
    UNKNOWN stays UNKNOWN. Both verdicts look alike under a bare WHERE; only one
    survives negation. Hence the negative assertions — ``is UNKNOWN`` alone
    would also hold if the singletons were ever collapsed.

    The mechanism under test is the guard at ``eval3.py:146``, whose escape
    hatch is ``_ABSENT_AWARE`` — empty until ``is_null()`` arrives with #366.
    Adding any operator to that set must fail this test for that pair, which is
    how this assertion was proved able to fail before it was trusted.
    """
    col_spec = pair.split(".", 1)[0]
    literal = (
        None
        if op in (Operator.IS_EMPTY, Operator.IS_NOT_EMPTY)
        else _NONE_CELL_LITERALS[col_spec]
    )
    a = Column("a", type_)
    predicate = a.operate(op, literal)
    cells = {"a": None}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_date_equals_matching_cell_is_true():
    """``equals`` on a date compares instants, not representations.

    The raw cell carries Notion's ISO strings (``{"start": "2024-06-01"}``)
    while the predicate's literal stays a Python ``date`` — the two sides of a
    date comparison never arrive in the same form. Comparing them directly is
    a dict against a ``date`` object, which is FALSE for every input, so the
    operator needs both sides normalised before it can answer.
    """
    d = Column("d", Date())
    predicate = d == date(2024, 6, 1)
    cells = {"d": {"date": {"start": "2024-06-01", "end": None}}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_date_after_earlier_cell_is_true():
    """``after`` orders the cell's start against the literal.

    The ordering counterpart, and the one where the shape mismatch stops being
    a wrong answer and becomes a crash: reading ``["start"]`` off the literal
    raises, because the literal is a ``date`` object rather than the
    ``{"start": ...}`` mapping the rule expects.

    Normalising only the cell is not enough either — that yields ``datetime``
    while the literal stays ``date``, and Python refuses to order the two. Both
    sides have to reach the same domain, and reaching it the way the pushed
    filter does is what keeps a residual date predicate agreeing with a pushed
    one.
    """
    d = Column("d", Date())
    predicate = d.after(date(2024, 1, 1))
    cells = {"d": {"date": {"start": "2024-06-01", "end": None}}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_date_is_empty_on_valueless_cell_is_true():
    """An unset date cell holds no value — ``is_empty`` is TRUE.

    The date analogue of ``test_is_empty_on_valueless_cell_is_true``, and not a
    duplicate of it: ``date.is_empty`` answers through
    ``normalize_page_date``, a different path from the number arm, and only a
    per-type test reaches it.

    This test used to construct ``{"date": {}}`` and its docstring asserted
    that "Notion echoes an unset date as an empty mapping rather than
    ``None``". **That was false**, and it was the source of the fiction this
    branch unwound: ``{"<col_spec>": {}}`` is a property *definition* — a
    column declaration — never a cell. Measured 2026-07-29: the API rejects it
    on ``POST /v1/pages`` with a 400, and clearing a date through the UI stores
    and emits ``null``. ``generators.py`` modelled an unset date on that schema
    object, then ``_has_no_value`` grew an ``== {}`` arm to agree with the
    generator, and this test pinned the result. See ADR-0019 Correction
    (6)-(10).

    The *behaviour* was right all along and is kept verbatim; only the cell it
    is measured on changed. A present-but-valueless cell is determinate for
    ``is_empty`` — TRUE — and must not be confused with an *absent* cell, which
    makes the same predicate UNKNOWN (``test_is_empty_on_absent_cell_is_unknown``).
    """
    d = Column("d", Date())
    predicate = d.is_empty()
    cells = {"d": {"date": None}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


def test_date_does_not_equal_on_unset_date_is_unknown():
    """``!=`` on an *unset* date is UNKNOWN — the NULL guard must be per-type.

    Found by the differential, not by hand: driving ``ReferenceGenerator``'s
    leaf conditions through a JSON-to-AST bridge and comparing ``eval3`` against
    ``reference_eval`` turned up exactly one divergent leaf in ~80 000
    evaluations, and this is it. ``d != <literal>`` over an unset date gave
    TRUE, while both the reference evaluator and the fake client's ``_Filter``
    gave False.

    It is a **pushdown-soundness** failure, ADR-0019's named invariant, not a
    disagreement with a test oracle. ``does_not_equal`` is pushable, so the same
    predicate may be answered Notion-side or here depending on a planner
    decision the user cannot see. ``_Condition.eval`` (``client.py``) hard-returns
    ``False`` for *every* binary date operator when the page date has no instant
    — overriding its own ``a != b`` entry — so pushed, this row is dropped;
    residually it is kept. The result depends on where the predicate landed.

    **The original diagnosis was wrong about the cause, and correcting it is
    why this test now reads ``null``.** It blamed each Notion type spelling
    emptiness differently — "number ``None``, rich_text ``[]``, date ``{}``" —
    and the fix grew an ``== {}`` arm on ``_has_no_value`` to match. But
    ``{"date": {}}`` is a property *definition*, not a cell, and is
    unproducible in value position (measured 2026-07-29; ADR-0019 Correction
    (6)-(10)). The shape the differential actually needed was ``null`` all
    along; it reached ``{}`` only because ``generators.py`` modelled an unset
    date on a column declaration.

    What the test still guards is real and unchanged: the valueless case must
    short-circuit **before** the operator table. If it does not, ``d !=
    <literal>`` reaches ``_date_cmp``'s ``on_incomparable=True`` — the
    "negative operators stay proper negations" default — and answers TRUE for a
    cell with nothing to compare. That default is safe *only* behind the guard,
    which is precisely the ordering this pins.

    UNKNOWN rather than FALSE, for the same reason as
    ``test_is_empty_on_absent_cell_is_unknown``: under a bare WHERE the two are
    indistinguishable — both drop the row, restoring parity with the pushed
    False whichever evaluator is the more faithful to Notion — but under
    negation FALSE flips to TRUE and resurrects a row that has no value to
    compare, while UNKNOWN stays UNKNOWN. ``{"date": null}`` is already pinned
    as present-but-valueless by ``test_date_is_empty_on_valueless_cell_is_true``;
    this test says a *comparison* against that same cell has no truth value.

    This is #384's core claim on the residual side, so it must not be deleted:
    the live API **matches** a valueless cell on ``does_not_equal`` where SQL
    drops it. (For *date* Notion rejects that operator outright with a 400,
    #383 — so the divergence is only reachable through
    ``number.does_not_equal``, which is #381.)

    Note the sibling operators (``equals``, ``after``, ``before``) reach FALSE
    through ``on_incomparable=False`` and so agree with the pushed side by
    coincidence, in both polarities. Only the negative operator's opposite
    default makes the gap visible — which is why one leaf, and not four,
    diverged.
    """
    d = Column("d", Date())
    predicate = d != date(2024, 6, 1)
    cells = {"d": {"date": None}}

    result = eval3(predicate, cells, schema=None)

    assert result is UNKNOWN
    assert result is not TRUE
    assert result is not FALSE


def test_unknown_or_true_is_true():
    """Kleene OR axiom: ``UNKNOWN OR TRUE = TRUE``.

    The dual of ``UNKNOWN AND FALSE = FALSE``: where FALSE dominates a
    conjunction, TRUE *dominates* a disjunction. One determinate TRUE operand
    makes the whole disjunction TRUE regardless of the UNKNOWN sibling — the
    unknown truth of the other operand cannot change an already-satisfied OR.
    ``a == 1`` over ``{"number": None}`` is UNKNOWN and ``b == 2`` over
    ``{"number": 2}`` is TRUE, so the disjunction is TRUE.
    """
    a = Column("a", Integer())
    b = Column("b", Integer())
    predicate = (a == 1) | (b == 2)
    cells = {"a": {"number": None}, "b": {"number": 2}}

    result = eval3(predicate, cells, schema=None)

    assert result is TRUE
    assert result is not FALSE
    assert result is not UNKNOWN


@pytest.mark.parametrize('type_obj', [
    Number('number'),
    Integer(),
    Float(),
    Numeric(),
    Money('euro'),
    String(),
    String(is_title=True),
    Boolean(),
    Date(),
    Relation(),
], ids=lambda t: type(t).__name__ + '/' + t.get_col_spec())
def test_eval3_calls_a_cell_valueless_only_if_that_cell_decodes_to_none(
    type_obj: TypeEngine,
):
    """``eval3`` may only call a cell valueless if the decoder agrees.

    This is the **other half** of the decode invariant (CONTEXT.md, "Raw cell
    <-> decoded NULL"): a raw cell decodes to Python ``None`` **iff** the
    raw-cell evaluator calls it valueless.
    ``test_every_type_decodes_a_valueless_cell_as_none`` pins the "if"
    direction at ``{"<col_spec>": null}``; this pins the "only if", and the two
    together are what make it an *iff* rather than two independent rules.

    It is stated as an agreement between the two functions rather than as a
    claim about what ``{"<col_spec>": {}}`` *means*, and that distinction is
    the point. ``{}`` in value position is **unproducible** -- the API rejects
    it on ``POST /v1/pages`` with a 400, and clearing a cell in the UI stores
    ``null`` -- so asserting a verdict for it would pin semantics on a cell
    that cannot exist. That is the very mistake this branch is unwinding:
    ``generators.py`` modelled an unset date on a *property definition*, and
    ``_has_no_value`` then grew an ``== {}`` arm to agree with the generator.
    Internal consistency is well defined whether or not the shape is
    reachable, so that is what this asserts.

    Why the disagreement is a defect and not dead weight: the two halves must
    not be able to answer differently, because ``None`` in a decoded ``Row``
    *is* how a user sees SQL NULL and there is no second channel. Today
    ``_has_no_value({})`` is ``True`` -- so a comparison would return UNKNOWN,
    dropping the row as NULL -- while no decoder returns ``None`` for that
    cell; every one of them raises. One half says SQL NULL, the other says
    "malformed, refuse to decode". Whichever is right, they cannot both be.

    Dropping ``== {}`` leaves ``val is None`` and is **semantics-preserving
    over the reachable domain**: ``eval3`` already answers UNKNOWN on ``>``,
    ``<`` and ``==`` against ``{"number": null}`` *without* that arm, so
    ADR-0019's Decision is not reopened. The loud failure that remains is the
    wanted one (settled: prefer the loud form) -- a type-drifted cell must
    raise, not decode as a silent SQL NULL.
    """
    result = type_obj.result_processor()
    empty_config = {}
    cell = {type_obj.get_col_spec(): empty_config}

    try:
        decoded = result(cell)
    except Exception:
        # not decoding at all is emphatically not decoding to None
        decoded = _DID_NOT_DECODE

    assert decoded is not None, (
        "premise of this test: no type decodes an empty-config cell to None"
    )
    assert not _has_no_value(empty_config), (
        "eval3 calls this cell valueless (-> UNKNOWN, a SQL NULL) but the "
        "decoder does not produce None for it, so the decode invariant is "
        "violated in the 'only if' direction"
    )
