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
from normlite.sql.eval3 import eval3, TRUE, FALSE, UNKNOWN
from normlite.sql.schema import Column
from normlite.sql.type_api import Integer


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
