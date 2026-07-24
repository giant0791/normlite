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
