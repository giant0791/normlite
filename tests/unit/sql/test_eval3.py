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
