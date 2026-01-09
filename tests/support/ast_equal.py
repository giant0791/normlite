# tests/support/generators.py
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

from normlite.sql.elements import BinaryExpression, BindParameter, BooleanClauseList, ColumnElement, UnaryExpression

def ast_equal(a: ColumnElement, b: ColumnElement) -> bool:
    """Return ``True`` if the two ASTs are equal.

    This is for round-trip tests needed.

    .. note::
        Two ASTs are equal if and only if they are structurally equivalent representations of the same query, 
        independent of object identity and construction path.

    Args:
        a (ColumnElement): Left operand
        b (ColumnElement): Right operand

    Raises:
        NotImplementedError: If no equality is defined for ``type(a)``.

    Returns:
        bool: ``True`` if the two ASTs are equal.
    """
    if a is b:
        return True

    if a is None or b is None:
        return a is b

    if type(a) is not type(b):
        return False

    method = f"_eq_{type(a).__name__}"
    try:
        return globals()[method](a, b)
    except KeyError:
        raise NotImplementedError(f"No equality defined for {type(a).__name__}")

def _eq_Column(a: ColumnElement, b: ColumnElement) -> bool:
    return (
        a.name == b.name
        and a.table.name == b.table.name
    )

def _eq_BindParameter(a: BindParameter, b: BindParameter) -> bool:
    if a.callable_ is not None or b.callable_ is not None:
        return a.callable_ is b.callable_

    return (
        a.value == b.value
        and a.type.__class__ is b.type.__class__
        and a.type.__dict__ == b.type.__dict__
    )

def _eq_BinaryExpression(a: BinaryExpression, b: BinaryExpression) -> bool:
    return (
        a.operator == b.operator
        and ast_equal(a.column, b.column)
        and ast_equal(a.value, b.value)
    )

def _eq_UnaryExpression(a: UnaryExpression, b: UnaryExpression) -> bool:
    return (
        a.operator == b.operator
        and ast_equal(a.element, b.element)
    )

def _eq_BooleanClauseList(a: BooleanClauseList, b: BooleanClauseList) -> bool:
    if a.operator != b.operator:
        return False

    if len(a.clauses) != len(b.clauses):
        return False

    return all(
        ast_equal(x, y)
        for x, y in zip(a.clauses, b.clauses)
    )

