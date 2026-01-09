# tests/axioms/test_nc_axioms.py
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
"""Axioms for validating the :class:`normlite.sql.compiler.NotionCompiler`.

These tests are simple regression tests for the Notion compiler.
"""
from normlite.sql.elements import BooleanClauseList, UnaryExpression
from normlite.sql.schema import Column
from normlite.sql.type_api import Integer
from tests.support.ast_equal import ast_equal

def test_and_flattening():
    """Boolean normalization axioms"""
    a, b, c = Column("a", Integer()), Column("b", Integer()), Column("c", Integer())
    expr = (a == 1) & ((b == 2) & (c == 3))

    assert isinstance(expr, BooleanClauseList)
    assert expr.operator == "and"
    assert len(expr.clauses) == 3

def test_not_precedence():
    """Operator precedence"""
    a, b = Column("a", Integer()), Column("b", Integer())
    expr = ~(a == 1) & (b == 2)

    assert isinstance(expr, BooleanClauseList)
    assert isinstance(expr.clauses[0], UnaryExpression)

def test_and_associativity_ast():
    """Associativity test for ASTs"""
    a, b, c = Column("a", Integer()), Column("b", Integer()), Column("c", Integer())

    e1 = (a == 1) & ((b == 2) & (c == 3))
    e2 = ((a == 1) & (b == 2)) & (c == 3)

    assert ast_equal(e1, e2)

