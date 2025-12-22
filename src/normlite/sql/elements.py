# sql/elements.py
# Copyright (C) 2025 Gianmarco Antonini
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

from __future__ import annotations
from enum import Enum
import pdb
from typing import TYPE_CHECKING, Any, Callable, Optional

from normlite.sql.base import ClauseElement

if TYPE_CHECKING:
    from normlite.sql.type_api import TypeEngine

class ColumnElement(ClauseElement):
    """Base class for SQLAlchemy-style expressions."""

    __visit_name__ = 'column_element'

    type_: TypeEngine
    """The type of this :class:`ColumnElement` object."""

    primary_key: bool = False
    """``True`` if this :class:`ColumnElement` object represents a primary key."""

    def __invert__(self):
        return UnaryExpression("not", self)    
    
    def __and__(self, other):
        if not isinstance(other, ColumnElement):
            return NotImplemented
        return BooleanClauseList("and", [self, other])

    def __or__(self, other):
        if not isinstance(other, ColumnElement):
            return NotImplemented
        return BooleanClauseList("or", [self, other])
        
class ColumnExpression(ClauseElement):
    """
    Represents a Notion filter expression node.
    Can be either:
      - logical: AND / OR
      - property filter
    """
    pass

class UnaryExpression(ColumnElement):
    def __init__(self, operator: str, element: ColumnElement):
        assert operator == "not"
        self.operator = operator
        self.element = element

class BinaryExpression(ColumnExpression):
    def __init__(self, column, operator: str, value):
        self.column = column              # Column
        self.operator = operator          # Notion operator string
        self.value = value                # BindParam

class BooleanClauseList(ColumnElement):
    def __init__(self, operator, clauses):
        self.operator = operator
        self.clauses = []

        for clause in clauses:
            if (
                isinstance(clause, BooleanClauseList)
                and clause.operator == operator
            ):
                self.clauses.extend(clause.clauses)
            else:
                self.clauses.append(clause)

class ColumnOperators:
    def __eq__(self, other):
        return self.operate("eq", other)

    def __ne__(self, other):
        return self.operate("ne", other)

    def __lt__(self, other):
        return self.operate("lt", other)

    def operate(self, op, other):
        raise NotImplementedError

class Comparator(ColumnOperators):
    def __init__(self, expr: ColumnElement):
        self.expr = expr
        self.type_ = expr.type_

class ObjectIdComparator(Comparator):
    pass

class BooleanComparator(Comparator):
    pass

class CheckboxComparator(Comparator):
    def operate(self, op, other):
        if op not in ("eq", "ne"):
            raise TypeError("Checkbox only supports equality")

        notion_op = "equals" if op == "eq" else "does_not_equal"

        return BinaryExpression(
            column=self.expr,
            operator=notion_op,
            value=coerce_to_bindparam(other, self.type_),
        )

class StringComparator(Comparator):
    def operate(self, op, other):
        if op not in ("eq", "ne", "contains"):
            raise TypeError('String only supports equality and contains')
        
        notion_op = "equals" if op == "eq" else "does_not_equal"

        return BinaryExpression(
            column=self.expr,
            operator=notion_op,
            value=coerce_to_bindparam(other, self.type_),
        )

    def __eq__(self, other) -> BinaryExpression:
        return self._binary("equals", other)
    
    def __ne__(self, other):
        return self._binary("does_not_equal", other)

    def contains(self, other) -> BinaryExpression:
        return self._binary("contains", other)

    def _binary(self, notion_op, other) -> BinaryExpression:
        return BinaryExpression(
            column=self.expr,
            operator=notion_op,
            value=coerce_to_bindparam(other, self.type_),
        )

class NumberComparator(Comparator):
    OPS = {
        "eq": "equals",
        "ne": "does_not_equal",
        "lt": "less_than",
        "gt": "greater_than",
        "le": "less_than_or_equal_to",
        "ge": "greater_than_or_equal_to",
    }

    def operate(self, op, other) -> BinaryExpression:
        try:
            notion_op = self.OPS[op]
        except KeyError:
            raise TypeError(f"Invalid number operator: {op}")

        return BinaryExpression(
            self.expr,
            notion_op,
            coerce_to_bindparam(other, self.type_),
        )


class _NoArg(Enum):
    NO_ARG = 0

    def __repr__(self):
        return f"_NoArg.{self.name}"

class BindParameter(ClauseElement):
    def __init__(
        self, 
        key: str, 
        value: Any =_NoArg.NO_ARG, 
        callable_: Optional[Callable[[], Any]] = None, 
        type_: Optional[TypeEngine]=None
    ):
        self.key = key
        self.value = value
        self.callable_ = callable_
        self.type = type_

    @property
    def effective_value(self):
        raw = self.callable_() if self.callable_ else self.value
        bind_processor = self.type.bind_processor()
        return bind_processor(raw)


def coerce_to_bindparam(value: Any, type_):
    if isinstance(value, BindParameter):
        if value.type is None:
            value.type = type_
        return value
    return BindParameter(None, value=value, type_=type_)
