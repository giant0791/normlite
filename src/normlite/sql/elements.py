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
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Protocol

from normlite.sql.base import ClauseElement

if TYPE_CHECKING:
    from normlite.sql.type_api import TypeEngine

class ColumnElement(ClauseElement):
    """Base class for SQLAlchemy-style expressions."""

    __visit_name__ = 'column_element'

    name: str

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
        

class UnaryExpression(ColumnElement):
    __visit_name__ = 'unary_expression'

    def __init__(self, operator: str, element: ColumnElement):
        if operator != "not":
            raise ValueError(f"Unsupported unary operator: {operator}")
        self.operator = operator
        self.element = element

class BinaryExpression(ColumnElement):
    __visit_name__ = 'binary_expression'

    def __init__(
            self, 
            column: ColumnElement, 
            operator: str, 
            value: Any
    ):
        self.column = column              # Column
        self.operator = operator          # Notion operator string
        self.value = value                # BindParam

class BooleanClauseList(ColumnElement):
    __visit_name__ = 'boolean_clause_list'
    def __init__(self, operator, clauses: list[ColumnElement]) -> None:
        self.operator = operator
        self.clauses: list[ColumnElement] = []

        for clause in clauses:
            if (
                isinstance(clause, BooleanClauseList)
                and clause.operator == operator
            ):
                self.clauses.extend(clause.clauses)
            else:
                self.clauses.append(clause)

class ColumnOperators:
    if TYPE_CHECKING:
        comparator: Comparator  # for type checkers only, this attribute exists at runtime, but is not structurally owned by this class.

    def operate(self, op: str, other: Any):
        return self.comparator.operate(op, other)

    def __eq__(self, other):
        return self.operate("eq", other)

    def __ne__(self, other):
        return self.operate("ne", other)

    def __gt__(self, other):
        return self.operate("gt", other)

    def __lt__(self, other):
        return self.operate("lt", other)

    def in_(self, other):
        return self.operate("in", other)

    def not_in(self, other):
        return self.operate("ni", other)

    def startswith(self, other):
        return self.operate("sw", other)

    def endswith(self, other):
        return self.operate("ew", other)

    def is_empty(self):
        return self.operate("ie", None)

    def is_not_empty(self):
        return self.operate("ine", None)

    def before(self, other):
        return self.operate("be", other)

    def after(self, other):
        return self.operate("af", other)

class Comparator:
    def __init__(self, expr: ColumnElement):
        self.expr = expr
        self.type_ = expr.type_

    def operate(self, op: str, other: Any):
        raise NotImplementedError

class ObjectIdComparator(Comparator):
    pass

class BooleanComparator(Comparator):
    OPS = {
        "eq": "equals",
        "ne": "does_not_equal",
    }

    def operate(self, op, other):
        try:
            notion_op = self.OPS[op]
        except KeyError:
            raise TypeError("Boolean only supports equality operators")

        return BinaryExpression(
            self.expr,
            notion_op,
            coerce_to_bindparam(other, self.type_),
        )

class StringComparator(Comparator):
    OPS = {
        "eq": "equals",
        "ne": "does_not_equal",
        "in": "contains", 
        "ni": "does_not_contain",
        "ew": "ends_with",
        "sw": "starts_with",
        "ie": "is_empty",
        "ine": "is_not_empty"
    }
    def operate(self, op, other) -> Optional[BinaryExpression]:
        try:
            notion_op = self.OPS[op]
            return BinaryExpression(
                column=self.expr,
                operator=notion_op,
                value=coerce_to_bindparam(other, self.type_),
            )
        except KeyError:
            raise TypeError(f'Invalid string operator: {op}')
    
class NumberComparator(Comparator):
    OPS = {
        "eq": "equals",
        "ne": "does_not_equal",
        "lt": "less_than",
        "gt": "greater_than",
        "le": "less_than_or_equal_to",
        "ge": "greater_than_or_equal_to",
        "ie": "is_empty",
        "ine": "is_not_empty"
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
    
class DateComparator(Comparator):
    OPS = {
        "af": "after",
        "be": "before",
        "eq": "equals",
        "is": "is_empty",
        "ine": "is_not_empty"        
    }

    def operate(self, op, other):
        try:
            notion_op = self.OPS[op]
        except KeyError:
            raise TypeError(f"Invalid date operator: {op}")
        
        return BinaryExpression(
            self.expr,
            notion_op,
            coerce_to_bindparam(other, self.type_)
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
        return self.callable_() if self.callable_ else self.value

def coerce_to_bindparam(value: Any, type_) -> BindParameter:
    if isinstance(value, BindParameter):
        if value.type is None:
            value.type = type_
        return value

    if callable(value):
        return BindParameter(None, callable_=value, type_=type_)

    return BindParameter(None, value=value, type_=type_)
