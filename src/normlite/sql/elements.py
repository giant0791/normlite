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
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol

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
        
class ColumnExpression(ClauseElement):
    """
    Represents a Notion filter expression node.
    Can be either:
      - logical: AND / OR
      - property filter
    """
    __visit_name__ = 'column_expression'

    pass

class UnaryExpression(ColumnElement):
    def __init__(self, operator: str, element: ColumnElement):
        assert operator == "not"
        self.operator = operator
        self.element = element

class BinaryExpression(ColumnExpression):
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

class HasComparator(Protocol):
    comparator: ComparatorProtocol

class ComparatorProtocol(Protocol):
    def __eq__(self, other: Any): 
        ...
    
    def __ne__(self, other: Any): 
        ...
    
    def in_(self, other: Any): 
        ...

    def not_in(self, other):
        ...

    def endswith(self, other):
        ...

    def __lt__(self, other):
        ...

    def before(self, other):
        ...

class ColumnOperators:
    if TYPE_CHECKING:
        comparator: ComparatorProtocol  # for type checkers only

    def __eq__(self, other):
        return self.operate("eq", other)

    def __ne__(self, other):
        return self.operate("ne", other)

    def __lt__(self, other):
        return self.operate("lt", other)
    
    def in_(self, other):
        return self.comparator.in_(other)
    
    def not_in(self, other):
        return self.comparator.not_in(other)
    
    def endswith(self, other):
        return self.comparator.endswith(other)
    
    def before(self, other):
        return self.comparator.before(other)

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
    OPS = {
        "eq": "equals",
        "ne": "does_not_equal",
        "in": "contains", 
        "ni": "does_not_contain",
        "ew": "ends_with"
    }
    def operate(self, op, other) -> Optional[BinaryExpression]:
        try:
            notion_op = self.OPS[op]
            return BinaryExpression(
                column=self.expr,
                operator=notion_op,
                value=coerce_to_bindparam(other, self.type_),
            )
        except KeyError as ke:
            raise TypeError(f'Unsupported or unknown string operator: {str(ke)}') from ke

    def __eq__(self, other) -> BinaryExpression:
        return self.operate("eq", other)
    
    def __ne__(self, other):
        return self.operate("ne", other)

    def in_(self, other) -> BinaryExpression:
        return self.operate("in", other)
    
    def not_in(self, other) -> BinaryExpression:
        return self.operate("ni", other)
    
    def endswith(self, other) -> BinaryExpression:
        return self.operate("ew", other)
    
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
    
class DateComparator(Comparator):
    def before(self, other):
        return self.operate('before', other)

    def operate(self, op, other):
        if op != 'before':
            raise TypeError(f'Invalid date operator: {op}')
        
        return BinaryExpression(
            self.expr,
            op,
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

def coerce_to_bindparam(value: Any, type_):
    if isinstance(value, BindParameter):
        if value.type is None:
            value.type = type_
        return value

    if callable(value):
        return BindParameter(
            None,
            value=None,
            callable_=value,
            type_=type_,
        )

    return BindParameter(
        None,
        value=value,
        type_=type_,
    )
