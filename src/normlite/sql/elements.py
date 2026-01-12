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
from enum import Enum, auto
import pdb
from typing import TYPE_CHECKING, Any, Callable, Optional

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
        
    def __bool__(self):
        """Forbid Python truthiness.

        Raises:
            TypeError: Always, boolean value of a SQL/Notion expression is not defined.
        """

        raise TypeError(
            "Boolean value of a SQL/Notion expression is not defined. "
            "Use explicit comparison."
        )

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
            operator: Operator, 
            value: Any
    ):
        self.column = column              # Column
        self.operator = operator          # Notion operator string
        self.value = value                # BindParam

class BooleanClauseList(ColumnElement):
    __visit_name__ = 'boolean_clause_list'
    def __init__(self, operator, clauses: list[ColumnElement]) -> None:
        if operator not in ['and', 'or', 'not']:
            raise TypeError(f"Invalid boolean clause list operator: {operator}")

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

class Operator(Enum):
    """Canonical backend-agnostic operators vocabulary."""

    # comparison
    EQ = auto()
    NE = auto()
    GT = auto()
    LT = auto()
    GE = auto()
    LE = auto()

    # set / string
    IN = auto()
    NOT_IN = auto()
    STARTSWITH = auto()
    ENDSWITH = auto()

    # date-specific
    BEFORE = auto()
    AFTER = auto()

    # unary / nullary
    IS_EMPTY = auto()
    IS_NOT_EMPTY = auto()

class ColumnOperators:
    if TYPE_CHECKING:
        comparator: Comparator  # for type checkers only, this attribute exists at runtime, but is not structurally owned by this class.

    def operate(self, op: Operator, other: Any):
        return self.comparator.operate(op, other)

    def __eq__(self, other):
        return self.operate(Operator.EQ, other)

    def __ne__(self, other):
        return self.operate(Operator.NE, other)

    def __gt__(self, other):
        return self.operate(Operator.GT, other)

    def __lt__(self, other):
        return self.operate(Operator.LT, other)
    
    def __ge__(self, other):
        return self.operate(Operator.GE, other)
    
    def __le__(self, other):
        return self.operate(Operator.LE, other)

    def in_(self, other):
        return self.operate(Operator.IN, other)

    def not_in(self, other):
        return self.operate(Operator.NOT_IN, other)

    def startswith(self, other):
        return self.operate(Operator.STARTSWITH, other)

    def endswith(self, other):
        return self.operate(Operator.ENDSWITH, other)

    def is_empty(self):
        return self.operate(Operator.IS_EMPTY, None)

    def is_not_empty(self):
        return self.operate(Operator.IS_NOT_EMPTY, None)

    def before(self, other):
        return self.operate(Operator.BEFORE, other)

    def after(self, other):
        return self.operate(Operator.AFTER, other)
    
    def is_(self, other):
        return self.operate(Operator.EQ, other)
    
    def is_not(self, other):
        return self.operate(Operator.NE, other)
    
def and_(*clauses: ColumnElement) -> BooleanClauseList:
    """Produce a conjunction of expressions joined by ``AND``.

    .. versionadded:: 0.8.0

    ..code:: python
        stmt = select(students).where(
            and_(
                students.c.balance.is_empty(), 
                students.c.grade > Decimal('996.56')
            )
        )

    The :func:`and_` conjunction is also available using the Python ``&`` operator. 
    Note that compound expressions need to be parenthesized in order to function with Python operator precedence behavior.

    ..code:: python
        stmt = select(students).where(
            (students.c.balance.is_empty()) & (students.c.grade > Decimal('996.56'))
        )
        
    Raises:
        ValueError: If less than two ``clauses`` are provided provided.

    Returns:
        BooleanClauseList: The AST node for this expression.
    """
    if len(clauses) < 2:
        raise ValueError("and_() requires at least two clauses")

    return BooleanClauseList(
        operator="and",
        clauses=list(clauses),
    )

def or_(*clauses: ColumnElement) -> BooleanClauseList:
    """Produce a conjunction of expressions joined by ``OR``.

    .. versionadded:: 0.8.0

    ..code:: python
        stmt = select(students).where(
            or_(
                students.c.balance.is_empty(), 
                students.c.grade > Decimal('996.56')
            )
        )

    The :func:`and_` conjunction is also available using the Python ``|`` operator. 
    Note that compound expressions need to be parenthesized in order to function with Python operator precedence behavior.

    ..code:: python
        stmt = select(students).where(
            (students.c.balance.is_empty()) | (students.c.grade > Decimal('996.56'))
        )
        
    Raises:
        ValueError: If less than two ``clauses`` are provided provided.

    Returns:
        BooleanClauseList: The AST node for this expression.
    """
    if len(clauses) < 2:
        raise ValueError("or_() requires at least two clauses")

    return BooleanClauseList(
        operator="or",
        clauses=list(clauses),
    )

def not_(clause: ColumnElement) -> UnaryExpression:
    """Return a negation of the given clause, i.e. ``NOT(clause)``.

    The Python ``~`` operator is also overloaded on all :class:`ColumnElement` subclasses to produce the same result.

    Args:
        clause (ColumnElement): The column element to be negated.

    Returns:
        UnaryExpression: The AST node for this expression.
    """
    return UnaryExpression(
        operator="not",
        element=clause,
    )

class Comparator:
    def __init__(self, expr: ColumnElement):
        self.expr = expr
        self.type_ = expr.type_

    def operate(self, op: Operator, other):
        if op not in self.type_.supported_ops:
            raise TypeError(
                f"Operator {op} not supported for type {type(self.type_).__name__}"
            )

        return BinaryExpression(
            self.expr,
            op,
            coerce_to_bindparam(other, self.type_)
        )

class ObjectIdComparator(Comparator):
    pass

class BooleanComparator(Comparator):
    pass

class StringComparator(Comparator):
    pass

class NumberComparator(Comparator):
    pass

class DateComparator(Comparator):
    pass

class _NoArg(Enum):
    NO_ARG = 0

    def __repr__(self):
        return f"_NoArg.{self.name}"

class BindParameter(ColumnElement):
    def __init__(
        self, 
        key: Optional[str], 
        value: Any =_NoArg.NO_ARG, 
        callable_: Optional[Callable[[], Any]] = None, 
        type_: Optional[TypeEngine]=None
    ):
        self.key = key if key else None
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
