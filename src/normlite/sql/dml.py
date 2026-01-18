# sql/dml.py
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
import pdb
from types import MappingProxyType
from typing import Any, Optional, Protocol, Self, Sequence, Union, TYPE_CHECKING
from normlite._constants import SpecialColumns
from normlite.exceptions import ArgumentError
from normlite.sql.base import Executable, ClauseElement, generative
from normlite.sql.elements import BindParameter, BooleanClauseList, ColumnElement

if TYPE_CHECKING:
    from normlite.sql.schema import Column, Table, ReadOnlyColumnCollection

class ValuesBase(ClauseElement):
    _values: Optional[MappingProxyType] = None
    """The immutable mapping holding the values."""

    def __init__(self, table: Table):
        self.table = table
        self._values = None

    @generative
    def values(self, *args: Union[dict, Sequence[Any]], **kwargs: Any) -> Self:
        """Provide the ``VALUES`` clause to specify the values to be inserted in the new row.

        .. versionchanged:: 0.8.0
            The values provided are now coerced to :class:`normlite.sql.elements.BindParameter`
            objects.

        Raises:
            ArgumentError: If both positional and keyword arguments are passes, or
                if not enough values are supplied for all columns, or if values are passed 
                with a class that is neither a dictionary not a tuple. 

        Returns:
            Self: This instance for generative usage.

        """
        existing = dict(self._values) if self._values else {}

        if args:
            # positional args have been passed:
            # either a dict or a sequence has been provided
            # IMPORTANT: args is a tuple containing the dict or sequence as first element
            arg = args[0]
            if kwargs:
                # either positional or kwargs but not both are allowed
                raise ArgumentError(
                    'Cannot pass positional and keyword arguments '
                    'to values() simultanesously'
                )

            if len(args) != 1:
                raise ArgumentError(
                    "values() accepts at most one positional argument"
                )
            arg = args[0]

            if not isinstance(arg, dict):
                raise ArgumentError(
                    "Positional argument to values() must be a dict"
                )

            new_values = arg
        else:
            new_values = kwargs

        # Convert raw values → BindParameter
        for key, value in new_values.items():
            # structural validation
            if key not in self.table.columns:
                raise ArgumentError(f"Wrong key supplied: {key}")

            if isinstance(value, BindParameter):
                bp = value
            else:
                bp = BindParameter(
                    key=key,
                    value=value,
                    type_=None      # compiler will fill this
                )

            existing[key] = bp

        self._values = MappingProxyType(existing)

class Insert(ValuesBase):
    is_insert = True
    __visit_name__ = 'insert'

    """Provide the SQL ``INSERT`` node to create a new row in the specified table. 

    This class provide a generative implementation of the SQL ``INSERT`` node to be executed on a
    :class:`normlite.connection.Connection`.

    Usage:
        >>> # create a new insert statement for the table students
        >>> stmt = Insert(students)
        ...
        >>> # create a new insert statement and specify the ``RETURNING`` clause.
        >>> stmt = Insert(students).returning(students.c.id, students.c.name)
        ...
        >>> # specify the values to be inserted as keyword arguments
        >>> stmt.values(id=123456, name='Isaac Newton', grade='B')
        ...
        >>> # specify the values to be inserted as dictionary
        >>> stmt.values({'id': 123456, 'name': 'Isaac Newton', 'grade': 'B'})
        ...
        >>> # specify the values to be inserted as tuple
        >>> stmt.values((123456, 'Isaac Newton', 'B'))

    Important:
        The :class:`Insert` has always by default the Notion specific columns as ``RETURNING`` clause.
        That is, the :class:`normlite.CursorResult` methods of an insert statement always returns rows with the
        columns corresponding to the Notion specific ones.
        You specify the :meth:`Insert.returning()` to get additional columns in the rows returned by the 
        :class:`normlite.CursorResult` methods.
    
    Note:
        The :class:`Insert`  can also be constructed without specifying the values.
        In this case, the parameters passed in the :meth:`normlite.connection.Connection.execute()` are bound
        as ``VALUES`` clause parameters at execution time.

    .. versionchanged:: 0.8.0
        New base class

    .. versionchanged:: 0.7.0 
        The old construct has been completely redesigned and refactored.
        Now, the new class provides all features of the SQL ``INSERT`` statement.

    """
    def __init__(self, table: Table):
        super().__init__(table)

        self._table: Table = table
        """The table object to insert a new row to."""

        self._returning = ()
        """The tuple holding the Notion specific columns."""

        for spec_col in SpecialColumns._member_names_:
            self._returning += (spec_col, )

    def _set_table(self, table: Table) -> None:
        self._table = table

    def get_table(self) -> ReadOnlyColumnCollection:
        return self._table.columns
    
    def returning(self, *cols: Column) -> Self:
        """Provide the ``RETURNING`` clause to specify the column to be returned.

        Raises:
            ArgumentError: If a specified column does not belong to the table this insert statement
                is applied to.

        Returns:
            Self: This instance for generative usage.
        """
        if cols:
            for col in cols:
                if col.parent is not self._table:
                    raise ArgumentError(
                        f'Column: {col.name} does not belong to table: {self._table.name}'
                    )
                self._returning += (col.name,)
        
        return self
    
    def _process_dict_values(self, dict_arg: dict) -> MappingProxyType:
        kv_pairs = {}
        try:
            for col in self._table.get_user_defined_colums():
                value = dict_arg[col.name]
                kv_pairs[col.name] = BindParameter(col.name, value, col.type_)
        except KeyError as ke:
            raise KeyError(f'Missing value for: {ke.args[0]}')
        
        return MappingProxyType(kv_pairs)


    def __repr__(self):
        kwarg = []
        if self._table:
            kwarg.append('table')

        if self._values:
            kwarg.append('values')

        
        return "Insert(%s)" % ", ".join(
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg]
        )
        
def insert(table: Table) -> Insert:
    """Construct an insert statement.

    This class constructs an SQL ``INSERT`` statement capable of inserting rows
    to this table.

    .. versionchanged:: 0.7.0
        Now, it uses the :class:`normlite.sql.schema.Table` as table object.

    Returns:
        Insert: A new insert statement for this table. 
    """
    return Insert(table)

class HasExpression(Protocol):
    """Mixin for elements that have an expression."""

    def has_expression(self) -> bool:
        ...

class WhereClause(HasExpression, ColumnElement):
    """Base class for DML statements that have a where-clause.
    
    .. versionadded:: 0.8.0
    """

    __visit_name__ = 'where_clause'

    def __init__(self, expression: Optional[ColumnElement] = None):
        self.expression = expression
        """The column expression in this where clause."""

    def has_expression(self) -> bool:
        """Explicit test on expression availability to avoid :exc:`TypeError`.
        
        Users of the :class:`WhereClause` shall test for presence of an expression 
        by invoking this method and not simply ...
        This safely bypasses Python thruthiness invocation, which otherwise raises
        a :exc:`TyperError`.

        .. seealso::
            :meth:`normlite.sql.elements.ColumnElement.__bool__` 
                This method is overloaded to forbid Python truthiness.             

        Returns:
            bool: ``True`` if :attr:`expression` is not ``None``.
        """
        return self.expression is not None

    def where(self, expr: ColumnElement) -> WhereClause:
        if self.expression is None:
            return WhereClause(expr)

        return WhereClause(
            BooleanClauseList(
                operator="and",
                clauses=[self.expression, expr]
            )
        )
    
class OrderByClause(HasExpression, ClauseElement):
    __visit_name__ = 'order_by_clause'

    def __init__(self, clauses: tuple[ColumnElement, ...] = ()):      
        self.clauses = clauses

    def add(self, *clauses: ColumnElement) -> OrderByClause:
        return OrderByClause(self.clauses + clauses)
    
    def has_expression(self) -> bool:
        return self.clauses

class Select(Executable):
    __visit_name__ = 'select'
    is_select = True

    def __init__(self, *entities: Union[Table, Column]):
        from normlite.sql.schema import Column, Table

        if not entities:
            raise ArgumentError(
                """
                    select() requires either table or a list of columns,
                    no arguments were provided.
                """
            )

        self._whereclause = WhereClause()
        self._order_by = OrderByClause()

        if len(entities) == 1 and isinstance(entities[0], Table):
            # a Table object has been provided
            table = entities[0]
            self.table = table
            self._projection = None  # project all columns
            return

        # A list of columns has bein provided
        tables = set()
        columns: list[Column] = []

        for ent in entities:
            if not isinstance(ent, Column):
                raise ArgumentError(
                    "select() arguments must be either a Table or Column objects"
                )
            tables.add(ent.parent)
            columns.append(ent)

        if len(tables) != 1:
            raise ArgumentError(
                "All selected columns must belong to the same table"
            )

        self.table: Table = tables.pop()
        # --- SAFEGUARD: column names must exist on the table ---
        table_columns = self.table.get_user_defined_colums()

        for col in columns:
            if col.name not in table_columns:
                raise ArgumentError(
                    f'Column: {col.name} does not belong to table: {self.table.name}'
                )
        self._projection = list(columns)

    @generative
    def where(self, expr: ColumnElement) -> Self:
        self._whereclause = self._whereclause.where(expr)
        return self

    @generative
    def order_by(self, *clauses: ColumnElement) -> Self:
        if not clauses:
            raise ArgumentError(
                """
                    order_by() requires at least one clause,
                    no arguments provided.
                """
            )

        self._order_by = self._order_by.add(*clauses)
        return self


def select(*entities: Union[Table, Column]) -> Select:
    return Select(*entities)
        

