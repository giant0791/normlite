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
from typing import Any, Optional, Self, Sequence, Union, TYPE_CHECKING
from normlite._constants import SpecialColumns
from normlite.exceptions import ArgumentError
from normlite.sql.base import Executable
from normlite.sql.elements import ColumnExpression

if TYPE_CHECKING:
    from normlite.sql.schema import Column, Table, ReadOnlyColumnCollection

class Insert(Executable):
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

    .. versionchanged:: 0.7.0 The old construct has been completely redesigned and refactored.
        Now, the new class provides all features of the SQL ``INSERT`` statement.

    """
    def __init__(self):
        super().__init__()
        self._values: MappingProxyType = None
        """The immutable mapping holding the values."""

        self._table: Table = None
        """The table object to insert a new row to."""

        self._returning = ()
        """The tuple holding the Notion specific columns."""

        for spec_col in SpecialColumns._member_names_:
            self._returning += (spec_col, )

    def _set_table(self, table: Table) -> None:
        self._table = table

    def get_table(self) -> ReadOnlyColumnCollection:
        return self._table.columns
    
    def values(self, *args: Union[dict, Sequence[Any]], **kwargs: Any) -> Self:
        """Provide the ``VALUES`` clause to specify the values to be inserted in the new row.

        Raises:
            ArgumentError: If both positional and keyword arguments are passes, or
                if not enough values are supplied for all columns, or if values are passed 
                with a class that is neither a dictionary not a tuple. 

        Returns:
            Self: This instance for generative usage.

       """
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

            if isinstance(arg, dict):
                self._values = self._process_dict_values(arg)

            elif isinstance(arg, tuple):
                if len(arg) != self._table.c.len():
                    raise ArgumentError(
                        'Not enough values supplied for all columns: '
                        f'Required: {self._table.c.len()}, '
                        f'supplied: {len(arg)}'
                    )
                
                kv_pairs = {col.name: value for col, value in zip(self._table.c, arg)}
                self._values = self._process_dict_values(kv_pairs)
            else:
                raise ArgumentError(
                    f'dict or tuple values are supported only: {arg.__class__.__name__}'
                )

        else:
            # kwargs have been passed        
            self._values = self._process_dict_values(kwargs)

        return self
    
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
            for col in self._table.c:
                if col.name in SpecialColumns.__members__.values():
                    # skip Notion-managed columns
                    continue
                kv_pairs[col.name] = dict_arg[col.name]
        except KeyError as ke:
            raise KeyError(f'Missing value for: {ke.args[0]}')
        
        return MappingProxyType(kv_pairs)
    
class Select(Executable):
    __visit_name__ = 'select'
    is_select = True

    def __init__(self, table: Table):
        self.table = table
        self._whereclause = None

    def where(self, clause: ColumnExpression) -> Self:
        self._whereclause = clause
        return self 

def select(table: Table) -> Select:
    return Select(table)
        

def insert(table: Table) -> Insert:
    """Construct an insert statement.

    This class constructs an SQL ``INSERT`` statement capable of inserting rows
    to this table.

    .. versionchanged:: 0.7.0
        Now, it uses the :class:`normlite.sql.schema.Table` as table object.

    Returns:
        Insert: A new insert statement for this table. 
    """
    insert_stmt = Insert()
    insert_stmt._set_table(table)
    return insert_stmt
