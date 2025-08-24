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
from types import MappingProxyType
from typing import Any, Optional, Self, Sequence, Union, TYPE_CHECKING
from normlite.exceptions import ArgumentError
from normlite.sql.base import Executable
from normlite.sql.sql import CreateTable, SqlNode

if TYPE_CHECKING:
    from normlite.sql.schema import Column, Table

class Insert(SqlNode):
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
        >>> stmt.values({'id': 123456, 'name':'Isaac Newton', 'grade': 'B'})
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
        self._values: MappingProxyType = None
        """The immutable mapping holding the values."""

        self._table: Table = None
        """The table object to insert a new row to."""

        self._returning = ('_no_id', '_no_archived', )
        """The tuple holding the """

    def accept(self, visitor):
        """Not supperted yet."""
        ...

    def _set_table(self, table: Table) -> None:
        self._table = table

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
                if col.name in ['_no_id', '_no_archived']:
                    # skip Notion-managed columns
                    continue
                kv_pairs[col.name] = dict_arg[col.name]
        except KeyError as ke:
            raise KeyError(f'Missing value for: {ke.args[0]}')
        
        return MappingProxyType(kv_pairs)

class SQLCompiler:
    """Provide the central compiler for all SQL executables."""
    
    _stmt_map = {
        'Insert': {'endpoint': 'pages', 'request': 'create'}
    }

    def compile_insert(self, ins_stmt: Insert) -> dict:
        # construct the operation object
        operation = SQLCompiler._stmt_map['Insert']

        # construct the payload
        payload = {
            "parent": { 
                "type": "database_id",
                "database_id": ins_stmt._table._database_id
            }
        }

        # construct the properties wih placeholder values
        properties = {}

        for col in ins_stmt._table.columns:
            col_val = f':{col.name}:'

            if col.type == 'int':
                # IMPORTANT: 
                # Currently the col.type member contains the SQL type
                # not the Notion type
                properties[col.name] = {"number": col_val}
            elif col.type.startswith("title_varchar"):
                properties[col.name] = {
                    "title": [{"text": {"content": col_val}}]
                }
            elif col.type.startswith("varchar"):
                properties[col.name] = {
                    "rich_text": [{"text": {"content": col_val}}]
                }
            else:
                raise TypeError(f"Unsupported type for column '{col.name}': {col.type}")

        payload['properties'] = properties

        # construct the parameters
        parameters = {}
        parameters['payload'] = payload
        parameters['params'] = ins_stmt._values

        return {'operation': operation, 'parameters': parameters}

class OldInsert(Executable):
    """Provide an insert statement to add rows to the associated table.

    This class respresents an SQL ``INSERT`` statement. Every insert statement is associated
    to the table it adds rows to.

    Warning:
        This is going to be removed. Don't use!
        Use :class:`Insert` instead.

    """
    def __init__(self, table: CreateTable):
        self._table = table
        """The table subject of the insert."""

        self._values: dict = {}
        """The mapping column name, column value."""
        
        self._operation: dict = {}
        """The dictionary containing the compiled operation."""
        
        self._parameters: dict = {}
        """The dictionary containing the compiled parameters."""

    def prepare(self) -> None:
        # cross-compile the insert statement. At the end:
        # self._operation contains the keys "endpoint" and "request"
        # self._parameters contains the keys "payload" and "params"
        # 1. cross-compile
        sql_compiler: SQLCompiler = SQLCompiler()
        compiled_stmt = sql_compiler.compile_insert(self)

        # the cross-compilation result is a dictionary containing the following keys:
        # "operation": {"endpoint": "<some_ep>", "request": "<some_req>"}
        # "parameters": {"payload": {<payload object>}, "parameters": {<parameters object>}} 
        self._operation = compiled_stmt.get('operation')
        self._parameters = compiled_stmt.get('parameters')

    def bindparams(self, parameters: Optional[dict]) -> None:
        """Bind (assign) the parameters to the insert values clause."""
        if not parameters:
            raise Exception(
                'Expected bind parameters for this statement. '
                'None was supplied or no binding performed yet.'
            )

        for col in self._table.columns:
            self._values[col.name] = parameters[col.name]

    def operation(self):
        return self._operation

    def parameters(self):
        return self._parameters

def old_insert(table: CreateTable) -> OldInsert:
    """Construct an insert statement.

    This class constructs an SQL ``INSERT`` statement capable of inserting rows
    to this table.

    Returns:
        OldInsert: A new insert statement for this table. 

    Warning:
        This is going to be removed. Don't use!
        Use :func:`insert()` instead.
    """
    return OldInsert(table)

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
