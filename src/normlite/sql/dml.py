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
from typing import Optional
from normlite.sql.base import Executable
from normlite.sql.sql import CreateTable

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

class Insert(Executable):
    """Provide an insert statement to add rows to the associated table.

    This class respresents an SQL ``INSERT`` statement. Every insert statement is associated
    to the table it adds rows to.

    .. versionadded:: 0.7.0

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

def insert(table: CreateTable) -> Insert:
    """Construct an insert statement.

    This class constructs an SQL ``INSERT`` statement capable of inserting rows
    to this table.

    Returns:
        Insert: A new insert statement for this table. 
    """
    return Insert(table)
