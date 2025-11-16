# normlite/engine/context.py 
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
"""Provide key runtime abstraction for executing SQL statements.

The design of SQL statement execution separates responsibilities cleanly using the following key classes:

.. list-table:: SQL Statement Execution Design
   :header-rows: 1
   :widths: 23 77
   :class: longtable

   * - Class
     - Responsibility
   * - :class:`normlite.sql.base.Executable`
     - Describes *what* to execute (e.g. :class:`normlite.sql.ddl.CreateTable`, :class:`normlite.sql.dml.Insert`) 
       :class:`notiondbapi._model.NotionPage` for more details).
   * - :class:`normlite.sql.base.SQLCompiler`
     - Translates the :class:`normlite.sql.base.Executable` into a serializable payload.
   * - :class:`ExecutionContext`
     - Orchestrates binding, compilation, and result setup.
   * - :class:`normlite.engine.base.Connection`
     - Owns the DBAPI connection and *executes* statements.
   * - :class:`normlite.engine.base.Engine`
     - Factory for :class:`normlite.sql.base.SQLCompiler` and :class:`Connection`.


.. versionadded:: 0.7.0
    :class:`ExecutionContext` orchestrates binding, compilation, and result setup.

"""
from __future__ import annotations
from typing import TYPE_CHECKING
import copy

from normlite.cursor import CursorResult
from normlite.exceptions import ArgumentError
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.type_api import TypeEngine

if TYPE_CHECKING:
    from normlite.sql.base import Compiled

class ExecutionContext:
    """Orchestrate binding, compilation, and result setup.

    This class manages the key activities for the execution of SQL statements.
    It binds dynamically the parameters at runtime prior to execution using the owned :class:`normlite.sql.Compiled` object.
    After execution, it sets up the :class:`normlite.cursor.CursorResult` object to be returned using the owned :class:`normlite.notiondbapi.dbapi2.Cursor` 
    (which contains the result set of the executed statement). 

    .. versionadded:: 0.7.0
        :meth:`_bind_params()` expects that parameters have been provided for all columns.
        It raises :exc:`normlite.exceptions.ArgumentError` if this is not the case.

    """
    def __init__(self, dbapi_cursor: Cursor, compiled: Compiled):
        self._dbapi_cursor = dbapi_cursor
        """The DBAPI cursor holding the result set of the executed statement."""

        self._compiled = compiled
        """The compiled statement."""

        self._element = compiled._element
        """The statement object."""

        self._binds = compiled.params
        """The parameters to be bound."""

        self._result = None
        """The constructed :class:`normlite.cursor.CursorResult` to be returned as execution result."""

    def setup(self) -> None:
        """Perform value binding and type adaptation before execution.""" 
        operation = self._compiled.as_dict().get('operation')
        params = self._binds
        
        if self._binds:
            # bind the parameters into the template and build the payload
            payload = self._bind_params(operation['template'], params)

        else:
            payload = operation['template']

        self._compiled._compiled['operation']['payload'] = payload
        
    def _bind_params(self, template: dict, params: dict) -> dict:
        """Helper for binding parameters at runtime."""

        payload = copy.deepcopy(template)
        properties = payload.get('properties')
        payload_properties = {}
        for pname, pvalue in params.items():
            # params = {'student_id': 123456, 'name': 'Galileo Galilei', 'grade': 'A'}
            # pname = 'student_id', pvalue = 123456
            # properties = {{'student_id': {'number': ':student_id'}, {'name': {'title': {'text': {'context': ':name'}}}}, ...}
            col_type: TypeEngine = self._element.get_table()[pname].type_
            # col_type = Integer()

            bind_processor = col_type.bind_processor()
            bound_value = bind_processor(pvalue)
            # bound_value = {'number': 123456}
            
            payload_properties[pname] = bound_value
            # payload_properties = {'student_id': {'number': 123456}}

            properties.pop(pname)
            # properties = {{'name': {'title': {'text': {'context': ':name'}}}}, ...}
        
        if not properties:
            # params does not contain all binding values
            not_bound = [key for key in properties.keys()]
            not_bound_cols = ', '.join(not_bound)
            raise ArgumentError(
                f'Could not bind all columns: {not_bound_cols}.'
            )

        payload['properties'] = payload_properties
        return payload

    def _setup_cursor_result(self, cursor: Cursor) -> CursorResult:
            """Setup the cursor result to be returned."""
            self._result = CursorResult(cursor, self._compiled.result_columns())
            return self._result
