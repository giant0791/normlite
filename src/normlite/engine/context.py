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
import pdb
from typing import TYPE_CHECKING, Optional
import copy

from normlite.exceptions import ArgumentError

if TYPE_CHECKING:
    from normlite.sql.base import Compiled
    from normlite.engine.cursor import CursorResult
    from normlite.notiondbapi.dbapi2 import Cursor
    from normlite.sql.elements import BindParameter


class ExecutionContext:
    """Orchestrate binding, compilation, and result setup.

    This class manages the key activities for the execution of SQL statements.
    It binds dynamically the parameters at runtime prior to execution using the owned :class:`normlite.sql.Compiled` object.
    After execution, it sets up the :class:`normlite.cursor.CursorResult` object to be returned using the owned :class:`normlite.notiondbapi.dbapi2.Cursor` 
    (which contains the result set of the executed statement). 

    .. versionchanged:: 0.8.0
        :meth:`_bind_params()` now binds values to named arguments by looking up parameters recursively.
        This enables to support bind parameters for all DDL/DML constructs.

        .. seealso::

            `normlite` SQL compiler :py:mod:`normlite.sql.compiler` module
                Here you find examples illustrating the code emitted for various DDL/DML constructs.

    .. versionadded:: 0.7.0
        :meth:`_bind_params()` expects that parameters have been provided for all columns.
        It raises :exc:`normlite.exceptions.ArgumentError` if this is not the case.

    """
    def __init__(self, dbapi_cursor: Cursor, compiled: Compiled, parameters: Optional[dict] = None):
        self._dbapi_cursor = dbapi_cursor
        """The DBAPI cursor holding the result set of the executed statement."""

        self._compiled = compiled
        """The compiled statement."""

        self._element = compiled._element
        """The statement object."""

        compiled_params = compiled.params or {}
        execution_params = parameters or {}
        
        self._binds = {
            **compiled_params,
            **execution_params
        }
        """The parameters to be bound."""

        self._result = None
        """The constructed :class:`normlite.cursor.CursorResult` to be returned as execution result."""

    def setup(self) -> None:
        """Perform value binding and type adaptation before execution.""" 
        operation = self._compiled.as_dict().get('operation')
        
        template = operation['template']
        payload = self._bind_params(
            copy.deepcopy(template),
            self._compiled.params,
        )

        if self._compiled.params:
            raise ArgumentError(
                f"Unused bind parameters: {', '.join(self._compiled.params.keys())}"
            )

        self._compiled._compiled['operation']['payload'] = payload

    def _resolve_bindparam(self, bindparam: BindParameter, usage: str) -> dict:
        raw = bindparam.callable_() if bindparam.callable_ else bindparam.value
        type_ = bindparam.type

        if usage == "filter":
            processor = type_.filter_value_processor()
        else:
            processor = type_.bind_processor()

        return processor(raw) if processor else raw
     
    def _bind_params(self, template: dict, params: dict) -> dict:
        """Helper for binding parameters at runtime."""

        if isinstance(template, dict):
            return {k: self._bind_params(v, params) for k, v in template.items()}

        elif isinstance(template, list):
            return [self._bind_params(item, params) for item in template]

        elif isinstance(template, str) and template.startswith(":"):
            # parameter placeholder?
            key = template[1:]
            if key not in params:
                raise KeyError(f"Missing parameter: {key}")
            
            bindparam, usage = params.pop(key)
            return self._resolve_bindparam(bindparam, usage)

        # int, float, None …
        return template
        
    def _setup_cursor_result(self) -> CursorResult:
            """Setup the cursor result to be returned."""
            from normlite.engine.cursor import CursorResult
            self._result = CursorResult(self._dbapi_cursor, self._compiled)
            return self._result
