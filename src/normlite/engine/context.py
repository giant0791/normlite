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
from enum import Enum, auto
import pdb
from typing import TYPE_CHECKING, Any, Mapping, Optional
import copy

from normlite.exceptions import ArgumentError
from normlite.engine.interfaces import _CoreMultiExecuteParams
from normlite.utils import frozendict

if TYPE_CHECKING:
    from normlite.sql.base import Compiled
    from normlite.engine.cursor import CursorResult
    from normlite.notiondbapi.dbapi2 import Cursor as DBAPICursor
    from normlite.sql.base import Executable

class ExecutionStyle(Enum):
    """Define the execution style for a context.
    
    .. versionadded:: 0.8.0
    """
    
    EXECUTE = auto()
    """A single operation is executed."""

    EXECUTEMANY = auto()
    """Multiple operations are executed on a result.
    
    This execution style is used for DELETE/UPDATE statements where the execution loop is
    driven by the query results.
    """

    INSERTMANYVALUES = auto()
    """The same operation is executed multiple on time with different parameters.
    
    This execution style is used for INSERT statements that add multiple rows. The execution loop is
    driven by the parameters.
    """

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

    cursor: DBAPICursor
    """The DBAPI cursor holding the result set of the executed statement.
    
    .. versionchanged:: 0.8.0
    """

    compiled: Compiled
    """The compiled statement.
    
    .. versionchanged:: 0.8.0
    """

    compiled_dict: dict
    """The JSON object representing result of the compilation + parameter binding.

    This attribute must be computed prior to executing the context.
    
    .. versionadded:: 0.8.0

    Example::

        # insert many values, note "payload" is a list containing each row to be added
        {
            "operation": {
                "endpoint": "pages",
                "request": "create",
            },
            "payload" :[
                {
                    "parent": {
                        "type": "database_id",
                        "database_id": "59833787-2cf9-4fdf-8782-e53db20768a5",
                    },
                    "properties": {
                        "name": {
                            "title": [
                                {
                                    "text": {
                                        "content": "Galileo Galilei"
                                    }
                                }
                            ]
                        },
                        "is_active" : {
                            "checkbox": True
                        }
                    }
                },
                {
                    "parent": {
                        "type": "database_id",
                        "database_id": "59833787-2cf9-4fdf-8782-e53db20768a5",
                    },
                    "properties": {
                        "name": {
                            "title": [
                                {
                                    "text": {
                                        "content": "Isaac Newton"
                                    }
                                }
                            ]
                        },
                        "is_active" : {
                            "checkbox": False
                        }
                    }
                },
            ]
        }
    """

    invoked_stmt: Optional[Executable]
    """The :class:`normlite.sql.base.Executable` statement object that was given in the first place.

    This should be structurally equivalent to ``compiled.statement``, but not
    necessarily the same object as in a caching scenario the compiled form
    will have been extracted from the cache.

    .. versionadded: 0.8.0
    """

    distilled_params: _CoreMultiExecuteParams
    """The normalized bind parameters containing the values to be bound into :attr:`compiled_dict`.
    
    .. versionadded: 0.8.0
    """

    path_params: Optional[dict]
    query_params: Optional[dict]
    payload: Optional[list[dict]]

    execution_style: ExecutionStyle
    """the style of DBAPI cursor method that will be used to execute a statement.

    .. versionadded:: 0.8.0

    """

    no_parameters: bool
    """``True`` if the execution style does not use parameters"""

    execution_options: Mapping[str, Any]
    """Execution options associated with the current statement execution."""

    def __init__(
            self, 
            cursor: DBAPICursor, 
            compiled: Compiled, 
            execution_options: Optional[Mapping[str, Any]] = None,
            distilled_params: Optional[_CoreMultiExecuteParams] = None
    ) -> None:
        self.cursor = cursor
        self.compiled = compiled
        self.compiled_dict = compiled.as_dict()
        self.invoked_stmt = compiled._element
        if execution_options is not None:
            self.execution_options = frozendict(execution_options)
        else:
            # set default values for execution options
            self.execution_options = frozendict(page_size=25, preserve_rowid=True)

        if distilled_params is None:
            self.no_parameters = True
        else:
            self.no_parameters = False

        self.distilled_params = distilled_params

    @property
    def operation(self) -> dict:
        return self.compiled_dict['operation']
    
    def pre_exec(self) -> None:
        raise NotImplementedError
    
    def post_exec(self) -> None:
        raise NotImplementedError
    
    def _resolve_parameters(self, overrides: _CoreMultiExecuteParams) -> None:
        """Resolve binding parameters using the values passed as normalized parameters in the constructor.
        
        .. versionadded:: 0.8.0
        """
        
        from normlite.sql.elements import BindParameter
        if self.no_parameters:
            return

        resolved_params: Mapping[str, Any] = dict(self.compiled.params)
        distilled_params: Mapping[str, Any] = None
        if len(overrides) == 1:
            # distilled parameters contain 1 mapping only
            self.execution_style = ExecutionStyle.EXECUTE
            distilled_params = overrides[0]
        else:
            NotImplementedError('Execution style executemanyvalues not implemented yet.')

        # 1. override with distilled parameters 
        for key in distilled_params.keys():
            old_value: BindParameter = resolved_params[key]
            new_value = BindParameter(
                key=key,
                value=distilled_params[key],
                type_=old_value.type_,
            )
            new_value.role = old_value.role
            resolved_params.update({key: new_value})


        #pdb.set_trace()
        # 2. bind the parameters for execution with the resolved values
        if 'path_params' in self.compiled_dict:
            self.path_params = self._bind_params(
                copy.deepcopy(self.compiled_dict['path_params']), 
                resolved_params
            )

        if 'payload' in self.compiled_dict:
            self.payload = [
                self._bind_params(
                    copy.deepcopy(self.compiled_dict['payload']),
                    resolved_params
                )
            ]

        if resolved_params:
            raise ArgumentError(
                f"Unused bind parameters: {', '.join(resolved_params.keys())}"
            )

    def _resolve_exec_options(self):
        raise NotImplementedError

    def setup(self) -> None:
        """Perform value binding and type adaptation before execution.

        .. admonition:: TODO
            This method shall be used in future versions to perform execution option resolution, 
            i.e. overriding options that have been set at engine creation level with those provided at
            connection creation or in the execute call.
        
        .. versionchanged:: 0.8.0
            In this version, binding has been extended to support the override case (user-provided parameters in the execute call).
            Execution options resolution is not supported yet.
        """ 
        self._resolve_parameters(self.distilled_params)

    def _resolve_bindparam(self, bindparam: BindParameter) -> dict:
        from normlite.sql.elements import _BindRole

        raw = bindparam.callable_() if bindparam.callable_ else bindparam.value
        type_ = bindparam.type_

        if bindparam.role == _BindRole.COLUMN_FILTER:
            processor = type_.filter_value_processor()
        elif bindparam.role == _BindRole.COLUMN_VALUE:
            processor = type_.bind_processor()
        elif bindparam.role == _BindRole.DBAPI_PARAM:
            processor = None
        else:
            raise RuntimeError(f'Bind parameter: {bindparam.key} has incorrect role: {bindparam.role}')

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
            
            bindparam = params.pop(key)
            return self._resolve_bindparam(bindparam)

        # int, float, None …
        return template
        
    def _setup_cursor_result(self) -> CursorResult:
        """Setup the cursor result to be returned."""
        from normlite.engine.cursor import CursorResult
        self._result = CursorResult(self._dbapi_cursor, self._compiled)
        return self._result
        


class DMLExecContex(ExecutionContext):
    pass
