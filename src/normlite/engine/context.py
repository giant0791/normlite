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

from normlite.exceptions import ArgumentError, InvalidRequestError
from normlite.engine.interfaces import _CoreMultiExecuteParams, ExecutionOptions
from normlite.sql._sentinels import VALUE_PLACEHOLDER
from normlite.sql.resultschema import SchemaInfo
from normlite.utils import frozendict

if TYPE_CHECKING:
    from normlite.sql.base import Compiled
    from normlite.engine.base import Engine, Connection
    from normlite.engine.cursor import CursorResult
    from normlite.notiondbapi.dbapi2 import Cursor as DBAPICursor
    from normlite.sql.base import Executable
    from normlite.sql.elements import BindParameter

class ExecutionStyle(Enum):
    """Define the execution style for a context.
    
    .. versionadded:: 0.8.0
    """
    
    EXECUTE = auto()
    """A single operation is executed.
    
    It indicates cursor.execute() will be used.

    .. versionchanged:: 0.9.0
    """

    EXECUTEMANY = auto()
    """Multiple operations are executed on a query result.

    It indicates that cursor.execute() will be used.
    This execution style is used for DELETE/UPDATE statements where the execution loop is
    driven by the query results.

    .. versionchanged:: 0.9.0
    """

    PARAMETER_BULK = auto()
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

    engine: Engine
    """Engine which the connection is associated with.
    
    .. versionadded:: 0.8.0
    """

    connection: Connection
    """Connection associated with the engine.
    
    .. versionadded::0.8.0
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
            "payload" : [
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

    .. versionadded:: 0.8.0
    """

    distilled_params: _CoreMultiExecuteParams
    """The normalized bind parameters containing the values to be bound into :attr:`compiled_dict`.
    
    .. versionadded:: 0.8.0
    """

    path_params: Optional[dict]
    """The path params for the DBAPI operation.
    
    .. versionadded:: 0.8.0
    """

    query_params: Optional[dict]
    """The query params for the DBAPI operation.
    
    .. versionadded:: 0.8.0
    """

    payload: Optional[list[dict]]
    """The payload parameter for the DBAPI operation.
    
    ..versionadded:: 0.8.0
    """

    execution_style: ExecutionStyle
    """The style of DBAPI cursor method that will be used to execute a statement.

    .. versionadded:: 0.8.0
    """

    execution_options: ExecutionOptions
    """Execution options associated with the current statement execution.
    
    .. versionadded:: 0.8.0
    """

    _result: Optional[CursorResult]
    """The cursor result of the operation(s) executed in this context.
    
    .. versionadded:: 0.8.0
    """

    _rowcount: Optional[int] 
    """The rowcount returned by an INSERT/UPDATE/DELETE/SELECT statement.
    
    .. versionadded: 0.9.0
    """

    bulk_operation: Optional[dict]
    """The operation to be executed when :attr:`execution_style` is :attr:`ExecutionStyle.EXCUTEMANY`.
    
    .. versionadded:: 0.9.0
    """

    bulk_parameters: Optional[list[dict]]
    """The parameters set for the bulk operation when :attr:`execution_style` is :attr:`ExecutionStyle.EXCUTEMANY`.

    .. versionadded:: 0.9.0
    """

    internal_cursor: Optional[DBAPICursor]
    """The cursor used internally for prefetching Notion pages in delete/update.
    
    .. versionadded:: 0.9.0
    """



    def __init__(
            self,
            engine: Engine,
            connection: Connection, 
            cursor: DBAPICursor, 
            compiled: Compiled, 
            distilled_params: Optional[_CoreMultiExecuteParams] = None,
            *,
            execution_options: Optional[Mapping[str, Any]] = None
    ) -> None:
        self.engine = engine
        self.connection = connection
        self.cursor = cursor
        self.compiled = compiled
        self.compiled_dict = compiled.as_dict()
        self.invoked_stmt = compiled._element
        self.execution_options = execution_options or frozendict() 
        self.distilled_params = distilled_params or [{}]
        self.path_params = None
        self.query_params = None
        self.payload = None
        self._result = None
        self._rowcount = None
        self.bulk_operation = None
        self.bulk_parameters = None
        self.internal_cursor = None

    def _determine_execution_style(self) -> ExecutionStyle:
        stmt = self.invoked_stmt

        # delete
        if stmt.is_delete:
            return ExecutionStyle.EXECUTEMANY

        return ExecutionStyle.EXECUTE

    @property
    def operation(self) -> dict:
        """Return the DBAPI operation.
        
        .. versionadded:: 0.8.0
        """
        return self.compiled_dict['operation']
    
    @property
    def parameters(self) -> dict:
        """Return the DBAPI parameters for the related operation.

        This attribute provides the DBAPI parameters as a dictionary with the following keys:

        - "path_params": This stores the path parameters for the DBAPI operation.

        - "query_params": This stores the query paramters for the DBAPI operation.

        - "payload": This stores the payload for the DBAPI operation.
        
        .. versionadded:: 0.8.0
        """

        dbapi_params = {}
        if self.path_params:
            dbapi_params['path_params'] = self.path_params

        if self.query_params:
            dbapi_params['query_params'] = self.query_params

        if self.payload:
            dbapi_params['payload'] = self.payload

        return dbapi_params
    
    def pre_exec(self) -> None:
        """Perform value binding and type adaptation before execution.

        .. versionchanged:: 0.8.0
            In this version, binding has been extended to support the override case (user-provided parameters in the execute call).
            Execution options resolution is also supported now.
        """ 
        # determine execution style
        self.execution_style = self._determine_execution_style()

        # resolve parameters only if there are parameters to be resolved
        resolved_params = (
            self.compiled.params
            if self.distilled_params is None
            else
            self._resolve_parameters(self.distilled_params)
        )

        # validate insert values, if stmt is_insert
        if self.compiled._compiler_state.is_insert:
            self._validate_insert_values(resolved_params)
        
 
        # resolve execution options
        self._resolve_exec_options()

        # bind the parameters for execution with the resolved values
        if 'path_params' in self.compiled_dict:
            self.path_params = self._bind_params(
                copy.deepcopy(self.compiled_dict['path_params']), 
                resolved_params
            )

        if 'payload' in self.compiled_dict:
            self.payload = self._bind_params(
                    copy.deepcopy(self.compiled_dict['payload']),
                    resolved_params
            )
            

        if resolved_params:
            raise ArgumentError(
                f"Unused bind parameters: {', '.join(resolved_params.keys())}"
            )
        
       # extract the query params
        if 'query_params' in self.compiled_dict:
            self.query_params = self.compiled_dict['query_params']

        # inject schema info if invoked statement is not DDL
        if self.invoked_stmt.is_ddl:
            return
        
        schema = SchemaInfo.from_table(
            self.invoked_stmt.get_table(), 
            projected_usr_names=self.compiled.result_columns()
        )
        self.cursor._inject_description(schema.as_sequence())

    def post_exec(self) -> None:
        """Perform row counting preservation after execution.
        
        .. versionchanged:: 0.9.0
        """
        if self.invoked_stmt.is_ddl:
            # for DDL statements rowcount is always -1
            self._rowcount = -1
            return

        exec_opts = self.execution_options
        if exec_opts.get('preserve_rowcount', False):
            self._rowcount =  self.cursor.rowcount
            return
        
        self._rowcount = -1
    
    def _resolve_parameters(self, overrides: _CoreMultiExecuteParams) -> Mapping[str, BindParameter]:
        """Resolve binding parameters using the values passed as normalized parameters in the constructor.
        
        .. versionadded:: 0.8.0
        """
        
        from normlite.sql.elements import BindParameter

        resolved_params: Mapping[str, Any] = dict(self.compiled.params)
        distilled_params: Mapping[str, Any] = None
        if len(overrides) == 1:
            # distilled parameters contain 1 mapping only
            distilled_params = overrides[0]
        else:
            NotImplementedError('Execution style executemanyvalues not implemented yet.')

        # override with distilled parameters 
        for key in distilled_params.keys():
            old_value: BindParameter = resolved_params[key]
            new_value = BindParameter(
                key=key,
                value=distilled_params[key],
                type_=old_value.type_,
            )
            new_value.role = old_value.role
            resolved_params.update({key: new_value})

        return resolved_params

    def _validate_insert_values(self, resolved_parameters: dict[str, BindParameter]):
        missing_cols = [
            key
            for key, bindparam in resolved_parameters.items()
            if bindparam.value is VALUE_PLACEHOLDER
        ]

        if missing_cols:
            table_name = self.compiled._compiler_state.stmt.get_table().name
            cols_word = "column" if len(missing_cols) == 1 else "columns"
            formatted = ", ".join(f"'{col}'" for col in missing_cols)
            raise ArgumentError(
                f"Missing values for {cols_word} {formatted} in INSERT into '{table_name}'."
            )

    def _resolve_exec_options(self) -> None:
        opts = {}

        # update with engine and connection options
        opts.update(self.engine.get_execution_options())
        opts.update(self.connection.get_execution_options())

        # update with the statement options
        if self.invoked_stmt._execution_options:
            opts.update(self.invoked_stmt._execution_options)

        # update with the call options
        opts.update(self.execution_options)

        self.execution_options = frozendict(opts)

    def setup(self) -> None:
        raise NotImplementedError
    
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
        
    def setup_cursor_result(self) -> CursorResult:
        """Finalize execution and materialize a :class:`normlite.engine.cursor.CursorResult`.

        This method represents the **terminal step of the execution pipeline**.
        It materializes a read-only façade (`CursorResult`) over the DBAPI cursor's
        post-execution state and freezes the outcome of this execution.

        Key design guarantees
        ---------------------
        * **Execution is frozen**
        Once this method is called, the execution outcome is considered final.
        No further mutation of the underlying cursor or execution context is
        permitted or expected.

        * **Single-execution binding**
        The returned :class:`CursorResult` is bound to exactly one execution of
        one compiled statement. Each :class:`ExecutionContext` may produce
        **at most one** result object.

        * **Materialized result façade**
        The :class:`CursorResult` acts as a read-only façade over the cursor’s
        final state. It exposes rows, metadata, and identity information derived
        from the cursor without copying or re-buffering data.

        * **Identity preservation**
        At the time this method is called:
            - execution has fully completed
            - all row and object identities (e.g. Notion object IDs / lastrowid)
            have been resolved and preserved
            - the cursor reflects the final, stable execution state

        * **Source of truth**
        The execution context and its cursor remain the authoritative source
        of execution data. The result object does not mutate or re-interpret
        execution state.

        Lifecycle and usage
        -------------------
        This method is **not intended for direct invocation by users**.
        It is called internally by the connection execution pipeline once
        statement execution has completed successfully.

        Repeated calls to this method return the same :class:`CursorResult`
        instance, ensuring idempotence and enforcing one-time result creation.

        .. note::
           - The underlying DBAPI cursor must not be mutated after this method has been invoked.
           - Result consumption (iteration, scalar access, identity inspection)
            may occur lazily, but the execution itself is fully complete.

        .. versionchanged:: 0.8.0
            This version formalizes structural membership and API contract.
            
        Returns:
            CursorResult: A read-only result object representing the finalized outcome of
                this execution.
        """
        from normlite.engine.cursor import CursorResult

        if self._result is None:
            self._result = CursorResult(self)

        return self._result

    def get_rowcount(self) -> Optional[int]:        
        """Return the DBAPI ``cursor.rowcount`` value, or in some
        cases an interpreted value.

        See :attr:`normlite.engine.cursor.CursorResult.rowcount` for details on this.

        .. versionadded:: 0.9.0
        """
        return self._rowcount
