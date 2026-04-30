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
from typing import TYPE_CHECKING, Any, Mapping, Optional, Union, Sequence
import copy

from normlite.exceptions import ArgumentError, StatementError
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

    INSERTMANYVALUES = auto()
    """The same operation is executed multiple times with different parameters.
    
    This execution style is used for INSERT statements that add multiple rows. The execution loop is
    driven by the parameters.

    .. versionadded:: 0.9.0
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

    _cursor: Optional[DBAPICursor]
    """The DBAPI cursor holding the result set of the executed statement if no pre-fetch/post-fetch 
    is required.

    .. seealso::
        :attr:`ExecutionContext._result_cursor`

    .. versionchanged: 0.9.0

    .. versionadded:: 0.8.0
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
    
    .. versionadded:: 0.8.0
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
    
    .. versionadded:: 0.9.0
    """

    _returned_primary_keys_rows: Optional[list[tuple]]
    """The list of primary keys returned by the last executed DML statement as row.
    
    .. versionadded:: 0.9.0 
    """

    bulk_operation: Optional[dict]
    """The operation to be executed when :attr:`execution_style` is :attr:`ExecutionStyle.EXCUTEMANY`.
    
    .. versionadded:: 0.9.0
    """

    bulk_parameters: Optional[list[dict]]
    """The parameters set for the bulk operation when :attr:`execution_style` is :attr:`ExecutionStyle.EXCUTEMANY`.

    .. versionadded:: 0.9.0
    """

    _result_cursor: Optional[DBAPICursor]
    """The cursor used internally for prefetching/postfetching Notion pages in delete/update/insert.
    
    .. versionadded:: 0.9.0
    """

    _staged_result_cursor: Optional[DBAPICursor]
    """The cursor used internally to route results after pre-fetch/post-fetching.
    
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
        self._cursor = cursor
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
        self._returned_primary_keys_rows = None
        self.bulk_operation = None
        self.bulk_parameters = None
        self._result_cursor = None
        self._staged_result_cursor = None

    @property
    def cursor(self) -> Optional[DBAPICursor]:
        """Return the effective DBAPI cursor for this execution.

        This property implements **cursor routing**, selecting the cursor that
        represents the *final, user-visible outcome* of the execution.
        
        Design principle
        ----------------
        The decision of *which cursor to expose* is made **entirely upstream**
        during the execution pipeline, specifically in the statement’s
        ``_finalize_execution()`` phase. This property does **not** contain any
        conditional logic related to statement type (INSERT/UPDATE/DELETE),
        execution options, or returning behavior.

        Instead, it simply reflects the outcome of that decision:

        - If a post-processing step (e.g. bulk update, post-fetch, RETURNING)
        has produced a new cursor, it will have been assigned to
        :attr:`_result_cursor` and is returned here.
        - Otherwise, the original execution cursor (:attr:`_cursor`) is returned.

        .. versionadded:: 0.9.0
        """
        return self._result_cursor or self._cursor

    def _get_exec_cursor(self) -> DBAPICursor:
        """Return the execution cursor to be used in the pipeline.
        
        This method is a private API that only :class:`normlite.engine.base.Connection` may use.
        It hides the implementaion details related to which cursor shall be used to execute the DBAPI operation.
        The cursor is crucial because it holds the result set(s). Different cursors are created, 
        depending on the statement being executed.
        For ``EXECUTE`` and ``INSERTMANYVALUES`` style statements (e.g., SELECT or bulk inserts), the :attr:`_cursor` is used to execute the operation
        and to hold the corresponding result set.
        For ``EXECUTEMANY`` style statements (e.g., DELETE/UPDATE/INSERT...RETURNING), the :attr:`_staged_result_cursor`
        is used to execute the operation and to hold the corresponding result set.

        .. versionadded:: 0.9.0
        """
        if self.execution_style in (ExecutionStyle.EXECUTE, ExecutionStyle.INSERTMANYVALUES):
            return self._cursor
        
        return self._staged_result_cursor

    def _determine_execution_style(self) -> ExecutionStyle:
        if len(self.distilled_params) > 1:
            # user has provided a list of dictionaries as parameters to Connection.execute()
            return ExecutionStyle.INSERTMANYVALUES 

        stmt = self.invoked_stmt

        # insert with multi parameters
        if stmt.is_insert and stmt._has_multi_parameters:
            return ExecutionStyle.INSERTMANYVALUES

        # delete or insert with single parameters
        if stmt.is_delete:
            return ExecutionStyle.EXECUTEMANY
        
        # select or insert without returning
        return ExecutionStyle.EXECUTE

    @property
    def operation(self) -> dict:
        """Return the DBAPI operation.
        
        .. versionadded:: 0.8.0
        """
        return self.compiled_dict['operation']
    
    @property
    def parameters(self) -> Union[dict, list[dict]]:
        """Return the DBAPI parameters for the related operation.

        This attribute provides the DBAPI parameters as a dictionary with the following keys:

        - "path_params": This stores the path parameters for the DBAPI operation.

        - "query_params": This stores the query paramters for the DBAPI operation.

        - "payload": This stores the payload for the DBAPI operation.

        :attr:`parameters` is a materialized view based on the parameters calculations done in
        :meth:`pre_exec`. It aggregates the computed attributes :attr:`path_params`, :attr:`query_params`,
        and :attr:`payload` into the DBAPI parameters dictionary structure.

        .. seealso::
            :class:`normlite.notion_sdk.client.AbstractNotionClient` for the client parameters API structure.
        
        .. versionchanged:: 0.9.0    
        
        .. versionadded:: 0.8.0
        """

        if self.execution_style == ExecutionStyle.INSERTMANYVALUES:
            if not isinstance(self.payload, list):
                RuntimeError("INSERTMANYVALUES requires DBAPI 'payload' parameter to be a list")

            return [
                {
                    "payload": payload
                }
                for payload in self.payload

            ]

        if not isinstance(self.payload, dict):
            RuntimeError("EXECUTE and EXCUTEMANY require DBAPI 'payload' parameter to be a dictionary")

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

        ..versionchanged: 0.9.0
            In this version, binding supports bulk inserts with multi-parameters.

        .. versionchanged:: 0.8.0
            In this version, binding has been extended to support the override case (user-provided parameters in the execute call).
            Execution options resolution is also supported now.
        """ 
        # determine execution style
        self.execution_style = self._determine_execution_style()

        # build overrides sets
        overrides = self._build_overrides_sets()

        # resolve parameters only if there are parameters to be resolved
        resolved_params = self._resolve_parameters(overrides)

        # validate insert values, if stmt is_insert
        if self.compiled._compiler_state.is_insert:
            self._validate_insert_values(resolved_params)
        
 
        # resolve execution options
        self._resolve_exec_options()

        if 'path_params' in self.compiled_dict:
            self.path_params = self._bind_params(
                self.compiled_dict['path_params'], 
                resolved_params[0]
            )

        # bind the parameters for execution with the resolved values
        if 'payload' in self.compiled_dict:
            template = self.compiled_dict["payload"]

            if self.execution_style == ExecutionStyle.INSERTMANYVALUES:
                self.payload = [
                    self._bind_params(template, param_set)
                    for param_set in resolved_params
                ]
            else:
                self.payload = self._bind_params(template, resolved_params[0])

        self._assert_all_params_consumed(resolved_params)

        # extract the query params
        if 'query_params' in self.compiled_dict:
            self.query_params = self.compiled_dict['query_params']

        # inject schema info if invoked statement is not DDL
        if self.invoked_stmt.is_ddl:
            return
        
        schema = SchemaInfo.from_table(
            self.invoked_stmt.get_table(),
            execution_names=self.compiled.fetch_columns(), 
            projected_names=self.compiled.result_columns()
        )
    
        self._cursor._inject_description(schema.as_sequence())

    def _assert_all_params_consumed(self, resolved_params: list[dict]):
        for i, param_set in enumerate(resolved_params):
            if param_set:
                raise ArgumentError(
                    f"Unused bind parameters in parameter set {i}: "
                    f"{', '.join(param_set.keys())}"
                )
                
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
            self._rowcount =  self._cursor.rowcount
            return
        
        self._rowcount = -1
    
    def _is_no_params(self, distilled):
        return distilled is None or distilled == [{}]
    
    def _resolve_parameters(
        self, 
        overrides: _CoreMultiExecuteParams
    ) -> Sequence[Mapping[str, BindParameter]]:
        """Resolve binding parameters using the values passed as normalized parameters in the constructor.

        Supports both single and multi-parameter execution.
        
        .. versionchanged: 0.9.0
            It uses :meth:`normlite.sql.compiler.NotionCompiler.construct_params` to resolve overrides.

        .. versionadded:: 0.8.0
        """
        
        from normlite.sql.elements import BindParameter

        param_sets: list[dict[str, Any]] = []

        # resolve values
        for override in overrides:
            resolved = self.compiled._compiler.construct_params(override)
            param_sets.append(resolved)

        # rebuild BindParameter objects
        bind_template = self.compiled._compiler_state.execution_binds
        bound_param_sets = []

        for resolved in param_sets:
            bound = {}

            for key, value in resolved.items():
                template = bind_template[key]

                bp = BindParameter(
                    key=key,
                    value=value,
                    type_=template.type_,
                )
                bp.role = template.role

                bound[key] = bp

            bound_param_sets.append(bound)

        return bound_param_sets

    def _build_overrides_sets(self) -> list[Mapping[str, Any]]:
        """Build the list of parameter dictionaries used for execution.

        Returns a list of dictionaries, one per execution (even for single execution).
        """
        stmt = self.compiled._compiler_state.stmt
        distilled = [] if self._is_no_params(self.distilled_params) else self.distilled_params

        # INSERT LOGIC
        if stmt.is_insert:

            # CASE 1: bulk via .values([...])
            if stmt._has_multi_parameters:
                if distilled and distilled[0]:
                    # bulk via .values([...]) and .execute(parameters=[...]) is not allowed
                    raise StatementError(
                        "Cannot combine execution parameters with bulk VALUES()"
                    )
                return stmt._multi_parameters

            # CASE 2: execution-time bulk: INSERT+UPDATE
            # .values() specifies values common to all rows
            # .execute(stmt, parameters=[...]) specifies single row values
            if len(distilled) > 1:
                if stmt._single_parameters:
                    return [
                        {**stmt._single_parameters, **params}
                        for params in distilled
                    ]
                return distilled

            # CASE 3: single execution
            if stmt._single_parameters:
                if distilled:
                    return [{**stmt._single_parameters, **distilled[0]}]
                return [stmt._single_parameters]

            # CASE 4: no statement values
            if distilled:
                return [distilled[0]]

            return [{}]

        # NON-INSERT LOGIC (DELETE / DDL / others)
        # These statements do not support VALUES semantics

        if len(distilled) > 1:
            return distilled

        if distilled:
            return [distilled[0]]

        return [{}]

    def _validate_insert_values(
        self,
        resolved_parameters: Union[
            dict[str, BindParameter],
            list[dict[str, BindParameter]]
        ]
    ):
        # normalize to list
        if isinstance(resolved_parameters, dict):
            param_sets = [resolved_parameters]
        else:
            param_sets = resolved_parameters

        for i, params in enumerate(param_sets):
            missing_cols = [
                key
                for key, bindparam in params.items()
                if bindparam.value is VALUE_PLACEHOLDER
            ]

            if missing_cols:
                table_name = self.compiled._compiler_state.stmt.get_table().name
                cols_word = "column" if len(missing_cols) == 1 else "columns"
                formatted = ", ".join(f"'{col}'" for col in missing_cols)
                group_info = f" in parameter set {i}" if len(param_sets) > 1 else ""

                raise StatementError(
                    f"Missing value for {cols_word} {missing_cols} {formatted} in INSERT into '{table_name}'"
                    f"{group_info}"
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
