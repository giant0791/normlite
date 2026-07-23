# sql/queryplan.py
# Copyright (C) 2026 Gianmarco Antonini
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
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

"""Provide abstractions for a query planner based on the Volcano iterator model."""

from typing import Any, Callable, Optional, Protocol, Sequence, Type, Union, runtime_checkable

from normlite.engine.context import ExecutionContext
from normlite.exceptions import InvalidRequestError
from normlite.notion_sdk.client import NotionError
from normlite.notiondbapi.dbapi2 import Connection, Cursor
from normlite.sql.compiler import compile_residual_filter, compile_residual_sorts
from normlite.sql.dml import Join, JoinExecution
from normlite.sql.elements import BinaryExpression
from normlite.sql.resultschema import ResultColumn, SchemaInfo
from normlite.sql.schema import Column, Table
from normlite.sql.type_api import type_mapper

#: Notion's maximum (and default) result page size. A ``Scan`` pulls the store
#: one Notion page at a time, so this is the operator's batch granularity.
NOTION_MAX_PAGE_SIZE = 100

@runtime_checkable
class VolcanoOperator(Protocol):
    """Base class for Volcano operators implementing the iterator model.
    
    .. versionadded:: 0.13.0
    """
    def open(self, connection: Connection) -> None:
        ...

    def next(self) -> Optional[list[tuple]]:
        ...

    def close(self) -> None:
        ...

    @property
    def result_schema(self) -> SchemaInfo:
        ...

class Scan(VolcanoOperator):
    def __init__(
        self, 
        operation: dict, 
        parameters: Union[dict, list[dict]],
        schema: SchemaInfo,
    ) -> None:
        self._operation = operation
        self._parameters = parameters
        self._schema = schema
        self._cursor = None

    def open(self, connection: Connection) -> None:
        self._cursor = connection.cursor()
        self._cursor._inject_description(self._schema.as_sequence())
        self._cursor.execute(
            self._operation,
            self._parameters, 
            stream_results=True,
        )

    def next(self) -> Optional[list[tuple]]:
        next_batch = self._cursor.fetchmany(size=NOTION_MAX_PAGE_SIZE)            
        return next_batch if next_batch else None
    
    def close(self) -> None:
        self._cursor.close()

    @property
    def result_schema(self) -> SchemaInfo:
        return self._schema
    
class Retrieve(Scan):
    """Retrieve joining rows by id with lax semantics.
    
    Silences `object_not_found` from `pages.retrieve` (ADR-0002 lax-FK semantics:
    a dangling relation entry is an absent reference, not an error). All other
    errors propagate via the default DBAPI raise path.
    """

    def __init__(
        self, 
        parameters: list[dict], 
        schema: SchemaInfo
    ):
        super().__init__(
            {"endpoint": "pages", "request": "retrieve"}, 
            parameters, 
            schema
        )

    def open(self, connection: Connection) -> None:
        self._cursor = connection.cursor()
        self._cursor._inject_description(self._schema.as_sequence())
        self._cursor.errorhandler = self._lax_retrieve_errorhandler

    def execute_with(self, batch: list[dict]) -> None:
        self._parameters = batch
        self._cursor.executemany(
            self._operation,
            self._parameters
        )

    def next(self) -> Optional[list[tuple]]:
        if self._parameters is None:
            raise InvalidRequestError(
                "No parameters provided: Retrieve on the right side needs the ids to fetch the rows."
            )
        
        # drain across result sets (one per page)
        next_batch = [
            row 
            for row in self._cursor._iter_all()
        ]
        return next_batch if next_batch else None

    def _lax_retrieve_errorhandler(
        self, 
        connection: Connection, 
        cursor: Cursor, 
        errorclass: Type[BaseException],
        errorvalue: BaseException) -> None:
        """Errorhandler for lax semantics.

        Silences `object_not_found` from `pages.retrieve` (ADR-0002 lax-FK semantics:
        a dangling relation entry is an absent reference, not an error). All other
        errors propagate via the default DBAPI raise path.
        """
        cause = errorvalue.__cause__
        if isinstance(cause, NotionError) and cause.code == "object_not_found":
            # Dangling-FK / lax-reference semantics (ADR-0002):
            # missing pages are treated as absent references
            # INNER JOIN silently drops left rows whose relation list resolves 
            # to zero existing right rows
            return                       

        raise errorvalue                 # propagate everything else

class HashJoin(VolcanoOperator):
    def __init__(
        self,
        left_child: VolcanoOperator,
        right_child: VolcanoOperator,
        join: Join,
        projection: list[Column],
        right_filter: Optional[dict] = None,
        right_sorts: Optional[dict] = None
    ) -> None:
        self._left_child = left_child
        self._right_child = right_child
        self._join_exec = JoinExecution(
            join,
            projection,
            right_filter,
            right_sorts
        )
        self._result_schema: Optional[SchemaInfo] = None

    @property
    def result_schema(self) -> Optional[SchemaInfo]:
        return self._result_schema

    def open(self, connection: Connection) -> None:
        self._left_child.open(connection)
        self._right_child.open(connection)
        
    def next(self) -> Optional[list[tuple]]:
        # drain the left child fully to get all left side pages
        left_rows = self._left_child.next()
        while left_rows is not None:
            next_rows = self._left_child.next()
            if next_rows is None:
                break
            left_rows.extend(next_rows)

        if left_rows is None:
            return None

        retrieve_batch = self._join_exec.prepare(left_rows)

        # drain the right child fully to get all right side pages
        right_rows = self._right_child.next()
        while right_rows is not None:
            next_rows = self._right_child.next()
            if next_rows is None:
                break
            right_rows.extend(next_rows)

        # an outer join with an all-dangling / empty right must still keep its left rows None-filled
        right_rows = right_rows or []

        self._result_schema, merged_rows = self._join_exec.assemble(right_rows)
        return merged_rows
    
    def close(self) -> None:
        self._left_child.close()
        self._right_child.close()

class Filter(VolcanoOperator):
    def __init__(
        self,
        source: VolcanoOperator,
        schema: SchemaInfo,
        filter: dict[str, Any],
        table: Table,
    ) -> None:
        self._source = source
        self._merged_schema = schema
        self._filter = filter
        self._table = table

        # right-side WHERE is answered client-side, AFTER the join
        # (ADR-0005): build getters from the merged schema and keep only
        # rows whose right slice passes the predicate.
        # Select the right-side result columns by IDENTITY (provenance),
        # not by name: under a collision the right column is keyed
        # fully-qualified (`courses.title`) and would never match
        # `c.name in right.uc`. The getter is taken at the merged
        # (qualified) name via the existing index. See ADR-0009.
        # 
        # IMPORTANT:
        # identity-by-table is collision-proof but **not self-join-proof**
        self._right_cols = [c for c in self._merged_schema.columns if c.table is self._table]
        self._right_getters = [
            self._merged_schema.column_getter(c.name) 
            for c in self._right_cols
        ]

    @property
    def result_schema(self) -> SchemaInfo:
        return self._merged_schema

    def open(self, connection: Connection) -> None:
        self._source.open(connection)

    def next(self) -> Optional[list[tuple]]:
        merged_rows = self._source.next()
        if merged_rows is None:
            return None
        
        merged_rows = [
            r for r in merged_rows
            if self._right_side_passes(r, self._right_getters, self._right_cols)
        ]

        return merged_rows
    
    def close(self) -> None:
        self._source.close()

    def _right_side_passes(
        self,
        merged_row: tuple[Any, ...],
        row_getters: list[Callable[[Sequence[Any]], Any]],
        right_cols: Sequence[ResultColumn],
    ) -> bool:
        """Shape adapter applying the ``_Filter`` predicate to a merged row's
        right slice.

        NOTE: verbatim strangler-duplicate of ``JoinExecution._right_side_passes``
        (``sql/dml.py``). Both copies exist only until step 4 of #363 cuts the old
        ``join_right_filter`` channel; edit BOTH until then. See #363.
        """

        from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
        from normlite.notion_sdk.client import _Filter

        right_slice = tuple(getter(merged_row) for getter in row_getters)

        if all(c is None for c in right_slice):
            return False        # phantom: NULL fails every right-side predicate

        # Key the synthetic page by the BARE name: the compiled Notion filter
        # references the unqualified property (`title`, emitted by
        # visit_binary_expression), and the page is right-only so bare names
        # are unambiguous within it. See ADR-0009.
        properties = {}
        for col, cell in zip(right_cols, right_slice):
            typ = type_mapper[col.type_code].get_col_spec()
            properties[col.bare_name] = {"type": typ, **cell}

        page = {"properties": properties}
        return _Filter(page, {"filter": self._filter}).eval()
    
class Sort(VolcanoOperator):
    def __init__(        
        self,
        source: VolcanoOperator,
        schema: SchemaInfo,
        sorts: list[dict],
        table: Table,
    ) -> None:
        self._source = source
        self._merged_schema = schema
        self._sorts = sorts
        self._table = table

    @property
    def result_schema(self) -> SchemaInfo:
        return self._merged_schema

    def open(self, connection: Connection) -> None:
        self._source.open(connection)

    def next(self) -> Optional[list[tuple]]:
        merged_rows = self._source.next()
        if merged_rows is None:
            return None

        from normlite.sql.type_api import type_mapper
        from normlite.notion_sdk.client import EMPTY_TEXT, EMPTY_NUMBER

        right_cols = [c for c in self._merged_schema.columns if c.table is self._table]
        by_bare = {c.bare_name: c for c in right_cols}
        merged_rows = list(merged_rows)
        for sort in reversed(self._sorts):
            # identity, keyed by the sort's own property
            col = by_bare[sort["property"]]

            # merged name — survives collision
            getter = self._merged_schema.column_getter(col.name)
            direction = sort.get("direction", "ascending")
            reverse = direction == "descending"

            def sort_key(
                row: tuple[dict], 
                col: ResultColumn = col, 
                getter: Callable[[Sequence[Any]], Any] = getter
            ) -> tuple[bool, Any]:
                value = type_mapper[col.type_code].result_processor()(getter(row))

                # Empties-first/last sentinel, inherited from
                # _extract_sort_value. For a right-side TITLE key this branch
                # is currently UNREACHABLE: an empty/None right title is
                # unconstructable through the public interface (insert
                # title=None is rejected by the client; title="" yields a
                # non-empty list). Kept for parity with the shared sort-value
                # semantics and for nullable types if they become sortable
                # right-side keys. See ADR-0005 / the unreachable-empty-title
                # boundary; not covered by a test because the input can't be
                # built.
                is_empty = value in (None, EMPTY_TEXT, EMPTY_NUMBER)
                return (is_empty, value)

            merged_rows.sort(key=sort_key, reverse=reverse)
        return merged_rows

class Planner:
    """Provide a query plan as a pipeline composed of Volcano operators."""

    _exec_ctx: ExecutionContext

    def __init__(self, ctx: ExecutionContext) -> None:
        self._exec_ctx = ctx

    def plan(self) -> VolcanoOperator:
        invoked_stmt = self._exec_ctx.invoked_stmt
        ctx: ExecutionContext = self._exec_ctx
        if not invoked_stmt.is_select:
            raise InvalidRequestError(
                f"Query planner build plans for SELECT statements only. "
                f"The invoked statement is not a SELECT ({type(invoked_stmt).__name__})"
            )

        if not invoked_stmt._joins:
            # SELECT statement without JOIN
            schema = SchemaInfo.from_table(
                invoked_stmt.get_table(),
                execution_names=ctx.compiled.fetch_columns(),
                projected_names=ctx.compiled.result_columns(),
            )
            return Scan(ctx.operation, ctx.parameters, schema=schema)
        
        # build the plan for JOIN
        join: Join = invoked_stmt._joins[0]
        projection = list(invoked_stmt._projection)
        left_schema, right_schema = SchemaInfo.from_join_sides(
            join.left,
            join.right,
            projection,
            join.onclause
        )
        left_child = Scan(ctx.operation, ctx.parameters, schema=left_schema)
        right_child = Scan(
            ctx.operation,
            parameters={"path_params": {"data_source_id": join.right.get_data_source_id()}},
            schema=right_schema,
        )
        plan = HashJoin(
            left_child,
            right_child,
            join,
            projection
        )
    
        # add a filter on top of the plan, if there is a WHERE-clause on the right table
        residual_where = ctx.compiled.planning_context.residual_where
        if residual_where is not None:
            if not isinstance(residual_where, BinaryExpression):
                raise InvalidRequestError(
                    f"Only single-binary expressions supported, "
                    f"received a '{type(residual_where).__name__}' expression."
                )
            right_filter = compile_residual_filter(residual_where)
            merged_schema = SchemaInfo.from_join(
                join.left,
                join.right,
                *invoked_stmt._projection,
            )
            updated = Filter(
                source=plan,
                schema=merged_schema,
                filter=right_filter,
                table=join.right
            )

            plan = updated

        # add a sort on top of the plan, if there is a ORDER BY-clause on the right table
        residual_sorts = ctx.compiled.planning_context.residual_sorts
        if residual_sorts is not None:
            sorts = compile_residual_sorts(residual_sorts)
            merged_schema = SchemaInfo.from_join(join.left, join.right, *invoked_stmt._projection)
            plan = Sort(source=plan, schema=merged_schema, sorts=sorts, table=join.right)
        
        return plan
 
        