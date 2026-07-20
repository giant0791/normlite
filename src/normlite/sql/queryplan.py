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

from typing import Any, Callable, Optional, Protocol, Sequence, Union, runtime_checkable

from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.exceptions import CompileError, InvalidRequestError
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.compiler import compile_residual_filter
from normlite.sql.dml import Join, JoinExecution
from normlite.sql.elements import BindParameter, ColumnElement, Operator, BinaryExpression
from normlite.sql.resultschema import ResultColumn, SchemaInfo
from normlite.sql.schema import Column, Table

#: Notion's maximum (and default) result page size. A ``Scan`` pulls the store
#: one Notion page at a time, so this is the operator's batch granularity.
NOTION_MAX_PAGE_SIZE = 100

@runtime_checkable
class VolcanoOperator(Protocol):
    """Base class for Volcano operators implementing the iterator model.
    
    .. versionadded:: 0.13.0
    """
    def open(self, cursor: Cursor) -> None:
        ...

    def next(self) -> Optional[list[tuple]]:
        ...

    def close(self) -> None:
        ...

class Scan(VolcanoOperator):
    def __init__(
        self, 
        operation: dict, 
        parameters: Union[dict, list[dict]],
        style: ExecutionStyle = ExecutionStyle.EXECUTE    
    ) -> None:
        self._operation = operation
        self._parameters = parameters
        self._style = style
        self._cursor = None

    def open(self, cursor: Cursor) -> None:
        self._cursor = cursor
        if self._style is ExecutionStyle.EXECUTE:
            # left leaf: execute eagerly
            self._cursor.execute(
                self._operation,
                self._parameters, 
                stream_results=True,
            )
        # right leaf: delay execution until 
        # the schema-aware caller drives .execute_with

    def next(self) -> Optional[list[tuple]]:
        if self._style is ExecutionStyle.EXECUTEMANY:
            # drain across result sets (one per page)
            next_batch = [
                row 
                for row in self._cursor._iter_all()
            ]
        else:
            # return paginated next batch
            next_batch = self._cursor.fetchmany(size=NOTION_MAX_PAGE_SIZE)            

        return next_batch if next_batch else None
    
    def close(self) -> None:
        self._cursor.close()

    def execute_with(self, batch: list[dict]) -> None:
        self._parameters = batch
        self._cursor.executemany(
            self._operation,
            self._parameters
        )

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

    def open(self, cursor: Cursor) -> None:
        self._left_child.open(cursor)
        
        right_cursor = None
        if cursor is not None:
            right_cursor = cursor._connection.cursor()
            right_cursor._inject_description(
                self._join_exec.right_schema.as_sequence()
            )

        self._right_child.open(right_cursor)

    def next(self) -> Optional[list[tuple]]:
        left_rows = self._left_child.next()
        if left_rows is None:
            return None
        
        retrieve_batch = self._join_exec.prepare(left_rows)
        self._right_child.execute_with(retrieve_batch)
        right_rows = self._right_child.next()
        if right_rows is None:
            return None
        
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

    def open(self, cursor: Cursor) -> None:
        self._source.open(cursor)

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

        type_mapper = {
            DBAPITypeCode.NUMBER: "number",
            DBAPITypeCode.NUMBER_WITH_COMMAS: "number",
            DBAPITypeCode.NUMBER_DOLLAR: "number",
            DBAPITypeCode.TITLE: "title",
            DBAPITypeCode.RICH_TEXT: "rich_text",
            DBAPITypeCode.CHECKBOX: "checkbox",
            DBAPITypeCode.DATE: "date",
            DBAPITypeCode.RELATION: "relation",
        }

        right_slice = tuple(getter(merged_row) for getter in row_getters)

        if all(c is None for c in right_slice):
            return False        # phantom: NULL fails every right-side predicate

        # Key the synthetic page by the BARE name: the compiled Notion filter
        # references the unqualified property (`title`, emitted by
        # visit_binary_expression), and the page is right-only so bare names
        # are unambiguous within it. See ADR-0009.
        properties = {}
        for col, cell in zip(right_cols, right_slice):
            typ = type_mapper[col.type_code]
            properties[col.bare_name] = {"type": typ, **cell}

        page = {"properties": properties}
        return _Filter(page, {"filter": self._filter}).eval()

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
            return Scan(ctx.operation, ctx.parameters)
        
        # build the plan for the join
        join: Join = invoked_stmt._joins[0]
        projection = list(invoked_stmt._projection)
        left_child = Scan(ctx.operation, ctx.parameters)
        right_child = Scan(
            operation={"endpoint": "pages", "request": "retrieve"}, 
            parameters=None,
            style=ExecutionStyle.EXECUTEMANY
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

        return plan
 
        