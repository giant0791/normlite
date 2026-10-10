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

"""Provide abstractions for a query planner based on the Volcano iterator model.

A plan is a tree of operators, each implementing :class:`VolcanoOperator` and
pulled one batch of rows at a time: :class:`Scan` fetches, :class:`HashJoin`
merges, :class:`Filter` re-checks the WHERE, :class:`Sort` orders,
:class:`Aggregate` reduces and :class:`Project` trims. :class:`Planner` builds
the tree from the compiled statement.

.. versionadded:: 0.13.0
"""

from typing import Any, Callable, Optional, Protocol, Sequence, Union, runtime_checkable

from normlite._constants import SpecialColumns
from normlite.engine.context import ExecutionContext
from normlite.exceptions import InvalidRequestError
from normlite.notiondbapi.dbapi2 import Connection
from normlite.sql.compiler import _get_expression_parent_tables, compile_residual_sorts, _get_expression_columns
from normlite.sql.dml import AggregateExecution, Join
from normlite.sql.elements import ColumnElement
from normlite.sql.functions import FunctionElement
from normlite.sql.resultschema import ResultColumn, SchemaInfo
from normlite.sql.schema import Column, Table
from normlite.sql.type_api import String

#: Notion's maximum (and default) result page size. A ``Scan`` pulls the store
#: one Notion page at a time, so this is the operator's batch granularity.
NOTION_MAX_PAGE_SIZE = 100

@runtime_checkable
class VolcanoOperator(Protocol):
    """Base class for Volcano operators implementing the iterator model.
    
    .. versionadded:: 0.13.0
    """
    def open(self, connection: Connection) -> None:
        """Acquire this operator's resources, and those of its children."""
        ...

    def next(self) -> Optional[list[tuple]]:
        """Return the next batch of rows, or ``None`` when the source is drained."""
        ...

    def close(self) -> None:
        """Release this operator's resources, and those of its children."""
        ...

    @property
    def result_schema(self) -> SchemaInfo:
        """Provide the schema of the rows this operator emits."""
        ...

class Scan(VolcanoOperator):
    """Fetch rows from a Notion data source, one result page at a time.

    This is the only leaf a plan has: it opens its own cursor, executes the
    compiled operation and hands the rows above it in batches of at most
    :data:`NOTION_MAX_PAGE_SIZE`. Rows leave this operator RAW: decoding is
    :class:`normlite.notiondbapi.resultset.ResultSet`'s job, and a
    :class:`Filter` above needs the Notion type tag still on the cell.

    .. versionadded:: 0.13.0
    """

    def __init__(
        self, 
        operation: dict, 
        parameters: Union[dict, list[dict]],
        schema: SchemaInfo,
        *,
        yield_per: Optional[int] = None
    ) -> None:
        """Construct a scan over an already compiled operation.

        Args:
            operation (dict): The compiled Notion API operation to execute.
            parameters (Union[dict, list[dict]]): The DBAPI parameters bound to it.
            schema (SchemaInfo): Describes the rows this scan emits.
            yield_per (Optional[int]): Caps the batch size, so the operator above
                can consume batch by batch. ``None`` yields full Notion pages.
        """
        self._operation = operation
        self._parameters = parameters
        self._schema = schema
        self._cursor = None
        self._yield_per = yield_per
        self._page_size = min(yield_per, NOTION_MAX_PAGE_SIZE) if yield_per else NOTION_MAX_PAGE_SIZE

    def open(self, connection: Connection) -> None:
        """Open a cursor on ``connection`` and execute the operation on it."""
        self._cursor = connection.cursor()
        self._cursor._inject_description(self._schema.as_sequence())
        self._cursor.execute(
            self._operation,
            self._parameters, 
            stream_results=True,
            yield_per=self._yield_per
        )

    def next(self) -> Optional[list[tuple]]:
        """Return the next batch of raw rows, or ``None`` once the store is drained."""
        next_batch = self._cursor.fetchmany(size=self._page_size)            
        return next_batch if next_batch else None
    
    def close(self) -> None:
        """Close the cursor opened by :meth:`open`."""
        self._cursor.close()

    @property
    def result_schema(self) -> SchemaInfo:
        """Provide the schema this scan was constructed with."""
        return self._schema
    
class HashJoin(VolcanoOperator):
    """Merge two scans on the left table's relation column.

    Both children are drained on the first :meth:`next` call and the right rows
    are hashed by ``object_id``, so ``yield_per`` cannot cascade through this
    operator. Only the left child carries a Notion-side filter; the right child
    is a full data-source scan (ADR-0021).

    .. versionadded:: 0.13.0
    """

    def __init__(
        self,
        left_child: VolcanoOperator,
        right_child: VolcanoOperator,
        join: Join,
        projection: list[Column],
    ) -> None:
        """Construct a join over two already built child operators.

        Args:
            left_child (VolcanoOperator): Supplies the left (driving) rows.
            right_child (VolcanoOperator): Supplies the right rows, scanned in full.
            join (Join): The join to execute; carries the onclause and ``isouter``.
            projection (list[Column]): The columns to emit, in projection order.
                ``None`` emits every user column of both sides, left then right,
                skipping ``object_id``.
        """
        self._left_child = left_child
        self._right_child = right_child
        self._join = join
        self._projection = projection
        self._result_schema: Optional[SchemaInfo] = SchemaInfo.from_join(
            self._join.left,
            self._join.right,
            *projection
        )
        self._left_schema, self._right_schema = SchemaInfo.from_join_sides(
            self._join.left,
            self._join.right,
            self._projection,
            self._join.onclause
        )


    @property
    def result_schema(self) -> Optional[SchemaInfo]:
        """Provide the merged schema of both join sides."""
        return self._result_schema

    def open(self, connection: Connection) -> None:
        """Open both children on ``connection``."""
        self._left_child.open(connection)
        self._right_child.open(connection)
        
    def next(self) -> Optional[list[tuple]]:
        """Drain both children and return every merged row in one batch.

        Returns:
            Optional[list[tuple]]: The merged rows, or ``None`` if the left side
            is empty -- in which case no join kind, inner or outer, has a row.
        """
        # drain the left child fully to get all left side pages
        left_rows = self._left_child.next()
        while left_rows is not None:
            next_rows = self._left_child.next()
            if next_rows is None:
                break
            left_rows.extend(next_rows)

        if left_rows is None:
            return None

        # drain the right child fully to get all right side pages
        right_rows = self._right_child.next()
        while right_rows is not None:
            next_rows = self._right_child.next()
            if next_rows is None:
                break
            right_rows.extend(next_rows)

        # an outer join with an all-dangling / empty right must still keep its left rows None-filled
        right_rows = right_rows or []

        return self._merge_rows(left_rows, right_rows)
    
    def _merge_rows(self, left_rows: list[tuple], right_rows: list[tuple]) -> list[tuple]:
        """Cross-product the captured left rows with the right rows whose
        object_id matches the decoded relation list on each left row.

        Inner vs outer differ in exactly ONE place: what to do with a left row
        that matched zero right rows. Inner drops it; outer rescues it with a
        single None-filled row. Every left row that matched at least once is
        treated identically by both kinds, so ``isouter`` is consulted at one
        site only -- the per-row fallback below.

        INVARIANT (per left row, not across the batch): a given left row
        contributes exactly one None-filled row iff THAT row's foreign keys
        matched zero right rows. This single predicate subsumes all three
        zero-match shapes -- empty relation, all-dangling ids, and a lone
        dangling id -- so none of them needs its own branch.

        Args:
            left_rows (list[tuple]): The fully drained left side.
            right_rows (list[tuple]): The fully drained right side.

        Returns:
            list[tuple]: The merged rows, in left-row order.
        """
        onclause = self._join.onclause
        isouter = self._join.isouter

        merged_rows = []

        # prepare the getters
        relation_proc = onclause.type_.result_processor()
        get_oids = self._left_schema.column_getter(onclause.name)
        get_left_oid = self._left_schema.column_getter("object_id")
        get_right_oid = self._right_schema.column_getter("object_id")

        # {object_id: right_row} answers "does THIS oid resolve?" in O(1). It
        # does NOT answer "did this left row match anything?" -- that is a
        # per-row aggregate (see `matched`) known only after the oid loop.
        right_by_oid = {get_right_oid(rr): rr for rr in right_rows}

        for left_row in left_rows:
            fk_oids = relation_proc(get_oids(left_row)) or []

            # `matched` is reset per left row: it tracks whether THIS row found
            # any partner. Truthiness is all we read.
            matched = []
            for fk_oid in fk_oids:
                right_row = right_by_oid.get(fk_oid)
                if right_row is None:
                    # dangling id: contributes no row of its own. Whether the
                    # row gets rescued is decided once, after the loop -- a
                    # later oid in this same row may still match.
                    continue

                merged_rows.append(self._project_join_row(left_row, right_row))
                matched.append(get_left_oid(left_row))

            # The ONLY place inner and outer diverge. Under inner join a
            # zero-match row is simply dropped; without this guard None-fill
            # would leak into inner join and break its drop-the-unmatched
            # contract.
            if not matched and isouter:
                merged_rows.append(self._project_join_row(left_row, None))

        return merged_rows

    def _project_join_row(
        self,
        left_row: tuple,
        right_row: Optional[tuple],
    ) -> tuple:
        """Construct the merged row from a left row and an optional right row.

        Args:
            left_row (tuple): The left row being merged.
            right_row (Optional[tuple]): Its right partner, or ``None`` for an
                outer-join phantom, whose right-owned columns are None-filled.

        Returns:
            tuple: One merged row, in projection order.
        """
        projection = self._projection

        if projection is not None:
            # Project in PROJECTION ORDER, exactly one value per projected
            # column, sourced from the column's OWNING table. Ownership is
            # decided by identity (col.parent is left), NOT by name membership:
            # under a name collision both schemas contain the name, so name
            # membership is ambiguous.
            left_table = self._join.onclause.parent
            projected = tuple()
            for col in projection:
                if col.parent is left_table:
                    getter = self._left_schema.column_getter(col.name)
                    projected += (getter(left_row),)
                elif right_row is not None:
                    getter = self._right_schema.column_getter(col.name)
                    projected += (getter(right_row),)
                else:
                    # right-owned column with no right row (outer-join phantom)
                    projected += (None,)

            return projected

        # No projection: project ALL user columns from both sides (left then
        # right), skipping object_id. Right side is None-filled when there is no
        # matching right row.
        left_projected = tuple([
            left_row[self._left_schema.column_index(lc.name)]
            for lc in self._left_schema.columns
            if lc.name != "object_id"
        ])

        if right_row is not None:
            right_projected = tuple([
                right_row[self._right_schema.column_index(rc.name)]
                for rc in self._right_schema.columns
                if rc.name != "object_id"
            ])
        else:
            right_projected = tuple([
                None
                for rc in self._right_schema.columns
                if rc.name != "object_id"
            ])

        return (*left_projected, *right_projected)

    def close(self) -> None:
        """Close both children."""
        self._left_child.close()
        self._right_child.close()

class Filter(VolcanoOperator):
    """Answer the WHERE client-side over raw cells, and decide the row.

    A pushed Notion filter is a lossy probe: it may keep rows SQL drops, so it
    is a hint and never the answer. Every conjunct the compiler pushed is
    therefore re-applied here, and it is this evaluation that decides
    (ADR-0022). A conjunct with no pushed form at all reaches the same place as
    a genuine residual.

    The predicate is evaluated over the slice of each row owned by the tables it
    reads: the whole row on the scan path, either or both sides on the join path.

    .. versionadded:: 0.13.0
    """

    def __init__(
        self,
        source: VolcanoOperator,
        schema: SchemaInfo,
        filter: ColumnElement,
        tables: list[Table],
    ) -> None:
        """Construct a filter over an already built source operator.

        Args:
            source (VolcanoOperator): Supplies the rows to filter.
            schema (SchemaInfo): The schema of those rows.
            filter (ColumnElement): The WHERE predicate, held as AST.
            tables (list[Table]): The tables the predicate reads. Tested for
                membership only -- the order is arbitrary and nothing may
                depend on it.
        """
        self._source = source
        self._merged_schema = schema
        self._filter = filter
        self._predicate_tables: list[Table] = tables

        # The WHERE is answered client-side over the slice of the row owned by
        # `tables` -- the tables the PREDICATE reads. That is NOT always the
        # join's right side any more (ADR-0022): on the scan path it is the
        # statement's only table, and on the join path it is whichever side(s)
        # the conjunct READS -- one for a single-sided conjunct, BOTH for a
        # compound spanning the join -- derived from the predicate in
        # `Planner.plan`.
        # A conjunct reading the LEFT table was evaluated against the RIGHT
        # slice while this argument was hardcoded, which found no cell, answered
        # UNKNOWN for every row, and returned nothing -- silently (#384 / C2).
        #
        # Select the slice's result columns by IDENTITY (provenance), not by
        # name: under a collision the column is keyed fully-qualified
        # (`courses.title`) and would never match a bare-name test. The getter
        # is taken at the merged (qualified) name via the existing index.
        # See ADR-0009.
        # 
        # IMPORTANT:
        # identity-by-table is collision-proof but **not self-join-proof**
        #
        # `is`, not `in`: `in` compares with __eq__, and Table defines none, so a
        # list test is identity only BY ACCIDENT. Say it literally, so that adding
        # the obvious `Table.__eq__` (by name) cannot silently broaden this slice
        # under a self-join -- no exception, just wrong rows.
        self._predicate_cols = [
            c for c in self._merged_schema.columns
            if any(c.table is t for t in self._predicate_tables)
        ]
        self._predicate_getters = [
            self._merged_schema.column_getter(c.name) 
            for c in self._predicate_cols
        ]

    @property
    def result_schema(self) -> SchemaInfo:
        """Provide the schema of the rows this filter was given.

        Filtering removes rows, never columns, so the schema passes through
        unchanged.
        """
        return self._merged_schema

    def open(self, connection: Connection) -> None:
        """Open the source operator on ``connection``."""
        self._source.open(connection)

    def next(self) -> Optional[list[tuple]]:
        """Return the next batch of the source, keeping only the rows that pass.

        One batch in, one batch out: the source is never drained here, which is
        what keeps a streaming ``SELECT`` lazy across a WHERE clause (ADR-0010).
        A batch may come back empty when every row of it is dropped.
        """
        merged_rows = self._source.next()
        if merged_rows is None:
            return None
        
        merged_rows = [
            r for r in merged_rows
            if self._predicate_passes(r, self._predicate_getters, self._predicate_cols)
        ]

        return merged_rows
    
    def close(self) -> None:
        """Close the source operator."""
        self._source.close()

    def _predicate_passes(
        self,
        merged_row: tuple[Any, ...],
        row_getters: list[Callable[[Sequence[Any]], Any]],
        predicate_cols: Sequence[ResultColumn],
    ) -> bool:
        """Answer the WHERE over the slice of a row owned by the predicate's tables.

        On the join path that slice may be either side or both; on the scan path it is
        the whole row. The WHERE reaching here is usually a **recheck** -- a
        conjunct that was also pushed, re-applied because a Notion filter is a
        lossy probe and never decides (ADR-0022) -- and sometimes a genuine
        **residual**, a conjunct with no pushed form at all.

        Shape adapter around :func:`eval3`: the merged row is a flat tuple of
        raw cells, while the evaluator reads them keyed by column name. The
        cells stay RAW because decoding erases the Notion TYPE TAG, and
        emptiness is per-type -- a number is empty when it is ``null``, a text
        when its plain text is ``""``, a relation when it holds no items.
        Picking the right rule needs the type, ``eval3`` dispatches on
        ``"<col_spec>.<op>"``, and ``<col_spec>`` *is* the raw cell's key --
        a decoded value cannot supply it, so pushdown parity for ``is_empty``
        would break (ADR-0019, as amended by its 2026-07-27 Correction).

        ``eval3`` returns a Ternary; the ``is TRUE`` here is the WHERE policy,
        which drops UNKNOWN along with FALSE. A ``CheckConstraint`` over the
        same logic applies the opposite policy (reject only on FALSE), which is
        why the evaluator never returns a bool and each caller narrows it.

        This is the sole implementation: the former strangler-duplicate on
        ``JoinExecution`` (``sql/dml.py``) was deleted with that class once the
        merge folded into ``HashJoin`` (#378 / ADR-0021).

        Args:
            merged_row (tuple[Any, ...]): One row, as the source emitted it.
            row_getters (list[Callable[[Sequence[Any]], Any]]): Getters for the
                predicate's cells, in ``predicate_cols`` order.
            predicate_cols (Sequence[ResultColumn]): The result columns the
                predicate reads, selected by provenance.

        Returns:
            bool: ``True`` only where the predicate evaluates to ``TRUE``.

        .. note::
            The page map is keyed by :attr:`normlite.sql.resultschema.ResultColumn.table` by identity, 
            so a self-join collapses both pages onto one key.

        .. versionadded:: 0.13.0
        """

        from normlite.sql.eval3 import eval3, TRUE

        predicate_slice = tuple(getter(merged_row) for getter in row_getters)

        # One properties object PER TABLE. Each inner dict is a genuine single-page
        # properties object keyed by bare_name -- the property name Notion itself uses
        # (ADR-0009, d37baae). A predicate spanning both join sides has no single page
        # to evaluate against, so it gets both, addressed by provenance.
        pages: dict[Table, dict] = {}
        for col, cell in zip(predicate_cols, predicate_slice):
            pages.setdefault(col.table, {})[col.bare_name] = cell

        return eval3(self._filter, pages, schema=None) is TRUE
    
class Sort(VolcanoOperator):
    """Apply a held-back ORDER BY client-side.

    A sort key owned by the join's right table has no pushed form, so it is a
    genuine residual: applied once, here, over the merged rows. Keys are applied
    least-significant first, so the first key dominates. Empty values sort last
    ascending and first descending, which is the order Postgres gives NULLs.

    .. versionadded:: 0.13.0
    """

    def __init__(        
        self,
        source: VolcanoOperator,
        schema: SchemaInfo,
        sorts: list[dict],
        table: Table,
    ) -> None:
        """Construct a sort over an already built source operator.

        Args:
            source (VolcanoOperator): Supplies the rows to sort.
            schema (SchemaInfo): The merged schema of those rows.
            sorts (list[dict]): Compiled Notion sort objects, most significant first.
            table (Table): The table owning the sort keys. Each ``property`` is
                resolved to its result column within this table, by identity.
        """
        self._source = source
        self._merged_schema = schema
        self._sorts = sorts
        self._table = table

    @property
    def result_schema(self) -> SchemaInfo:
        """Provide the schema of the rows this sort was given, unchanged."""
        return self._merged_schema

    def open(self, connection: Connection) -> None:
        """Open the source operator on ``connection``."""
        self._source.open(connection)

    def next(self) -> Optional[list[tuple]]:
        """Return the next batch of the source, ordered by the residual keys.

        Ordering is per batch, so this operator is only correct above a source
        that returns its whole result in one batch -- :class:`HashJoin` does.
        """
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

class Aggregate(VolcanoOperator):
    """Aggregate columns value over all rows in the table.

    The source is drained on the first :meth:`next` call, so ``yield_per``
    cannot cascade through this operator. The reduction is client-side, over
    the fetched pages -- Notion computes no aggregate for us (ADR-0011).

    .. versionadded:: 0.13.0
    """

    def __init__(self, source: VolcanoOperator, raw_cols: tuple[FunctionElement]) -> None:
        """Construct an aggregate over an already built source operator.

        Args:
            source (VolcanoOperator): Supplies the rows to reduce.
            raw_cols (tuple[FunctionElement]): The aggregate functions the
                statement selects, in projection order.
        """
        self._source = source
        self._agg = AggregateExecution(raw_cols)
        self._done = False

    @property
    def result_schema(self) -> SchemaInfo:
        """Provide the schema of the synthetic row, one column per function."""
        return self._agg.result_schema

    def open(self, connection: Connection):
        """Open the source operator on ``connection`` and arm the reduction."""
        self._source.open(connection)
        self._done = False

    def next(self) -> Optional[list[tuple]]:
        """Drain the source and return the single synthetic row, once.

        Every later call returns ``None``: an aggregate over no rows still has
        an answer, so the batch cannot signal exhaustion by being empty.
        """
        if self._done:
            return None

        self._done = True
        rows = []
        while (batch := self._source.next()) is not None:
            rows.extend(batch)

        _, synthetic_row = self._agg.reduce(rows)
        return synthetic_row

    def close(self) -> None:
        """Close the source operator."""
        self._source.close()

class Project(VolcanoOperator):
    """Trim each row to the columns the statement projects, and own that schema.

    The recheck must read columns the user did not select, so :class:`Planner`
    widens the :class:`Scan` to fetch them. This operator trims them back off,
    which is what keeps the widening invisible to the user's ``Row``
    (ADR-0022).

    .. versionadded:: 0.13.0
    """

    def __init__(self, source: VolcanoOperator, names: Sequence[str]) -> None:
        """Construct a projection over an already built source operator.

        Args:
            source (VolcanoOperator): Supplies the rows to trim.
            names (Sequence[str]): The result keys to keep, in output order.
                Pass the PRE-widening ``fetch_columns``, never
                ``result_columns()`` -- the latter drops the special columns
                the row still needs (ADR-0006).

        Raises:
            NoSuchColumnError: If a name is absent from the source's schema.
            InvalidRequestError: If the source's schema carries no columns.
        """
        self._source = source
        self._names = list(names)
        project_idxs = [
            source.result_schema.column_index(name)
            for name in self._names
        ]

        self._projected_schema = SchemaInfo([
            source.result_schema.columns[idx]
            for idx in project_idxs
        ])

        self._getters = [
            source.result_schema.column_getter(name)
            for name in self._names
        ]

    @property
    def result_schema(self) -> SchemaInfo:
        """Provide the trimmed schema, which is the one the user sees.

        On the plan path the user-visible ``keys()`` come from the plan root, so
        this is the schema that decides them.
        """
        return self._projected_schema

    def open(self, connection: Connection) -> None:
        """Open the source operator on ``connection``."""
        self._source.open(connection)

    def next(self) -> Optional[list[tuple]]:
        """Return the next batch of the source, each row trimmed to the projection."""
        batch = self._source.next()
        if batch is None:
            return None

        return [self._trim(row) for row in batch]

    def _trim(self, row: tuple) -> tuple:
        """Return ``row`` reduced to the projected cells, in output order."""
        trimmed = [
            getter(row)
            for getter in self._getters
        ]

        return tuple(trimmed)

    def close(self) -> None:
        """Close the source operator."""
        self._source.close()

class Planner:
    """Provide a query plan as a pipeline composed of Volcano operators.

    .. versionadded:: 0.13.0
    """

    _exec_ctx: ExecutionContext

    def __init__(self, ctx: ExecutionContext) -> None:
        """Construct a planner for one already compiled statement.

        Args:
            ctx (ExecutionContext): Carries the invoked statement, the compiled
                operation and its planning context.
        """
        self._exec_ctx = ctx

    def plan(self) -> VolcanoOperator:
        """Build the operator tree for the invoked SELECT/DELETE statement.

        The following shapes are produced, and every SELECT/DELETE takes one of them:

        * aggregate -- ``Scan -> Aggregate``
        * plain select -- ``Scan -> [Filter] -> Project``, the ``Scan`` widened to fetch
          whatever the recheck reads and ``Project`` trimming it back off
        * join -- ``(Scan, Scan) -> HashJoin -> [Filter] -> [Sort]``
        * delete -- ``Scan -> [Filter]``, the ``Scan`` widened to fetch
          whatever the recheck reads. A DELETE with no WHERE clause lowers the retrieval cost
          by fetching the title column only.

        Only the first join of the statement is planned; chaining ``.join()``
        silently drops the rest.

        Returns:
            VolcanoOperator: The root of the plan, ready to be opened.

        Raises:
            InvalidRequestError: If the invoked statement is not a SELECT or a DELETE.

        .. versionchanged:: 0.14.0
            Plan DELETE statements.

        .. versionadded:: 0.13.0
        """
        invoked_stmt = self._exec_ctx.invoked_stmt
        ctx: ExecutionContext = self._exec_ctx
        stmt_table = invoked_stmt.get_table()

        if not invoked_stmt.is_select and not invoked_stmt.is_delete:
            raise InvalidRequestError(
                f"Query planner builds plans for SELECT/DELETE statements only. "
                f"The invoked statement is not a SELECT or DELETE ({type(invoked_stmt).__name__})"
            )

        recheck_where = ctx.compiled.planning_context.recheck_where

        if invoked_stmt.is_select and invoked_stmt._is_aggregate:
            # SELECT aggregate: create a simple scan operator
            schema = SchemaInfo.from_table(
                stmt_table,
                execution_names=ctx.compiled.fetch_columns(),
                projected_names=ctx.compiled.result_columns(),
            )
            scan = Scan(ctx.operation, ctx.parameters, schema=schema)
            return Aggregate(scan, invoked_stmt._raw_columns)

        if invoked_stmt.is_delete or (invoked_stmt.is_select and not invoked_stmt._joins):
            # DELETE or plain SELECT statement (without JOIN)
            execution_names: list[str] = list(ctx.compiled.fetch_columns())
            scan_params = dict(ctx.parameters)

            # True when the plan fetches a different set of properties than the
            # compiler asked for, and so must send its own filter_properties.
            narrows = False

            if recheck_where is not None:
                # A predicate reads a SET of columns, not one: a compound WHERE
                # folds into a BooleanClauseList and every leaf contributes its
                # own. Widen PER COLUMN -- asking whether ANY predicate column
                # is already fetched answers the wrong question and leaves a
                # partially-overlapping predicate short of the one it misses,
                # which costs UNKNOWN on every row and returns nothing.
                #
                # sorted() because _get_expression_columns returns a set, whose
                # iteration order varies per process; without it the Scan's
                # schema and filter_properties are a different permutation on
                # every run.
                where_cols = sorted(
                    {c.name for c in _get_expression_columns(recheck_where)}
                )
                missing = [n for n in where_cols if n not in execution_names]

                if missing:
                    # append behind fetch_columns(), which stays authoritative
                    # for the columns the statement already asked for
                    execution_names.extend(missing)

                    # widen filter_properties to match, or Notion never returns
                    # the cells the recheck has to read.
                    narrows = True

            elif invoked_stmt.is_delete:
                # DELETE with no WHERE clause reads no property, but an empty
                # filter_properties means "all properties" to Notion. Fetch the
                # title only: every Notion data source has one. A list, because
                # a Table declared without a title gives [] (no narrowing).
                title_cols = [
                    c.name
                    for c in stmt_table.uc
                    if isinstance(c.type_, String) and c.type_.is_title
                ]
                execution_names.extend(title_cols)
                narrows = True

            if narrows:
                # Gated on `narrows`, and that gate is load-bearing: the
                # compiler emits query_params only `if query_params`, so a
                # statement the plan does not narrow may carry no key at all.
                # Writing one regardless would send filter_properties for
                # shapes the compiler deliberately left unnarrowed.
                #
                # Copy before writing: scan_params is a shallow copy, so its
                # query_params is still the compiled statement's dict.
                scan_params["query_params"] = dict(scan_params.get("query_params") or {})

                # specials must never ride into filter_properties (ADR-0006)
                scan_params["query_params"]["filter_properties"] = [
                    name
                    for name in execution_names
                    if name not in SpecialColumns
                ]

            schema = SchemaInfo.from_table(
                stmt_table,
                execution_names=execution_names,
                projected_names=ctx.compiled.result_columns(),
            )
            scan = Scan(
                ctx.operation, 
                scan_params,
                schema=schema, 
                # the operator above this Scan can consume batch-by-batch
                yield_per=ctx.execution_options.get("yield_per")    
            )
            plan = scan

            if recheck_where is not None:
                # The recheck (ADR-0022): a pushed conjunct is a HINT, never the answer.
                # Notion's filter is a lossy probe -- `does_not_equal` keeping a valueless
                # cell (#384) is the one divergence measured today, not the reason this is
                # unconditional. Re-apply every held conjunct over raw cells; the recheck
                # decides.
                plan = Filter(
                    source=scan,
                    schema=schema,
                    filter=recheck_where,
                    tables=[stmt_table]
                )

            if invoked_stmt.is_delete:
                # DELETE: just return the plan without projection
                return plan

            # plain SELECT: put the projection at the top of the plan
            return Project(plan, ctx.compiled.planning_context.pre_widening_fetch_columns)
        
        # SELECT with JOIN
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
    
        # add a filter on top of the plan, if the WHERE clause has an expression
        if recheck_where is not None:
            merged_schema = SchemaInfo.from_join(
                join.left,
                join.right,
                *invoked_stmt._projection,
            )

            # The tables the predicate READS: one per leaf, so one for a single-sided
            # conjunct and BOTH for a compound spanning the join.
            #
            # The ORDER OF THIS LIST IS ARBITRARY and nothing may depend on it: it comes
            # from a set, and `Table` defines no `__hash__`, so the iteration order is
            # id-based. That is why it is only ever tested for MEMBERSHIP, never indexed.
            # The `tables[0]` that stood here picked a side by memory-address coin flip;
            # `Filter` no longer needs a choice at all, because each leaf resolves its
            # own page from `column.parent` (ADR-0022).
            predicate_tables = list(_get_expression_parent_tables(recheck_where))
            updated = Filter(
                source=plan,
                schema=merged_schema,
                filter=recheck_where,
                tables=predicate_tables
            )

            plan = updated

        # add a sort on top of the plan, if there is a ORDER BY-clause on the right table
        residual_sorts = ctx.compiled.planning_context.residual_sorts
        if residual_sorts is not None:
            sorts = compile_residual_sorts(residual_sorts)
            merged_schema = SchemaInfo.from_join(join.left, join.right, *invoked_stmt._projection)
            plan = Sort(source=plan, schema=merged_schema, sorts=sorts, table=join.right)
        
        return plan
 
