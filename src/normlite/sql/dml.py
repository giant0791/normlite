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
from normlite.notion_sdk.client import NotionError
from normlite.notiondbapi import Error
from normlite.notiondbapi.resultset import ResultSet
from normlite.sql.schema import ColumnCollection

import pdb
from types import MappingProxyType
from typing import Any, Callable, Mapping, NoReturn, Optional, Protocol, Self, Sequence, Type, Union, TYPE_CHECKING, Tuple
import collections.abc as collections_abc

import warnings
from normlite.exceptions import ArgumentError
from normlite.sql.base import Executable, ClauseElement, generative
from normlite.sql.elements import BinaryExpression, BindParameter, BooleanClauseList, ColumnElement
from normlite.sql.resultschema import ResultColumn, SchemaInfo
from normlite.sql.type_api import Relation
from normlite.sql.schema import Column

if TYPE_CHECKING:
    from normlite.sql.schema import Table, ReadOnlyColumnCollection
    from normlite.engine.interfaces import _CoreAnyExecuteParams
    from normlite.engine.cursor import CursorResult
    from normlite.engine.base import Connection
    from normlite.engine.context import ExecutionContext
    from normlite.notiondbapi.dbapi2 import DBAPIErrorHandlerType
    from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection
    from normlite.notiondbapi.dbapi2 import Cursor as DBAPICursor 

def build_phase_two_batch(
    left_schema: SchemaInfo,
    onclause_column: Column,
    left_rows: list[tuple],
) -> list[dict]:
    """Return deduplicated `pages.retrieve` path_params for phase 2."""

    # build the bulk parameters
    # one parameter for each page_id contained in the FK relation column stored in
    # each page returned by the databases.query
    # REMEMBER: A Relation column stores non-scalar values -> list[str]
    seen: set[str] = set()
    bulk_params: list[dict] = []

    get_fk_col = left_schema.column_getter(onclause_column.name)
    for row in left_rows:
        oids = onclause_column.type_.result_processor()(get_fk_col(row)) or []
        for oid in oids:
            if oid not in seen:
                seen.add(oid)
                bulk_params.append({"path_params": {"page_id": oid}})

    return bulk_params

def merge_inner_join_rows(
    left_schema: SchemaInfo,
    right_schema: SchemaInfo,
    onclause_column: Column,
    left_rows: list[tuple],
    right_rows: list[tuple],
) -> list[tuple]:
    """Cross-product left rows with right rows whose object_id matches the
    decoded relation list on each left row. Drops left rows whose relation
    resolves to zero matching right rows (inner-join semantics)."""

    merged_rows = []

    # prepare the getters
    relation_proc = onclause_column.type_.result_processor()
    get_oids = left_schema.column_getter(onclause_column.name)
    get_right_oid =  right_schema.column_getter("object_id")

    # construct a helper dictionary to contain {"id": <right_row>}
    # then the check whether to merge the right row is o(1) (dictionary lookup)
    right_by_oid = {get_right_oid(rr): rr for rr in right_rows}

    # build the cross-product
    for left_row in left_rows:
        fk_oids = relation_proc(get_oids(left_row)) or []
        for fk_oid in fk_oids:
            right_row = right_by_oid.get(fk_oid)
            if right_row is None:
                continue

            merged_rows.append(
                _project_inner_join(
                    left_schema,
                    right_schema,
                    left_row, 
                    right_row
                )
            )

    return merged_rows

def _project_inner_join(
    left_schema: SchemaInfo,
    right_schema: SchemaInfo,
    left_row: tuple, 
    right_row: tuple
) -> tuple:
    left_projected = tuple(
        [
            left_row[left_schema.column_index(lc.name)] 
            for lc in left_schema.columns 
            if lc.name != "object_id"
        ]
    )
    right_projected = tuple(
        [
            right_row[right_schema.column_index(rc.name)] 
            for rc in right_schema.columns 
            if rc.name != "object_id"
        ]
    )

    return (*left_projected, *right_projected)


class HasTable(Protocol):
    """Mixin for DML statements"""

    def get_table(self) -> Table:
        try:
            return self._table
        except AttributeError:
            raise ArgumentError(
                f"Class '{self.__class__.__name__}' shall have a '_table' attribute."
            )

class ExecutableClauseElement(Executable):
    """Specialized executable for DML statements
    
    This class is the base of all DML constructs.

    .. versionadded:: 0.8.0
    """
    
    is_ddl = False

    def _execute_on_connection(
            self, 
            connection: Connection, 
            params: Optional[_CoreAnyExecuteParams],
            *, 
            execution_options: Optional[dict] = None
    ) -> CursorResult:

        stmt_opts = self._execution_options or {}
        call_opts = execution_options or {}
        merged_execution_options = stmt_opts | call_opts

        return connection._execute_context(
            self, 
            params, 
            execution_options=merged_execution_options
        )
    
class UpdateBase(HasTable, ExecutableClauseElement):
    """Base for all DML statements.

    This class allows the generative pattern and provide RETURNING columns incrementally.

    .. versionadded:: 0.9.0
    """

    _returning: tuple[Column, ...] = ()
    """Provide the columns to be returned by the DML statement."""

    _table: Table
    """The table this executable is referred to."""

    def __init__(self, table: Table):
        self._table = table

    @generative
    def returning(self, *cols: Column) -> Self:
        """Add the specified columns to the columns to be returned.

        Raises:
            ArgumentError: If a specified column does not belong to the table this statement
                is applied to.

        Returns:
            Self: This instance for generative usage.
        """

        existing = list(self._returning)
        seen = {
            (col.parent.name, col.name, col.is_system)
            for col in existing
        }

        for col in cols:
            key = (col.parent.name, col.name, col.is_system)

            if key in seen:
                warnings.warn(
                    f"Column '{col.name}' already declared as returning, skipping."
                )
                continue

            if col.parent is not self._table:
                raise ArgumentError(
                    f"Column: '{col.name}' does not belong to table: '{self._table.name}'"
                )
            
            seen.add(key)
            existing.append(col)

        self._returning = tuple(existing)
        return self

class ValuesBase(UpdateBase):
    """Base for all DML statements with the VALUES clause.

    This class allows the generative pattern and provide values incrementally.

    .. versionchanged:: 0.9.0
        Fix class MRO issue and provide more logical hierarchy structure.

    .. versionadded:: 0.8.0
    """

    _values: Optional[MappingProxyType] = None
    """The immutable mapping holding the template.

    This attributes holds the template to bind the values stored in :attr:`_single_parameters` at runtime.
    
    .. versionchanged:: 0.9.0
        This attributes holds a mapping containing bind parameters instead of plain values.
    """

    _has_multi_parameters: Optional[bool] = False
    """``True`` if the VALUES clause provides multiple parameters.
    
    .. versionadded:: 0.9.0
    """

    _single_parameters: Optional[Mapping[str, Any]] = None
    """The values for single parameters as a mapping.
    
    .. versionadded:: 0.9.0
    """

    _multi_parameters: Optional[Sequence[Mapping[str, Any]]] = None
    """The values for multiple parameters as a sequence of mappings.
    
    .. versionadded:: 0.9.0
    """

    def __init__(self, table: Table):
        super().__init__(table)

    def _process_single_values(self, values: Mapping[str, Any]) -> MappingProxyType:
        if self._has_multi_parameters:
            raise ArgumentError(
                "Cannot mix single values with bulk values"
            )

        existing_values = dict(self._values) if self._values else {}
        existing_params = dict(self._single_parameters) if self._single_parameters else {}

        for key, value in values.items():
            if key not in self.get_table().columns:
                raise ArgumentError(f"Wrong key supplied: {key}")

            # template
            existing_values[key] = BindParameter(key=key, type_=None)

            # actual data
            existing_params[key] = value

        self._values = MappingProxyType(existing_values)
        self._single_parameters = existing_params
    
    def _process_multi_values(
        self, 
        rows: Sequence[Mapping[str, Any]]
    ) -> None:

        if self._values and not self._has_multi_parameters:
            raise ArgumentError(
                "Cannot mix bulk values with existing single values"
            )

        if not rows:
            raise ArgumentError("values() received empty sequence")

        normalized_rows = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise ArgumentError(
                    "Bulk values must be a sequence of mappings"
                )
            normalized_rows.append(dict(row))

        key_sets = [set(row.keys()) for row in normalized_rows]
        all_keys = set.union(*key_sets)

        for i, keys in enumerate(key_sets):
            if keys != all_keys:
                raise ArgumentError(
                    f"Inconsistent keys in row {i}: {keys} != expected {all_keys}"
                )

        table_columns = self.get_table().columns
        for key in all_keys:
            if key not in table_columns:
                raise ArgumentError(f"Wrong key supplied: {key}")

        template = {
            key: BindParameter(
                key=key,
                type_=None      # no value specified, it defaults to NoArg.NO_ARG
            )
            for key in all_keys
        }
        self._values = MappingProxyType(template)
        self._multi_parameters = normalized_rows
        self._has_multi_parameters = True

    @generative
    def values(
        self, 
        *args: Union[dict, Sequence[Mapping[str, Any]]], 
        **kwargs: Any
    ) -> Self:
        """Provide the ``VALUES`` clause to specify the values to be inserted in the new row.

        Each call to this method create a new :class:`ValuesBase` object that carries over the
        values previously added.

        .. versionchanged:: 0.9.0
            Support for multi-parameters added.

        .. versionchanged:: 0.8.0
            The values provided are now coerced to :class:`normlite.sql.elements.BindParameter`
            objects.

        Raises:
            ArgumentError: If both positional and keyword arguments are passes, or
                if not enough values are supplied for all columns, or if values are passed 
                with a class that is neither a dictionary not a tuple. 

        Returns:
            Self: This instance for generative usage.

        """

        if self._has_multi_parameters:
            raise ArgumentError("values() already called with bulk parameters")

        if args:
            # positional case: either a dict or a sequence has been provided.
            # If it is a sequence, distinguish between multi-parameter case (sequence of dicts)
            # and the single parameter case (a tuple of values)
            # IMPORTANT: args is a tuple containing the dict or sequence as first element
            arg = args[0]
            if kwargs:
                # either positional or kwargs but not both are allowed
                raise ArgumentError(
                    'Cannot pass positional and keyword arguments '
                    'to values() simultanesously'
                )

            if len(args) != 1:
                raise ArgumentError(
                    "values() accepts at most one positional argument"
                )
            
            if isinstance(arg, collections_abc.Sequence) and not isinstance(arg, (str, bytes)):
                # it is a sequence: determine if it is a sequence of dictionary
                if arg and isinstance(arg[0], dict):
                    # values([{"name": "Galileo Galilei", ...}, {"name": "Isaac Newton", ...}, ...])
                    # multi-parameters case
                    self._process_multi_values(arg)
                    self._has_multi_parameters = True
                    return self
                
                if arg and isinstance(arg[0], (list, tuple)):
                    # values(["Galileo Galilei", ...], ["Isaac Newton", ...]) NOT SUPPORTED
                    raise ArgumentError(
                        "values() accepts sequence of dictionaries only for bulk inserts"
                    )
            
            if isinstance(arg, dict):
                # values({"name": "...", })
                self._process_single_values(arg)
                self._has_multi_parameters = False
                self._multi_parameters = None
                return self

            # values(("Galileo Galilei", 1, "A", ...)) single parameter case with a tuple or a list
            # normalize tuple or list to dict, but ensure length is equal to columns
            columns = list(self._table.uc)
            if len(arg) != len(columns):
                raise ArgumentError(
                    f"Expected {len(columns)} values, got {len(arg)}"
                )

            arg = {c.name: value for c, value in zip(columns, arg)}            
            new_values = arg
        else:
            # kwarg single parameter case
            new_values = kwargs

        self._has_multi_parameters = False
        self._multi_parameters = None
        self._process_single_values(new_values)

        return self

class Insert(ValuesBase):
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
        >>> stmt.values({'id': 123456, 'name': 'Isaac Newton', 'grade': 'B'})
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

    .. versionchanged:: 0.8.0
        New base class

    .. versionchanged:: 0.7.0 
        The old construct has been completely redesigned and refactored.
        Now, the new class provides all features of the SQL ``INSERT`` statement.

    """
    
    is_insert = True
    __visit_name__ = 'insert'

    def __init__(self, table: Table):
        super().__init__(table)

    def get_table_columns(self) -> ReadOnlyColumnCollection:
        return self._table.columns
        
    def _setup_execution(self, context: ExecutionContext) -> None:
        # setup only if returning is declared
        if not self._returning:
            return
        
        # 1. prepare result cursor (but don’t assign yet)
        result_cursor = context.connection._engine.raw_connection().cursor()

        fetch_schema = SchemaInfo.from_table(
            self._table,
            execution_names=None,
            projected_names=context.compiled.result_columns()
        )
        
        result_cursor._inject_description(fetch_schema.as_sequence())

        # 2. stage it
        context._staged_result_cursor = result_cursor

        # 3. stage operation
        context.bulk_operation = {
            "endpoint": "pages",
            "request": "retrieve"
        }

        # parameters will be filled in _finalize_execution()    

    def _handle_dbapi_error(
        self, 
        exc: Error, 
        context: ExecutionContext
    ) -> Optional[NoReturn]:
        
        # all DBAPI errors propagate
        raise

    def _finalize_execution(self, context: ExecutionContext) -> None:
        if self._returning:
            self._post_fetch_inserted_row(context)    
            context._returned_primary_keys_rows = None
            return
        
        implicit_returning = context.execution_options.get("implicit_returning", False)
        if not implicit_returning:
            result = context.setup_cursor_result()
            result._soft_close()
            context._returned_primary_keys_rows = None
            return
        
        if implicit_returning:
            result = context.setup_cursor_result()
            result._soft_close()
            context._returned_primary_keys_rows = context.cursor._last_inserted_row_ids

    def _post_fetch_inserted_row(self, context: ExecutionContext) -> None:
        elem = context.invoked_stmt

        # build parameters for pages.retrieve
        # for insert with single parameters, there is just 1 result set
        # for bulk insert, there are as many result sets as rows inserted, thus the use of _iter_all()
        context.bulk_parameters = []
        for row in context._cursor._iter_all():
            context.bulk_parameters.append({
                "path_params": {"page_id": row[0]}
            })

        # execute post-fetch
        try:
            context.engine.do_executemany(
                context._staged_result_cursor,
                context.bulk_operation,
                context.bulk_parameters
            )
        except Error as exc:
            elem._handle_dbapi_error(exc, context)

        # finalize cursor routing
        context._result_cursor = context._staged_result_cursor

    def __repr__(self):
        kwarg = []
        if self._table:
            kwarg.append('_table')

        if self._values:
            kwarg.append('values')

        
        return "Insert(%s)" % ", ".join(
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg]
        )
        
def insert(table: Table) -> Insert:
    """Construct an insert statement.

    This class constructs an SQL ``INSERT`` statement capable of inserting rows
    to this table.

    .. versionchanged:: 0.7.0
        Now, it uses the :class:`normlite.sql.schema.Table` as table object.

    Returns:
        Insert: A new insert statement for this table. 
    """
    return Insert(table)

class HasExpression(Protocol):
    """Mixin for elements that have an expression."""

    def has_expression(self) -> bool:
        ...

class WhereClause(HasExpression, ColumnElement):
    """Base class for DML statements that have a where-clause.
    
    .. versionadded:: 0.8.0
    """

    __visit_name__ = 'where_clause'

    def __init__(self, expression: Optional[ColumnElement] = None):
        self.expression = expression
        """The column expression in this where clause."""

    def has_expression(self) -> bool:
        """Explicit test on expression availability to avoid :exc:`TypeError`.
        
        Users of the :class:`WhereClause` shall test for presence of an expression 
        by invoking this method and not simply ...
        This safely bypasses Python thruthiness invocation, which otherwise raises
        a :exc:`TyperError`.

        .. seealso::
            :meth:`normlite.sql.elements.ColumnElement.__bool__` 
                This method is overloaded to forbid Python truthiness.             

        Returns:
            bool: ``True`` if :attr:`expression` is not ``None``.
        """
        return self.expression is not None

    def where(self, expr: ColumnElement) -> WhereClause:
        if self.expression is None:
            return WhereClause(expr)

        return WhereClause(
            BooleanClauseList(
                operator="and",
                clauses=[self.expression, expr]
            )
        )
    
class OrderByClause(HasExpression, ClauseElement):
    __visit_name__ = 'order_by_clause'

    def __init__(self, clauses: tuple[ColumnElement, ...] = ()):      
        self.clauses = clauses

    def add(self, *clauses: ColumnElement) -> OrderByClause:
        return OrderByClause(self.clauses + clauses)
    
    def has_expression(self) -> bool:
        return self.clauses != ()

class Select(HasTable, ExecutableClauseElement):
    __visit_name__ = 'select'
    
    is_select = True

    _projection: ReadOnlyColumnCollection
    """The column names to be projected in this select statement."""

    _right: Optional[Table] = None

    def __init__(self, *entities: Union[Table, Column]):
        from normlite.sql.schema import Column, Table

        if not entities:
            raise ArgumentError(
                """
                    select() requires either table or a list of columns,
                    no arguments were provided.
                """
            )

        self._whereclause = WhereClause()
        self._order_by = OrderByClause()

        # a tuple is than a list because it forces rebind-not-mutate discipline
        # see how WhereClause and OrderByClause both encapsulate a tuple of clauses
        self._joins: tuple[Join] = ()

        if len(entities) == 1 and isinstance(entities[0], Table):
            # a Table object has been provided
            table = entities[0]
            self._table = table

            # project all columns
            self._projection = self._table.c
            return
        
        if len(entities) == 2:
            if isinstance(entities[0], Table) and isinstance(entities[1], Table):
                # select columns from multiple tables: join case
                self._table = entities[0]
                self._right = entities[1]
                self._projection = ColumnCollection().as_readonly()
                return 

        # A list of columns has been provided
        tables = set()
        columns: list[tuple[str, Column]] = []

        for ent in entities:
            if not isinstance(ent, Column):
                raise ArgumentError(
                    "select() arguments must be either a Table or Column objects"
                )
            tables.add(ent.parent)
            columns.append((ent.name, ent))

        if len(tables) != 1:
            raise ArgumentError(
                "All selected columns must belong to the same table"
            )

        self._table: Table = tables.pop()
        # --- SAFEGUARD: column names must exist on the table ---
        table_columns: ReadOnlyColumnCollection = self._table.columns

        for column in columns:
            _, col = column
            if col.name not in table_columns:
                raise ArgumentError(
                    f'Column: {col.name} does not belong to table: {self._table.name}'
                )
        self._projection = ColumnCollection(columns).as_readonly()

    @generative
    def where(self, expr: ColumnElement) -> Self:
        self._whereclause = self._whereclause.where(expr)
        return self

    @generative
    def order_by(self, *clauses: ColumnElement) -> Self:
        if not clauses:
            raise ArgumentError(
                """
                    order_by() requires at least one clause,
                    no arguments provided.
                """
            )

        self._order_by = self._order_by.add(*clauses)
        return self

    @generative
    def join(self, onclause: ColumnElement) -> Self:
        if isinstance(onclause, BinaryExpression):
            inner_col = onclause.column
            if not isinstance(inner_col.type_, Relation):
                raise ArgumentError(
                    f"join() onclause must be a BinaryExpression with a Relation column, got '{inner_col.name}' "
                    f"(type: {type(inner_col.type_).__name__})"
                )
    
            if inner_col.parent is not self._table:
                message = f"""
                    join() onclause must a BinaryExpression and its column '{inner_col.name}' 
                    must belong to left table '{self._table.name}',
                    got column '{inner_col.name}' on table '{inner_col.parent.name}'
                """
                raise ArgumentError(message)
        elif isinstance(onclause, Column):
            if not isinstance(onclause.type_, Relation):
                    raise ArgumentError(
                        f"join() onclause must be a Relation column, got '{onclause.name}' "
                        f"(type: {type(onclause.type_).__name__})"
                    )
        
            if onclause.parent is not self._table:
                message = f"""
                    join() onclause must belong to left table '{self._table.name}',
                    got column '{onclause.name}' on table '{onclause.parent.name}'
                """
                raise ArgumentError(message)
        
        else: 
            raise ArgumentError(
                f"join() onclause must be a Column "
                f"or BinaryExpression, got {type(onclause).__name__}"
            )

        # mutate-vs-rebind: here you create a new tuple
        self._joins = (*self._joins, Join(self._table, self._right, onclause))
        return self
    
    def _setup_execution(self, context: ExecutionContext) -> None:
        # guard against execution if joins are empty
        if not self._joins:
            return
        
        # phase 1: Retrieve all rows of the left table in the join
        elem = context.invoked_stmt

        # 1. prepare schema for compiled query
        context._join = self._joins[0]
        context._join_left_schema = SchemaInfo.from_table(
            context._join.left,
            execution_names=context.compiled.fetch_columns(),
            projected_names=[c.name for c in context._join.left.uc]  
        )
        context._cursor._inject_description(
            context._join_left_schema.as_sequence()
        )

        # execute the compiled query
        context.engine.do_execute(
            context._cursor,
            context.operation,
            context.parameters
        )

        context._join_left_rows = context._cursor.fetchall()

        # prepare phase 2: retrieve left table rows 
        # build parameters for pages.retrieve
        result_cursor = context.connection._engine.raw_connection().cursor()
        right_schema: SchemaInfo = SchemaInfo.from_table(
            context._join.right,
            execution_names=context.compiled.fetch_columns(),
            projected_names=[c.name for c in context._join.right.uc]    
        )
        result_cursor._inject_description(right_schema.as_sequence())
        result_cursor.errorhandler = _join_errorhandler
        context._join_right_schema = right_schema

        # build the bulk parameters
        # one parameter for each page_id contained in the FK relation column stored in
        # each page returned by the databases.query
        # REMEMBER: A Relation column stores non-scalar values -> list[str]
        onclause_column = context._join.onclause
        bulk_params = build_phase_two_batch(
            context._join_left_schema,
            onclause_column,
            context._join_left_rows
        )

        # staged cursor will contain 1 result set for each retrieved row
        context._staged_result_cursor = result_cursor
        context.bulk_operation = {
            "endpoint": "pages",
            "request": "retrieve"
        }

        context.bulk_parameters = bulk_params

    def _finalize_execution(self, context: ExecutionContext) -> None:
        # guard against execution if joins are empty
        if not self._joins:
            return

        # context._result_cursor will hold the joined rows
        joined = self._build_joined_schema(context)
        context._result_cursor = context.engine.raw_connection().cursor()

        # merge left rows with the corresponding right rows
        # _staged_cursor has multiple result sets, one for each retrieved right row
        right_rows = [r for r in context._staged_result_cursor._iter_all()]
        
        # construct the inner join and add the result to the cursor
        merged_rows = merge_inner_join_rows(
            context._join_left_schema,
            context._join_right_schema,
            context._join.onclause, 
            context._join_left_rows, 
            right_rows
        )
        context._result_cursor._result_sets.append(
            ResultSet(
                joined.as_sequence(),
                "page",
                merged_rows
            )
        )

    def _build_joined_schema(self, context: ExecutionContext) -> SchemaInfo:
        # the result cursor has the schema of a joined table
        # the joined table has all left and right table's columns 
        left = context._join.left
        right = context._join.right
        joined_table_cols = [*left.uc, *right.uc]
        result_cols = [
            ResultColumn(
                rc.name, 
                type_code=rc.type_.get_dbapi_type(), 
                nullable=False
            )
            for rc in joined_table_cols
        ]
        return SchemaInfo(result_cols)
    
def select(*entities: Union[Table, Column]) -> Select:
    return Select(*entities)
        

class Delete(UpdateBase):
    """Represent a SQL DELETE statement."""

    __visit_name__ = "delete"
    is_delete = True

    def __init__(self, table: Table):
        UpdateBase.__init__(self, table)
        self._whereclause = WhereClause()

    @generative
    def where(self, expr: ColumnElement) -> Self:
        self._whereclause = self._whereclause.where(expr)
        return self
    
    def _setup_execution(self, context: ExecutionContext) -> None:
        elem = context.invoked_stmt

        # 1. prepare schema for compiled query
        schema_for_query: SchemaInfo = SchemaInfo.from_table(
            self._table,
            execution_names=context.compiled.fetch_columns(),
            projected_names=context.compiled.result_columns()    
        )
        context._cursor._inject_description(
            schema_for_query.as_sequence()
        )

        # execute the compiled query
        try:
            context.engine.do_execute(
                context._cursor,
                context.operation,
                context.parameters
            )
        except Error as exc:
            elem._handle_dbapi_error(exc, context)

        pages = context._cursor.fetchall()

        # build parameters for pages.update
        result_cursor = context.connection._engine.raw_connection().cursor()
        schema_for_returning: SchemaInfo = SchemaInfo.from_table(
            self._table,
            execution_names=context.compiled.fetch_columns(),
            projected_names=context.compiled.result_columns()    
        )
        result_cursor._inject_description(schema_for_returning.as_sequence())
        bulk_params = []
        get_object_id = schema_for_returning.column_getter("object_id")

        for page in pages:
            bulk_params.append({
                "path_params": {"page_id": get_object_id(page)},
                "payload": {"in_trash": True}
            })

        context._staged_result_cursor = result_cursor
        context.bulk_operation = {
            "endpoint": "pages",
            "request": "update"
        }

        context.bulk_parameters = bulk_params
    
    def _finalize_execution(self, context: ExecutionContext) -> None:
        if self._returning:
            # finalize cursor routing
            context._result_cursor = context._staged_result_cursor
            return

        implicit_returning = context.execution_options.get("implicit_returning", False)
        if not implicit_returning:
            result = context.setup_cursor_result()
            result._soft_close()
            context._returned_primary_keys_rows = None
            return
        
        if implicit_returning:
            result = context.setup_cursor_result()
            result._soft_close()
            context._returned_primary_keys_rows = context.cursor._last_inserted_row_ids

def delete(table: Table) -> Delete:
    return Delete(table)


class Update(ValuesBase):
    """Represent a SQL UPDATE statement."""

    __visit_name__ = 'update'
    is_update = True

    def __init__(self, table: Table):
        ValuesBase.__init__(self, table)
        self._whereclause = WhereClause()

    @generative
    def where(self, expr: ColumnElement) -> Self:
        self._whereclause = self._whereclause.where(expr)
        return self

    def _setup_execution(self, context: ExecutionContext) -> None:
        elem = context.invoked_stmt

        schema_for_query: SchemaInfo = SchemaInfo.from_table(
            self._table,
            execution_names=context.compiled.fetch_columns(),
            projected_names=context.compiled.result_columns(),
        )
        context._cursor._inject_description(schema_for_query.as_sequence())

        try:
            context.engine.do_execute(
                context._cursor,
                context.operation,
                context.parameters,
            )
        except Error as exc:
            elem._handle_dbapi_error(exc, context)

        pages = context._cursor.fetchall()

        result_cursor = context.connection._engine.raw_connection().cursor()
        schema_for_result: SchemaInfo = SchemaInfo.from_table(
            self._table,
            execution_names=context.compiled.fetch_columns(),
            projected_names=context.compiled.result_columns(),
        )
        result_cursor._inject_description(schema_for_result.as_sequence())

        get_object_id = schema_for_result.column_getter("object_id")
        update_payload_template = context.compiled_dict['update_payload']
        bulk_params = []

        for page in pages:
            params_copy = dict(context.resolved_params)
            properties = context._bind_params(update_payload_template, params_copy)
            bulk_params.append({
                "path_params": {"page_id": get_object_id(page)},
                "payload": {"properties": properties},
            })

        context._staged_result_cursor = result_cursor
        context.bulk_operation = {"endpoint": "pages", "request": "update"}
        context.bulk_parameters = bulk_params

    def _finalize_execution(self, context: ExecutionContext) -> None:
        if self._returning:
            context._result_cursor = context._staged_result_cursor
            return

        implicit_returning = context.execution_options.get("implicit_returning", False)
        if not implicit_returning:
            result = context.setup_cursor_result()
            result._soft_close()
            context._returned_primary_keys_rows = None
            return

        result = context.setup_cursor_result()
        result._soft_close()
        context._returned_primary_keys_rows = context.cursor._last_inserted_row_ids


def update(table: Table) -> Update:
    return Update(table)

class Join(ClauseElement):
    """Represent a ``JOIN`` construct between two tables.
    
    .. versionadded:: 0.11.0
    """
    __visit_name__ = "join"
    
    left: Table
    """Left side of the ``JOIN``"""

    right: Table
    """Right side of the ``JOIN``"""

    onclause: Column
    """The column representing the ``ON`` clause of the join"""

    isouter: bool
    """if True, render a ``LEFT OUTER JOIN``, instead of ``JOIN``"""

    def __init__(
        self,
        left: Table,
        right: Table,
        onclause: Column,
        isouter: bool = False
    ):
        self.left = left
        self.right = right
        self.onclause = onclause
        self.isouter = isouter


def _join_errorhandler(
    connection: DBAPIConnection, 
    cursor: DBAPICursor, 
    errorclass: Type[BaseException],
    errorvalue: BaseException
) -> None:
    """Errorhandler for the inner-join staged result cursor.

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

