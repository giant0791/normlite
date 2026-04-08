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
from copy import Error
from operator import itemgetter
import pdb
from types import MappingProxyType
from typing import Any, Callable, NoReturn, Optional, Protocol, Self, Sequence, Union, TYPE_CHECKING
import uuid
import warnings
from normlite._constants import SpecialColumns
from normlite.exceptions import ArgumentError
from normlite.sql.base import Executable, ClauseElement, generative
from normlite.sql.elements import BindParameter, BooleanClauseList, ColumnElement
from normlite.sql.resultschema import SchemaInfo

if TYPE_CHECKING:
    from normlite.sql.schema import Column, Table, ReadOnlyColumnCollection
    from normlite.engine.interfaces import _CoreAnyExecuteParams
    from normlite.engine.cursor import CursorResult
    from normlite.engine.base import Connection
    from normlite.engine.context import ExecutionContext


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

    _table: Table = None
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
            
            seen.add(col)
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
    """The immutable mapping holding the values."""

    def __init__(self, table: Table):
       super().__init__(table)
       self._values = None

    @generative
    def values(self, *args: Union[dict, Sequence[Any]], **kwargs: Any) -> Self:
        """Provide the ``VALUES`` clause to specify the values to be inserted in the new row.

        Each call to this method create a new :class:`ValuesBase` object that carries over the
        values previously added.

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
        existing = dict(self._values) if self._values else {}

        if args:
            # positional args have been passed:
            # either a dict or a sequence has been provided
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
            arg = args[0]

            if not isinstance(arg, dict):
                raise ArgumentError(
                    "Positional argument to values() must be a dict"
                )

            new_values = arg
        else:
            new_values = kwargs

        # Convert raw values → BindParameter
        for key, value in new_values.items():
            # structural validation
            if key not in self.get_table().columns:
                raise ArgumentError(f"Wrong key supplied: {key}")

            if isinstance(value, BindParameter):
                bp = value
            else:
                bp = BindParameter(
                    key=key,
                    value=value,
                    type_=None      # compiler will fill this
                )

            existing[key] = bp

        self._values = MappingProxyType(existing)

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
        
    def _process_dict_values(self, dict_arg: dict) -> MappingProxyType:
        kv_pairs = {}
        try:
            for col in self._table.get_user_defined_colums():
                value = dict_arg[col.name]
                kv_pairs[col.name] = BindParameter(col.name, value, col.type_)
        except KeyError as ke:
            raise KeyError(f'Missing value for: {ke.args[0]}')
        
        return MappingProxyType(kv_pairs)

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
            at_leat_one_usr_column = any([
                not col.is_system
                for col in self._returning
            ])

            if at_leat_one_usr_column:
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
        for row in context._cursor.fetchall():
            context.bulk_parameters = [{
                "path_params": {"page_id": row[0]}
            }]

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
        return self.clauses

class Select(HasTable, ExecutableClauseElement):
    __visit_name__ = 'select'
    is_select = True

    _projection: Sequence[str]
    """The column names to be projected in this select statement."""

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

        if len(entities) == 1 and isinstance(entities[0], Table):
            # a Table object has been provided
            table = entities[0]
            self._table = table

            # project all columns
            self._projection = [
                col
                for col in self._table.c
                if col.name != "table_name"
            ]  
            return

        # A list of columns has been provided
        tables = set()
        columns: list[Column] = []

        for ent in entities:
            if not isinstance(ent, Column):
                raise ArgumentError(
                    "select() arguments must be either a Table or Column objects"
                )
            tables.add(ent.parent)
            columns.append(ent)

        if len(tables) != 1:
            raise ArgumentError(
                "All selected columns must belong to the same table"
            )

        self._table: Table = tables.pop()
        # --- SAFEGUARD: column names must exist on the table ---
        table_columns = self._table.columns

        for col in columns:
            if col.name not in table_columns:
                raise ArgumentError(
                    f'Column: {col.name} does not belong to table: {self._table.name}'
                )
        self._projection = columns

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

    def _setup_execution(self, context: ExecutionContext) -> None:
        # nothing to be setup
        pass

    def _finalize_execution(self, context: ExecutionContext) -> None:
        # nothing to be finalized
        pass

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
