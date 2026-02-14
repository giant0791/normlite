# sql/ddl.py
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
from dataclasses import dataclass
import pdb
from typing import TYPE_CHECKING, Any, NoReturn, Optional

from normlite._constants import SpecialColumns
from normlite.future.engine.cursor import CursorResult
from normlite.engine.context import ExecutionContext
from normlite.exceptions import InvalidRequestError, NoSuchTableError
from normlite.engine.reflection import ReflectedTableInfo
from normlite.sql.base import Executable
from normlite.sql.type_api import type_mapper
from normlite.notiondbapi.dbapi2 import Error, InternalError, ProgrammingError

if TYPE_CHECKING:
    from normlite.sql.schema import Table
    from normlite.engine.interfaces import _CoreAnyExecuteParams
    from normlite.engine.cursor import CursorResult
    from normlite.engine.base import Connection

@dataclass
class _ColumnMetadata:
    """Lightweight view over one DBAPI column metadata row."""

    name: str
    type_code: Any
    col_id: Optional[str]
    args: Optional[dict]
    value: Any

    @classmethod
    def from_row(cls, row: tuple[Any, ...]) -> _ColumnMetadata:
        # positional mapping wrapped in one safe location
        name, type_code, notion_id, notion_args, value = row
        return cls(
            name=name,
            type_code=type_code,
            notion_id=notion_id,
            notion_args=notion_args,
            value=value
        )

    # --- helper accessors ---

    def is_special(self) -> bool:
        return self.name in SpecialColumns.values()

    def is_user_property(self) -> bool:
        return not self.is_special()

    def type_engine(self):
        """Return a TypeEngine instance, using the global type mapper."""
        Factory = type_mapper[self.type_code]
        return Factory(self.args)   # args set only types only
    
class HasTableMixin:
    """Mixin for objects that have a :class:`normlite.sql.schema.Table` object.
    
    .. versionadded:: 0.8.0
    """

    def get_table(self) -> Table:
        return self._table

class ExecutableDDLStatement(HasTableMixin, Executable):
    """Base class for all DDL executable statements.
    
    .. versionadded:: 0.8.0
        This class is a convenient base class to set the :attr:`is_ddl` for all subclasses.
    """
    is_ddl = True

    def __init__(self):
        self._table = None

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
            execution_options=merged_execution_options
        )

class CreateTable(ExecutableDDLStatement):
    """Represent a ``CREATE TABLE`` statement.
    
    .. versionchanged:: 0.8.0
        This version runs on the new :class:`normlite.engine.base.Connection` execution pipeline.

    .. versionadded:: 0.7.0    
    """
    __visit_name__ = 'create_table'

    def __init__(self, table: Table):
        super().__init__()
        self._table = table
      
    def _setup_execution(self, context: ExecutionContext) -> None:
        # nothing to be setup
        pass

    def _finalize_execution(self, context: ExecutionContext) -> None:
        # IMPORTANT: This consumes the result stored in the execution context.
        # DDL reflection is not part of execution — it is interpretation of results.
        # So reflection consumes the results by interpreting and leaves the
        # result empty in the context.
        result = context.setup_cursor_result()
        rows = result.all()        
        reflected_table_info = ReflectedTableInfo.from_rows(rows)

        table = self._table
        
        # assign the table id
        table.set_oid(reflected_table_info.id)

        # assign user column ids
        for colmeta in reflected_table_info.get_user_columns():
            table.c[colmeta.name]._id = colmeta.id

        # assign sys column values
        for colmeta in reflected_table_info.get_sys_columns():
            if colmeta.name in (SpecialColumns.NO_ID, SpecialColumns.NO_TITLE):
                # skip the object id and title, they currently are not modelled as columns
                # but attributes
                continue

            table.c[colmeta.name].value = colmeta.value
        
        # add this table to the sys "tables"
        engine = context.engine
        entry = context.engine._get_or_create_sys_tables_row(
            table.name,
            table_catalog=engine._user_database_name,
            table_id=table.get_oid()
        )

        table._sys_tables_page_id = entry.sys_tables_page_id

class DropTable(ExecutableDDLStatement):
    """Represent a ``DROP TABLE`` statement.

    .. versionadded:: 0.8.0
    """
    __visit_name__ = 'drop_table'    

    def __init__(self, table: Table):
        super().__init__()
        self._table = table

    def _setup_execution(self, context: ExecutionContext) -> None:
        # nothing to be setup
        pass

    def _handle_dbapi_error(
        self, 
        exc: Error, 
        context
    ) -> Optional[NoReturn]:
        # DROP TABLE on a non-existing table
        if isinstance(exc, ProgrammingError):
            raise NoSuchTableError(self._table.name) from exc

        # All other DBAPI errors propagate unchanged
        raise

    def _finalize_execution(self, context: ExecutionContext) -> None:
        # IMPORTANT: This consumes the result
        result = context.setup_cursor_result()
        rows = result.all()
        reflected_table_info = ReflectedTableInfo.from_rows(rows)

        engine = context.engine

        # Ensure we have a valid sys_tables_page_id
        if self._table._sys_tables_page_id is None:
            sys_tables_page = engine._require_sys_tables_row(
                self._table.name,
                table_catalog=engine._user_database_name,
            )
            self._table._sys_tables_page_id = sys_tables_page.sys_tables_page_id

        # Attempt deletion (retry once if stale page_id)
        try:
            context.engine._delete_restore_table(
                self._table._sys_tables_page_id,
                delete=True,
            )

        except ProgrammingError:
            # page_id likely stale → recover and retry once
            sys_tables_page = engine._require_sys_tables_row(
                self._table.name,
                table_catalog=engine._user_database_name,
            )

            self._table._sys_tables_page_id = sys_tables_page.sys_tables_page_id

            # retry; further failure propagates
            context.engine._delete_restore_table(
                self._table._sys_tables_page_id,
                delete=True,
            )

class ReflectTable(ExecutableDDLStatement):
    """Represent a convenient pseudo DDL statement to reflect a Notion database into a Python :class:`normlite.sql.schema.Table` object.
    
    :class:`ReflectTable` expects that the database id is known (from a previous execution of :class:`HasTable`).

    .. versionadded:: 0.8.0
    """
    __visit_name__ = 'reflect_table'

    def __init__(self, table: Table):
        self._reflected_table = table
        """The :class:`normlite.sql.schema.Table` object to be reflected."""

        self._reflected_table_info = None
        """The reflected info data structure holding the reflected columns."""

    def execute(self, context: ExecutionContext, parameters: Optional[dict] = None) -> CursorResult:
        cursor = context._dbapi_cursor
        compiled = context._compiled
        if not compiled.params:
            raise InvalidRequestError(
                'Pseudo-DDL statement "ReflectTable" cannot be used without previous '
                'execution of "HasTable" (table oid is unknown).'
            )
        cursor.execute(compiled.as_dict()['operation'], compiled.params)
        result = context._setup_cursor_result()
        reflected_cols_as_rows = result.all()
        self._reflected_table_info = ReflectedTableInfo.from_rows(reflected_cols_as_rows)
        self._post_exec(result, context)
        return result


    def _post_exec(self, result: CursorResult, context: ExecutionContext) -> None:
        from normlite.sql.schema import Column        
        
        self._reflected_table._db_parent_id = None

        # reflect columns
        for colmeta in self._reflected_table_info.get_columns():
            primary_key = True if colmeta.name == SpecialColumns.NO_ID.value else False
            new_col = Column(
                colmeta.name,
                colmeta.type,
                colmeta.id,
                primary_key
            )

            new_col._set_parent(self._reflected_table)
            self._reflected_table.append_column(new_col)

        # reflect primary key
        self._reflected_table._create_pk_constraint()
        self._reflected_table.add_constraint(self._reflected_table.primary_key)

    def _as_info(self) -> ReflectedTableInfo:
        return self._reflected_table_info

    def get_table(self) -> Table:
        return self._reflected_table


