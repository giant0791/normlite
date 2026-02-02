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
import pdb
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from normlite._constants import SpecialColumns
from normlite.future.engine.cursor import CursorResult
from normlite.engine.context import ExecutionContext
from normlite.exceptions import InvalidRequestError, MultipleResultsFound, NoResultFound, NormliteError
from normlite.engine.reflection import ReflectedTableInfo
from normlite.sql.base import ClauseElement, Executable
from normlite.sql.schema import HasIdentifier
from normlite.sql.type_api import type_mapper

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

class ExecutableDDLStatement(Executable):
    """Base class for all DDL executable statements.
    
    .. versionadded:: 0.8.0
        This class is a convenient base class to set the :attr:`is_ddl` for all subclasses.
    """
    is_ddl = True

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
    """Represent a ``CREATE TABLE`` statement."""
    __visit_name__ = 'create_table'

    def __init__(self, table: Table):
        super().__init__()
        self.table = table
    
    def get_table(self) -> Table:
        return self.table
    
    def _post_exec(self, result: CursorResult, context: ExecutionContext):
        row = result.one()
        self.table.set_oid(row[SpecialColumns.NO_ID])
        for col in self.table.columns:
            col.set_oid(row[col.name])

class HasTable(HasIdentifier, ExecutableDDLStatement):
    """Represent a convenient pseudo DDL statement to check for table exsistence.

    :class:`HasTable` stores the object id of the table being checked, if this exists.
    This allows a subsequent execution of :class:`ReflectTable` to run as a simple database retrieve.
    
    .. versionadded:: 0.8.0
        Initial version does not support if exists logic.
    """
    __visit_name__ = 'has_table'

    def __init__(self, table_name: str, tables_id: str, table_catalog: str):
        self.table_name = table_name
        """The table name to search for."""

        self._tables_id = tables_id
        """The database id for the "tables" table."""    

        self._table_catalog = table_catalog
        """The database name (table_catalog)  this table belongs to."""

        self._found = None
        """``True`` if the looked for table was found."""

        self._oid = None
        """The object id of the looked for table."""

    def found(self) -> bool:
        """Return ``True`` if the table does exist."""

        return self._found
    
    def get_oid(self) -> str:
        return self._oid
    
    def set_oid(self, id_: str) -> None:
        raise NotImplementedError
    
    def _post_exec(self, result: CursorResult, context: ExecutionContext) -> None:
        try: 
            row = result.one()
            # IMPORTANT: Here the found database's id is stored in the table_id columns, 
            # since the returned row belongs to the tables table.
            self._oid = row['table_id']
            self._found = True

        except NoResultFound:
            # table does not exist
            self._found = False

        except MultipleResultsFound:
            # multiple tables with the same name were found
            raise NormliteError(f'Internal error. Found multiple occurrences of {self.table_name}')                

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


