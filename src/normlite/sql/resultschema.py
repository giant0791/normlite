# sql/resultschema.py
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
"""Provide logical result-set representation (coming from compiled SQL).

.. note::
    In this version, the schema for the result-set faithfully represent
    **all** columns.
    Currently supported Notion object properties are:

    * "id" - string (UUIDv4), unique identifier of the page.
    * "created_time" - Python datetime normalized string from ISO 8601 ("+Z" replaced by "+0:00"),
      date and time when this page was created. 
    * "last_edited_time" - Python datetime normalized string from ISO 8601 ("+Z" replaced by "+0:00"),
      date and time when this page was updated. 
    * "archived" - boolean, backward compatible alias for "in_trash".
    * "in_trash" - boolean, whether the page has been trashed.

.. versionadded:: 0.9.0
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Sequence

from normlite._constants import SpecialColumns
from normlite.exceptions import ArgumentError, InvalidRequestError, NoSuchColumnError
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.schema import Table

@dataclass(frozen=True)
class ResultColumn:
    """Represent one column in the result set."""
    name: str
    type_code: DBAPITypeCode
    nullable: bool

_SYSCOL_SCHEMA = {
    SpecialColumns.NO_ID.value: ResultColumn(
        name=SpecialColumns.NO_ID, 
        type_code=DBAPITypeCode.ID, 
        nullable=None
    ),
    SpecialColumns.NO_ARCHIVED.value: ResultColumn(
        name=SpecialColumns.NO_ARCHIVED, 
        type_code=DBAPITypeCode.ARCHIVAL_FLAG, 
        nullable=None
    ),
    SpecialColumns.NO_IN_TRASH.value: ResultColumn(
        name=SpecialColumns.NO_IN_TRASH, 
        type_code=DBAPITypeCode.ARCHIVAL_FLAG, 
        nullable=None
    ),
    SpecialColumns.NO_CREATED_TIME.value: ResultColumn(
        name=SpecialColumns.NO_CREATED_TIME, 
        type_code=DBAPITypeCode.TIMESTAMP, 
        nullable=None
    ),
}

@dataclass(frozen=True)
class SchemaInfo:
    """Represent all comuns in the result set (incl. special ones)."""
    columns: Sequence[ResultColumn]
    _index_map: Optional[Dict[str, int]] = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )


    @classmethod
    def from_table(
        cls,
        table: Table,
        *,
        projected_sys_names: Optional[Sequence[str]] = None,
        projected_usr_names: Optional[Sequence[str]] = None,
    ) -> SchemaInfo:
        """Build schema information from a :class:`normlite.sql.schema.Table`.

        The schema information **always** contains all columns: system columns **and** user defined.

        Args:
            table (Table): The table representive the authoritative source of the schema.
            projected_sys_names (Optional[Sequence[str]]): The ordered projection list of system columns (Notion key values).
            projected_usr_names (Optional[Sequence[str]]): The ordered projection list of user columns (Notion properties)
                coming from :attr:`normlite.engine.context.ExecutionContext.compiled._result_columns`.

        Raises:
            NoSuchColumnError: If any of the column names in ``projection_names`` could not be found in the table.

        Returns:
            SchemaInfo: A new schema information instance.

        .. versionadded:: 0.9.0
        """

        # always initialize the result_columns with sys cols
        result_columns = []
        if projected_sys_names is not None:
            result_columns = [
                _SYSCOL_SCHEMA[sysc.name] 
                for sysc in table._sys_columns
                if sysc.name in projected_sys_names
            ]

        else:
            # always return all columns if projection is empty
            result_columns = [
                col
                for col in _SYSCOL_SCHEMA.values()
            ]

        if projected_usr_names is not None:
            for name in projected_usr_names:

                # Table-declared columns
                try:
                    column = table.c[name]
                except KeyError:
                    raise NoSuchColumnError(
                        f"Column '{name}' not found in table '{table.name}'."
                    )

                result_columns.append(
                    ResultColumn(
                        name=name,
                        type_code=column.type_.get_dbapi_type(),
                        nullable=None,
                    )
                )
        else:
            for column in table.c:
                result_columns.append(
                    ResultColumn(
                        name=column.name,
                        type_code=column.type_.get_dbapi_type(),
                        nullable=None,
                    )
                )

        return cls(tuple(result_columns))
    
    def as_sequence(self) -> Sequence[tuple]:
        """Provide the description for DBAPI cursors.
        
        This method is the official API for :class:`SchemaInfo`.
        """
        entries = []

        for col in self.columns:
            entry = (
                col.name,
                col.type_code,      
                None,                        # display_size
                None,                        # internal_size
                None,                        # precision
                None,                        # scale
                col.nullable,                # col.nullable for future versions.
            )

            entries.append(entry)

        return tuple(entries)

    def _ensure_index_map(self) -> None:
        if self._index_map is None:
            index_map = {
                col.name: idx
                for idx, col in enumerate(self.columns)
            }
            object.__setattr__(self, "_index_map", index_map)

    def column_index(self, name: str) -> int:
        if not self.columns:
            raise InvalidRequestError(
                "Cannot provide index on empty or uninitialized SchemaInfo object."
            )

        self._ensure_index_map()

        try:
            return self._index_map[name]
        except KeyError:
            raise NoSuchColumnError(f"Column: '{name}'")
        
    def column_getter(self, name: str) -> Callable[[Sequence[Any]], Any]:
        idx = self.column_index(name)

        def getter(row: Sequence[Any]) -> Any:
            return row[idx]

        return getter