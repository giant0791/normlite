# sql/reflection.py
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
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations
import pdb
from typing import Any, NamedTuple, Optional, Sequence
from normlite._constants import SpecialColumns
from normlite.notion_sdk import getters
from normlite.notion_sdk.getters import get_title
from normlite.sql.type_api import Boolean, ObjectId, String, TimeStampStringISO8601, TypeEngine, type_mapper
from normlite.utils import normlite_deprecated

class ReflectedColumnInfo(NamedTuple):
    name: str
    type: TypeEngine
    id: Optional[str]
    value: Optional[Any]
    is_system: bool
    """``True`` if the column is a system column.
    
    .. versionadded:: 0.9.0
        See refactoring in issue [#202](https://github.com/giant0791/normlite/issues/202)
    """

class ReflectedTableInfo:
    def __init__(self, columns: Sequence[ReflectedColumnInfo]):
        self._colmap = {
            rc.name: index
            for index, rc in enumerate(columns)
        }

        self._columns = columns

    @property
    def name(self) -> str:
        table_name = getters.rich_text_to_plain_text(
            self._columns[self._colmap[SpecialColumns.NO_TITLE]].value
        )
        return table_name
    
    @property
    def id(self) -> str:
        return self._columns[self._colmap[SpecialColumns.NO_ID]].value
    
    @property
    def archived(self) -> Optional[True]:
        return self._columns[self._colmap[SpecialColumns.NO_ARCHIVED]].value
    
    @property
    def in_trash(self) -> Optional[True]:
        return self._columns[self._colmap[SpecialColumns.NO_IN_TRASH]].value
    
    def get_user_columns(self) -> Sequence[ReflectedColumnInfo]:
        return [rc for rc in self._columns if not rc.is_system]
    
    def get_sys_columns(self) -> Sequence[ReflectedColumnInfo]:
        return [rc for rc in self._columns if rc.is_system]
        
    def get_columns(self) -> Sequence[ReflectedColumnInfo]:
        return self._columns
    
    @normlite_deprecated("This method is deprecated and will be removed in a future version.")
    def get_column_names(self, include_all: Optional[bool] = True) -> Sequence[str]:
        if include_all:
            return [rc.name for rc in self._columns]
        else:
            return [rc.name for rc in self._columns if not rc.is_system]
        
    @classmethod
    def from_tuples(cls, cols_as_tuples: Sequence[tuple]) -> ReflectedTableInfo:
        """
        Build a ReflectedTableInfo from a sequence of column-definition tuples.

        Each row must provide:
            ("column_name", "column_type", "column_id", "metadata", "is_system")

        Special columns carry table-level metadata via column_value.
        """

        columns: list[ReflectedColumnInfo] = []

        for row in cols_as_tuples:
            col_name, col_type, col_id, col_value, is_system = row
            columns.append(
                ReflectedColumnInfo(
                    name=col_name,
                    type=col_type,
                    id=col_id,
                    value=col_value,
                    is_system=is_system

                )
            )

        # ---- validation (fail fast) ----
        # TODO: see https://github.com/giant0791/normlite/issues/248

        return cls(columns=columns)

    @classmethod
    def from_dict(cls, database_obj: dict) -> ReflectedTableInfo:
        cols = []

        # special columns first
        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_ID,
            type=ObjectId(),
            id=None,
            value=database_obj['id'],
            is_system=True
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_TITLE,
            type=String(is_title=True),
            id=None,
            value=database_obj['title'],
            is_system=True
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_ARCHIVED,
            type=Boolean(),
            id=None,
            value=database_obj['archived'],
            is_system=True
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_IN_TRASH,
            type=Boolean(),
            id=None,
            value=database_obj['in_trash'],
            is_system=True
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_CREATED_TIME,
            type=TimeStampStringISO8601(),
            id=None,
            value=database_obj['created_time'],
            is_system=True
        ))

        # reflect properties
        for name, prop in database_obj["properties"].items():
            cols.append(
                ReflectedColumnInfo(
                    name=name,
                    type=type_mapper[prop["type"]],
                    id=prop["id"],
                    value=None,
                    is_system=False
                )
            )
        
        return cls(cols)

