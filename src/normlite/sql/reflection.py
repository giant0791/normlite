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
from normlite.notion_sdk.getters import get_title
from normlite.sql.type_api import Boolean, ObjectId, String, TimeStampStringISO8601, TypeEngine, type_mapper

class ReflectedColumnInfo(NamedTuple):
    name: str
    type: TypeEngine
    id: Optional[str]
    value: Optional[Any]

class ReflectedTableInfo:
    def __init__(self, columns: Sequence[ReflectedColumnInfo]):
        self._colmap = {
            rc.name: index
            for index, rc in enumerate(columns)
        }

        self._columns = columns

    @property
    def name(self) -> str:
        return self._columns[self._colmap[SpecialColumns.NO_TITLE]].value
    
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
        return [rc for rc in self._columns if rc.name not in SpecialColumns.values()]
    
    def get_sys_columns(self) -> Sequence[ReflectedColumnInfo]:
        return [rc for rc in self._columns if rc.name in SpecialColumns.values()]
        
    def get_columns(self) -> Sequence[ReflectedColumnInfo]:
        return self._columns
    
    def get_column_names(self, include_all: Optional[bool] = True) -> Sequence[str]:
        if include_all:
            return [rc.name for rc in self._columns]
        else:
            return [rc.name for rc in self._columns if rc.name not in SpecialColumns.values()]
        
    @classmethod
    def from_tuples(cls, cols_as_tuples: Sequence[tuple]) -> ReflectedTableInfo:
        """
        Build a ReflectedTableInfo from a sequence of column-definition tuples.

        Each row must provide:
            (column_name, column_type, column_id, column_value)

        Special columns carry table-level metadata via column_value.
        """

        columns: list[ReflectedColumnInfo] = []

        for row in cols_as_tuples:
            col_name, col_type, col_id, col_value = row
            columns.append(
                ReflectedColumnInfo(
                    name=col_name,
                    type=col_type,
                    id=col_id,
                    value=col_value,
                )
            )

        # ---- validation (fail fast) ----
        # TODO

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
        ))

        database_name = get_title(database_obj)
        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_TITLE,
            type=String(is_title=True),
            id=None,
            value=database_name,
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_ARCHIVED,
            type=Boolean(),
            id=None,
            value=database_obj['archived']
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_IN_TRASH,
            type=Boolean(),
            id=None,
            value=database_obj['in_trash']
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_CREATED_TIME,
            type=TimeStampStringISO8601(),
            id=None,
            value=database_obj['created_time']
        ))

        # reflect properties
        for name, prop in database_obj["properties"].items():
            cols.append(
                ReflectedColumnInfo(
                    name=name,
                    type=type_mapper[prop["type"]],
                    id=prop["id"],
                    value=None,
                )
            )
        
        return cls(cols)

