# cursor.py
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
from typing import List, Tuple

from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql import CreateTable

class _CursorMetaData:
    """Provide helper metadata structures to access raw data from low level `Cursor` DBAPI 2.0."""
    def __init__(self, table_def: CreateTable):
        self.index_to_key = {i: col.name for i, col in enumerate(table_def.columns)}
        self.key_to_index = {col.name: i for i, col in enumerate(table_def.columns)}

class CursorResult:
    """Provide pythonic high level interface to result sets from SQL statements."""
    def __init__(self, cursor: Cursor, metadata: _CursorMetaData):
        self._cursor = cursor
        self._metadata = metadata

    def fetchall(self) -> List[Row]:
        raw_rows = self._cursor.fetchall()  # [[(colname, type, val), ...], ...]
        return [Row(self._metadata, row_data) for row_data in raw_rows]
    
    def execute(self) -> List[Row]:
        pass

class Row:
    """Provide pythonic high level interface to a single SQL database row."""
    def __init__(self, metadata: _CursorMetaData, row_data: List[Tuple[str, str, str]]):
        self._metadata = metadata
        self._values = [None] * len(metadata.index_to_key)

        # Reorder row_data according to index order from metadata
        key_to_value = {key: value for key, _, value in row_data}
        for idx, key in metadata.index_to_key.items():
            self._values[idx] = key_to_value.get(key)

    def __getitem__(self, key: str) -> str:
        idx = self._metadata.key_to_index[key]
        return self._values[idx]

    def __repr__(self):
        return f"Row({{ {', '.join(f'{k!r}: {self[k]!r}' for k in self._metadata.key_to_index)} }})"
