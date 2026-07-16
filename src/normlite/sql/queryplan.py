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

from typing import Optional, Union

from normlite.notiondbapi.dbapi2 import Cursor

#: Notion's maximum (and default) result page size. A ``Scan`` pulls the store
#: one Notion page at a time, so this is the operator's batch granularity.
NOTION_MAX_PAGE_SIZE = 100


class Scan:
    def __init__(self, operation: dict, parameters: Union[dict, list[dict]]) -> None:
        self._operation = operation
        self._parameters = parameters
        self._cursor = None

    def open(self, cursor: Cursor) -> None:
        self._cursor = cursor
        self._cursor.execute(
            self._operation,
            self._parameters, 
            stream_results=True,
        )

    def next(self) -> Optional[list[tuple]]:
        next_batch = self._cursor.fetchmany(size=NOTION_MAX_PAGE_SIZE)
        return next_batch if next_batch else None
    
    def close(self) -> None:
        self._cursor.close()