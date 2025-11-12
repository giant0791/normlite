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
from typing import TYPE_CHECKING

from normlite._constants import SpecialColumns
from normlite.cursor import CursorResult
from normlite.engine.context import ExecutionContext
from normlite.sql.base import ClauseElement, Executable

if TYPE_CHECKING:
    from normlite.sql.schema import Table, Column

class CreateTable(Executable):
    __visit_name__ = 'create_table'

    def __init__(self, table: Table):
        super().__init__()
        self.table = table
        self.columns = [
            CreateColumn(col) 
            for col in self.table.c 
            if not col.name.startswith('_no_')      # skip Notion-specific columns
        ]
    
    def get_table(self) -> Table:
        return self.table
    
    def _post_exec(self, result: CursorResult, context: ExecutionContext):
        row = result.one()
        self.table.set_oid(row[SpecialColumns.NO_ID])
        for col in self.table.columns:
            col.set_oid(row[col.name])


class CreateColumn(ClauseElement):
    __visit_name__ = 'create_column'

    def __init__(self, column: Column):
        super().__init__()
        self.column = column

