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
"""Provide SQL-like cross-row aggregate functions."""
from __future__ import annotations

from normlite.sql.elements import ColumnElement
from normlite.sql.schema import Column
from normlite.sql.type_api import Integer, TypeEngine

class _FuncNameSpace:
    """Function namespace exposed as :attr:`func` to users.
    
    .. versionadded:: 0.12.0
    """
    def count(self, colum: Column) -> Count:
        """Count distinct column values for rows belonging to the column's parent table.

        Args:
            colum (Column): Column to count values on. 

        Returns:
            Count: A new instance of :class:`Count`.
        """
        return Count(colum)

class FunctionElement(ColumnElement):
    __func_name__ = "func_element"
    column: Column

    def __init__(self, column: Column):
        self.name = column.name
        self.column = column

class Count(FunctionElement):
    __func_name__ = "count"
    __visit_name__ = "count"

    def __init__(self, column: Column) -> None:
        super().__init__(column)
        self.type_ = Integer()

func = _FuncNameSpace()