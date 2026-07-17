# sql/functions.py
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
from typing import Optional, Self

from normlite.exceptions import ArgumentError
from normlite.sql.base import generative
from normlite.sql.elements import ColumnElement
from normlite.sql.schema import Column
from normlite.sql.type_api import Float, Integer, Number, Numeric, TypeEngine

class _FuncNameSpace:
    """Function namespace exposed as :attr:`func` to users.
    
    .. versionadded:: 0.12.0
    """
    def count(self, column: Optional[Column] = None) -> Count:
        """Count column values for rows belonging to the column's parent table.

        Args:
            column (Column): Column to count values on. 

        Returns:
            Count: A new instance of :class:`Count`.
        """
        return Count(column)
    
    def sum(self, column: Column) -> Sum:
        """Sums column values for rows belonging to the column's parent table

        Args:
            column (Column): Column to sum values on.

        Raises:
            ArgumentError: If column's type is not a subclass of :class:`normlite.sql.type_api.Number`.

        Returns:
            Sum: A new instance of :class:`Sum`
        """
        return Sum(column)
    
    def avg(self, column: Column) -> Avg:
        """Averages column values for rows belonging to the column's parent table

        Args:
            column (Column): Column to average values on.

        Raises:
            ArgumentError: If column's type is not a subclass of :class:`normlite.sql.type_api.Number`.

        Returns:
            Sum: A new instance of :class:`Avg`
        """
        return Avg(column)

class FunctionElement(ColumnElement):
    column: Column
    key: str

    def __init__(self, func_name: str, column: Column):
        self.name = func_name
        self.column = column
        self.type_ = self._infer_return_type(column)

        # This is NOT yet the final result key
        self.key = func_name

    @generative
    def label(self, name: str) -> Self:
        self.key = name

    def _infer_return_type(self, column: Column) -> Optional[TypeEngine]:
        raise NotImplementedError(
            f"Subclasses of {type(self).__name__} must define this method"
        )

    def _raise_if_is_not_instance(
            self,
            type_: TypeEngine,
            required_type: type[TypeEngine],
            func_name: str,
        ) -> None:
        if not isinstance(type_, required_type):
            raise ArgumentError(
                f"Function {func_name}() expects column of type {required_type.__name__}, "
                f"got '{type(type_).__name__}'"
            )

class Count(FunctionElement):
    __visit_name__ = "count"

    def __init__(self, column: Optional[Column] = None) -> None:
        super().__init__("count", column)
    
    def _infer_return_type(self, column: Column) -> Optional[TypeEngine]:
        return Integer()
    
class Sum(FunctionElement):
    __visit_name__ = "sum"

    def __init__(self, column: Column):
        self._raise_if_is_not_instance(column.type_, Number, "sum")
        super().__init__("sum", column)

    def _infer_return_type(self, column: Column):
        return column.type_

class Avg(FunctionElement):
    __visit_name__ = "avg"

    def __init__(self, column: Column):
        self._raise_if_is_not_instance(column.type_, Number, "avg")
        super().__init__("avg", column)    

    def _infer_return_type(self, column: Column) -> Optional[TypeEngine]:
        return Float()

func = _FuncNameSpace()