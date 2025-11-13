# sql/base.py
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
from abc import ABC
import json
import pdb
from typing import Any, Optional, Protocol, TYPE_CHECKING, Sequence

from normlite.exceptions import UnsupportedCompilationError
from normlite.engine.context import ExecutionContext

if TYPE_CHECKING:
    from normlite.sql.schema import ReadOnlyColumnCollection
    from normlite.sql.ddl import CreateTable, CreateColumn
    from normlite.sql.dml import Insert
    from normlite.cursor import CursorResult
    from normlite.notiondbapi.dbapi2 import Cursor

class Visitable(ABC):
    """Base class for any AST node that can be 'visited' by a compiler.

    It provides the :meth:`Visitable._compiler_dispatch()` method that delegates compilation
    to a compiler based on :attr:`__visit_name__`.

    .. versionchanged:: 0.7.0 This class has been completely redesigned to better separate
        concerns related to AST nodes, compilation, and compiled objects produced by the compilation.
    """
    __visit_name__: str
    """Class attribute used to denote the visit method to be called during compilation. """

    def _compiler_dispatch(self, compiler: SQLCompiler, **kwargs: Any) -> dict:
        """Delegate this node's compilation to the compiler.
        
        The compiler must implement a method named `visit_<__visit_name__>`.

        Args:
            compiler (SQLCompiler): The compiler object to be used for compilation
            kwargs (Any): Additional parameters to be passed to the compiler's visit method.

        Raises:
            UnsupportedCompilationError: If the instance owning this method is missing the :attr:`__visit_name__` or if it has no visit method.

        Returns:
            dict: The compilation result in form of a dictionary (JSON) object
        """
        visit_name = getattr(self, '__visit_name__', None)

        if not visit_name:
            raise UnsupportedCompilationError(
                f"{self.__class__.__name__} is missing '__visit_name__' attribute."
            )

        visit_fn = getattr(compiler, f"visit_{visit_name}", None)
        if not callable(visit_fn):
            raise UnsupportedCompilationError(
                f"{compiler.__class__.__name__} has no method visit_{visit_name}() "
                f"for {self.__class__.__name__}"
            )

        return visit_fn(self, **kwargs)

class ClauseElement(Visitable):
    """Base class for SQL elements that can be compiled by the compiler.
    
    .. versionadded: 0.7.0
    """
    __visit_name__ = 'clause'

    def compile(self, compiler: SQLCompiler, **kwargs: Any) -> Compiled:
        compiled_dict = compiler.process(self, **kwargs)
        result_columns = compiled_dict.get('result_columns', None)
        if result_columns:
            compiled_dict.pop('result_columns')

        return Compiled(self, compiled_dict, result_columns)

    def get_table(self) -> ReadOnlyColumnCollection:
        raise NotImplementedError

class Executable(ClauseElement):
    """Provide the interface for all executable SQL statements.
    
    .. versionadded:: 0.7.0
    
    """

    def execute(self, context: ExecutionContext, parameters: Optional[dict] = None) -> CursorResult:
        """Run this executable within the context setup by the connection."""

        # TODO: for INSERT/UPDATE statements that do not have values, parameters is not None
        # Implement this use case and bind the supplied parameters
        # Suppor the same SqlAlchemy convention that parameters override the values() clause.
        cursor = context._dbapi_cursor
        compiled = context._compiled
        cursor.execute(compiled.as_dict()['operation'], compiled.params)
        result = context._setup_cursor_result(cursor)
        self._post_exec(result, context)
        return result

    def _post_exec(self, result: CursorResult, context: ExecutionContext) -> None:
        """Optional hook for subclassess."""
        ...

class Compiled:
    """The result of compiling :class:`ClauseElement` subclasses.

    .. versionadded:: 0.7.0

    """

    def __init__(self, element: ClauseElement, compiled: dict, result_columns: Optional[Sequence[str]] = None):
        self._element = element
        self._compiled = compiled
        self._result_columns = result_columns

    @property
    def string(self) -> str:
        return json.dumps(self._compiled, indent=2)
    
    @property
    def params(self) -> dict:
        return self._compiled['parameters']
    
    def as_dict(self) -> dict:
        return self._compiled
    
    def result_columns(self) -> Optional[Sequence[str]]:
        return self._result_columns
    
    def __str__(self) -> str:
        return self.string
    
    def __repr__(self):
        return f"Compiled {self.element.__class__.__name__}"


class SQLCompiler(Protocol):
    def process(self, element: ClauseElement, **kwargs: Any) -> dict:
        return element._compiler_dispatch(self, **kwargs)

    def visit_create_table(self, table: CreateTable) -> dict:
        ...

    def visit_create_column(self, column: CreateColumn) -> dict:
        ...

    def visit_insert(self, insert: Insert) -> dict:
        ...
