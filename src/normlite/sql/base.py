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
from dataclasses import dataclass, field
from enum import Enum, auto
from functools import wraps
import json
import pdb
from typing import Any, ClassVar, NoReturn, Optional, Protocol, TYPE_CHECKING, Self, Sequence, overload
import copy

from normlite.engine.interfaces import _distill_params
from normlite.exceptions import ArgumentError, UnsupportedCompilationError
from normlite.utils import frozendict

if TYPE_CHECKING:
    from normlite.sql.schema import Table
    from normlite.sql.ddl import CreateTable, DropTable, ReflectTable
    from normlite.sql.dml import Insert, Select
    from normlite.sql.elements import ColumnElement, UnaryExpression, BinaryExpression, BindParameter, BooleanClauseList
    from normlite.engine.cursor import CursorResult
    from normlite.engine.interfaces import _CoreAnyExecuteParams, ExecutionOptions, ReturningStrategy
    from normlite.engine.base import Connection
    from normlite.engine.context import ExecutionContext
    from normlite.notiondbapi.dbapi2 import Error

class Generative:
    """Mixin providing SQLAlchemy-style generative behavior."""

    def _generate(self):
        """Return a shallow copy of this statement."""
        return copy.copy(self)

def generative(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        new = self._generate()
        fn(new, *args, **kwargs)
        return new
    return wrapper

class Visitable(ABC):
    """Base class for any AST node that can be "visited" by a compiler.

    It provides the :meth:`Visitable._compiler_dispatch()` method that delegates compilation
    to a compiler based on :attr:`__visit_name__`.

    .. versionchanged:: 0.7.0 
        This class has been completely redesigned to better separate
        concerns related to AST nodes, compilation, and compiled objects produced by the compilation.
    """
    __visit_name__: str
    """Class attribute used to denote the visit method to be called during compilation. """

    def _compiler_dispatch(self, compiler: SQLCompiler, **kwargs: Any) -> dict:
        """Delegate this node's compilation to the compiler.
        
        The compiler must implement a method named `visit_<__visit_name__>`.

        Args:
            compiler (SQLCompiler): The compiler object to be used for compilation.
            **kwargs (Any): Additional parameters to be passed to the compiler's visit method to allow context-sensitive compilation.

        Raises:
            UnsupportedCompilationError: If the instance owning this method is missing the :attr:`__visit_name__` or if it has no visit method.

        Returns:
            dict: The compilation result in form of a dictionary (JSON) object.
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
    
    def __repr__(self) -> str:
        return self.__class__.__name__

class ClauseElement(Generative, Visitable):
    """Base class for SQL elements that can be compiled by the compiler.

    This class orchestrates for all subclasses the compilation process by implementing the :meth:`compile`.
    Subclasses do not have to take care of the compilation.   
    
    .. versionadded:: 0.7.0
        In this version, the :meth:`compile` method supports standard compilation.
        The compilation process can be fine-tuned using keyword arguments for context-sensitive compilation
        (for example ``{"literal_binds": True}`` could be used to render the values in the compiled object *literalized* 
        instead of parameterized for logging or debugging purposes). 
        
        This feature will be added in a future version.
    """
    __visit_name__ = 'clause'

    def compile(self, compiler: SQLCompiler, **kwargs: Any) -> Compiled:
        """Compile this clause element.

        Args:
            compiler (SQLCompiler): The compiler to be used for the compilation.
            **kwargs (Any): Keyword arguments for context-sensitive compilation. **Not supported yet**.

        Returns:
            Compiled: The compiled object rusult of the compilation.
        """
        compiler._compiler_state = CompilerState()
        compiled_dict = compiler.process(self, **kwargs)
        if compiler._compiler_state.is_ddl:
            return DDLCompiled(self, compiled_dict, compiler)
        else:
            return Compiled(self, compiled_dict, compiler)

    def get_table(self) -> Table:
        """Return a collection of columns this clause element refers to."""
        raise NotImplementedError

class Executable(ClauseElement):
    """Provide the interface for all executable SQL statements.

    This base class implements the command design pattern for execution of clause elements.
    The :meth:`execute` method has a base implementation of the execution flow that can be used as default implementation
    in the subclasses.

    It provides a post execution hook with the :meth:`_post_exec` that is intended to be optionally implemented in 
    the subclasses.

    ..versionchanged: 0.8.0
        It introduces global context for compilation with `is_*` attributes.
    
    .. versionadded:: 0.7.0
        This base class fully supports the connection-driven execution flow of SQL statements.
    """

    supports_execution: bool = True
    """Structural flag denoting the main characteristic for an executable of supporting execution.
    
    ..versionadded:: 0.8.0
    """

    is_ddl: bool = False
    """``True`` if the executable subclass is a DDL statement.
    
    ..versionadded:: 0.8.0
    """

    _execution_options: Optional[ExecutionOptions] = None
    """per-statement execution options.
    
    .. versionadded:: 0.8.0
    """

    @overload
    def execution_options(
        self,
        *,
        returning_strategy: ReturningStrategy = "echo",
        preserve_rowcount: bool = False,            
        **opts: Any
    ) -> Connection:
        ...

    @overload
    def execution_options(self, **opts: Any) -> Connection:
        ...

    def execution_options(self, **opts: Any) -> Self:
        """Update the execution options **in-place** returning the same executable.
        
        .. versionadded:: 0.8.0
        """

        if 'isolation_level' in opts:
            raise ArgumentError(
                """
                    'isolation_level' execution option may only be specified 
                    on Connection.execution_options(), or 
                    per-engine using the isolation_level
                    argument to create_engine().
                """
            )
        
        if 'compiled_cache' in opts:
            raise ArgumentError(
                """
                    'compiled_cache' execution option may only be specified
                    on Connection.execution_options(), not per statement. 
                """
            )

        self._execution_options = (
            self._execution_options or {} 
            | frozendict(opts)
        )

        return self

    def get_execution_options(self) -> ExecutionOptions:
        """Return the execution options that will take effect during execution of this executable.
        
        .. versionadded:: 0.8.0

        .. seealso::
        
            :meth:`Engine.execution_execution_options`
        """
        return self._execution_options


    def _execute_on_connection(
            self, 
            connection: Connection, 
            params: Optional[_CoreAnyExecuteParams],
            *, 
            execution_options: Optional[ExecutionOptions] = None
    ) -> CursorResult:
        raise NotImplementedError
    
    def _setup_execution(self, context: ExecutionContext) -> None:
        """Per-statement internal hook to setup execution.
        
        Subclasses shall use this hook method to prepare the execution and
        store execution-related data in execution context.

        .. versionadded:: 0.8.0
        """
        raise NotImplementedError
    
    def _finalize_execution(self, context: ExecutionContext) -> None:
        """Per-statement internal hook to finalize execution.
        
        Subclasses shall use this hook method to finalize results and
        store execution-related data in execution context.

        .. versionadded:: 0.8.0
        """
        raise NotImplementedError
    
    def _handle_dbapi_error(
        self, 
        exc: Error, 
        context: ExecutionContext
    ) -> Optional[NoReturn]:
        """Handle the DBAPI error that may occur during execution.

        Args:
            exc (Error): The error to be handled
            context (ExecutionContext): The execution context in which this executables is running.

        Returns:
            Optional[NoReturn]: Nothin
        """
        raise NotImplementedError

class _CompileState(Enum):
    NOT_STARTED            = auto()
    COMPILING_VALUES       = auto()
    COMPILING_WHERE        = auto()
    COMPILING_DBAPI_PARAM  = auto()

@dataclass
class CompilerState:
    # statement being compiled
    stmt: ClauseElement = None

    # statement kind flags
    is_ddl: bool = False
    is_dml: bool = False
    is_select: bool = False
    is_insert: bool = False
    is_update: bool = False
    is_delete: bool = False

    # location flags
    in_where: bool = False

    # bind handling
    execution_binds: dict[str, BindParameter] = field(
        default_factory=dict
    )
    """Bind parameters to be evaluated at execution time."""

    # result metadata
    result_columns: Optional[list[str]] = None

    # compiler phase
    compile_state: _CompileState = _CompileState.NOT_STARTED
 
class Compiled:
    """The result of compiling :class:`ClauseElement` subclasses.

    This class provides an abstraction for compilation results.

    .. versionchanged:: 0.8.0
        This version introduces the :attr:`is_ddl` class attribute. 

    .. versionadded:: 0.7.0
        This version implements the main basic functions for a compiled object.
    """

    is_ddl: ClassVar[bool] = False
    """``True`` if the compiled class represents a compiled DDL statement.
    
    .. versionadded:: 0.8.0
        This class attribute is used by :class:`normlite.cursor.CursorResult` to properly process the cursor description and the result set.
    """

    def __init__(self, element: ClauseElement, compiled: dict, compiler: SQLCompiler):
        self._element = element
        """The compiled clause element."""

        self._compiled = compiled
        """The dictionary containing the compilation result."""
        
        # IMPORTANT: The _compiler_state.execution_binds contains the mapping that associates
        # a bind param key to a tuple[BindParameter, str], where str is the usage="value" | "filter"
        # This is used by the ExecutionContext to choose the right bind processor (bind_processor | filter_processor)
        self._execution_binds = dict(compiler._compiler_state.execution_binds)
        """The bind parameters for this compiled object."""

        self._result_columns = compiler._compiler_state.result_columns
        """Optional sequence of strings specifying the column names to be considered 
        in the rows produced by the :class:`normlite.cursor.CursorResult` methods.
        """

        self._compiler_state = compiler._compiler_state

    @property
    def string(self) -> str:
        """Provide a linted string of this compiled object."""
        return json.dumps(self._compiled, indent=2)
    
    @property
    def params(self) -> dict:
        """Provide the bind parameters for this compiled object."""
        return self._execution_binds
    
    def as_dict(self) -> dict:
        """Return this compiled object in the original dictionary form."""
        return self._compiled
    
    def result_columns(self) -> Optional[Sequence[str]]:
        """Optionally return the column names for the cursor result."""
        return self._result_columns
    
    def __str__(self) -> str:
        return self.string
    
    def __repr__(self):
        return f"Compiled {self._element.__class__.__name__}"
    
class DDLCompiled(Compiled):
    is_ddl: ClassVar[bool] = True

class SQLCompiler(Protocol):
    """Base class for SQL compilers.

    This class defines the standard interface for SQL compilers.
    Concrete compilers take a :class:`Visitable` object and generate executable code.

    .. seealso::
        :class:`normlite.sql.compiler` for a Notion specific compiler. 

    .. versionadded:: 0.7.0
        This version supports compilation of ``CREATE TABLE`` and ``INSERT`` statements.   
    """

    _compiler_state: CompilerState

    def process(self, element: ClauseElement, **kwargs: Any) -> dict:
        """Entry point for the compilation process.

        This method is called by :meth:`ClauseElement.compile` to produce a :class:`Compiled` object,
        which stores the compilation result.

        Note:
            The return type dict allows to model the compilation result as a JSON object (Python dictionary).
            This is very flexible as it enables the implementation of compilers, 
            which have to provide specific downstream methods for representing their own generated code.
            For example, the :class:`normlite.sql.compiler.NotionCompiler` class generates Notion payload objects for
            several Notion API requests.

        Returns:
            dict: The JSON object representing the compilation result.
        """
        return element._compiler_dispatch(self, **kwargs)

    def visit_create_table(self, table: CreateTable) -> dict:
        """Compile a table (DDL ``CREATE TABLE``)."""
        ...

    def visit_drop_table(self, ddl_stmt: DropTable) -> dict:
        """Compile the pseudo DDL statement for checking for table existence."""
        ...

    def visit_reflect_table(self, reflect_table: ReflectTable) -> dict:
        """Compile the DDL statement for reflecting an existing table."""

    def visit_insert(self, insert: Insert) -> dict:
        """Compile an insert statement (DML ``INSERT``)."""
        ...

    def visit_select(self, select: Select) -> dict:
        ...

    def visit_column_element(self, column: ColumnElement) -> dict:
        ...

    def visit_unary_expression(self, expression: UnaryExpression) -> dict:
        ...
 
    def visit_binary_expression(self, expression: BinaryExpression) -> dict:
        ...

    def visit_boolean_clause_list(self, expression: BooleanClauseList) -> dict:
        ...
