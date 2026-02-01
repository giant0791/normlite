# normlite/exceptions.py
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

"""Provide ``normlite`` specific exceptions.

.. versionadded:: 0.5.0
"""

class NormliteError(Exception):
    """Base exception class for all ``normlite`` exceptions.
    
    .. versionadded: 0.5.0
    """
    ...

class NoResultFound(NormliteError):
    """Raised when exactly one result row is expected, but none was found.
    
    .. versionadded: 0.5.0
    """
    ...

class MultipleResultsFound(NormliteError):
    """Raised if multiple rows were found when exactly one was required.
    
    .. versionadded: 0.5.0
    """
    ...

class DuplicateColumnError(NormliteError):
    """Raised when an already existing column is added to a table.
    
    .. versionadded:: 0.7.0
    """
    ...

class ArgumentError(NormliteError):
    """Raised when an erroneous argument is passed.
    
    .. versionadded:: 0.7.0
    """
    ...

class InvalidRequestError(NormliteError):
    """Raised when a ``normlite`` method or function cannot perform as requested.
    
    .. versionadded:: 0.7.0
    """
    ...

class UnsupportedCompilationError(NormliteError):
    """Raised when an operation is not supported by the given compiler.
    
    .. versionadded:: 0.7.0
    """

class ResourceClosedError(NormliteError):
    """The cursor cannot deliver rows.
    
    This exception is raised when a cursor (resource) cannot return rows because it is
    either closed (i.e. exhausted) or it represents the result from an SQL statement that 
    does not return values (e.g. ``INSERT``).

    .. versionadded:: 0.5.0

    """

class CompileError(NormliteError):
    """Raised when an error occurs during SQL compilation.

    .. versionadded:: 0.8.0
    """

class ObjectNotExecutableError(ArgumentError):
    """Raised when an object is passed to .execute() that can't be
    executed as SQL.

    .. versionadded:: 0.8.0
    """