# normlite/__init__.py
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

from normlite.engine import create_engine
from normlite.engine import Engine
from normlite.engine import Connection
from normlite.engine import Inspector
from normlite.engine import IsolationLevel
from normlite.engine import ExecutionOptions
from normlite.engine import SystemTablesEntry
from normlite.engine import CursorResult
from normlite.engine import Row
from normlite.engine import RowMapping

from normlite.sql import Compiled
from normlite.sql import insert
from normlite.sql import select
from normlite.sql import delete
from normlite.sql import Column 
from normlite.sql import Table 
from normlite.sql import MetaData
from normlite.sql import Constraint 
from normlite.sql import PrimaryKeyConstraint
from normlite.sql import TypeEngine
from normlite.sql import Currency 
from normlite.sql import Number 
from normlite.sql import Integer 
from normlite.sql import Numeric 
from normlite.sql import Money 
from normlite.sql import String 
from normlite.sql import Boolean 
from normlite.sql import Date 
from normlite.sql import ReflectedColumnInfo
from normlite.sql import ReflectedTableInfo
from normlite.sql import DateTimeRange

from .exceptions import NormliteError
from .exceptions import NoResultFound
from .exceptions import MultipleResultsFound
from .exceptions import DuplicateColumnError
from .exceptions import ArgumentError
from .exceptions import InvalidRequestError
from .exceptions import UnsupportedCompilationError
from .exceptions import ResourceClosedError
from .exceptions import CompileError
from .exceptions import ObjectNotExecutableError
from .exceptions import NoSuchTableError
from .exceptions import StatementError


__all__ = [
    # engine
    "create_engine",
    "Connection",
    "Engine",
    "Inspector",
    "IsolationLevel",
    "ExecutionOptions",
    "ReflectedColumnInfo",
    "ReflectedTableInfo",
    "SystemTablesEntry",
    "CursorResult",
    "Row",
    "RowMapping",

    # sql
    "Compiled",
    "insert",
    "select",
    "delete",
    "Column",
    "Table",
    "MetaData",
    "Constraint",
    "PrimaryKeyConstraint",
    "TypeEngine",
    "Currency",
    "Number",
    "Integer",
    "Numeric",
    "Money",
    "String",
    "Boolean",
    "Date",
    "DateTimeRange",

    # exceptions
    "NormliteError",
    "NoResultFound",
    "MultipleResultsFound",
    "DuplicateColumnError",
    "ArgumentError",
    "InvalidRequestError", 
    "UnsupportedCompilationError",
    "ResourceClosedError",
    "CompileError",
    "ObjectNotExecutableError",
    "NoSuchTableError",
    "StatementError",
]