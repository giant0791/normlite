# normlite/sql/__init__.py
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
"""SQL parsing, DML and DDL constructs, and schema storage and introspection.

The sql package defines the basic components for parsing and constructing DML/DDL constructs.
The primary "entry point" into SQL parsing is the :func:`normlite.sql.sql.text` constructor.
For DDL schema primitives, the :class:`normlite.sql.schema.Table` and its executable counter-part :class:`normlite.sql.ddl.CreateTable` 
are the building blocks to create Notion databases.
For DML statements, the main "entry point" is the :func:`normlite.sql.dml.insert` constructor.

.. deprecated:: 0.7.0
    The whole SQL parsing package is obsolete and thus deprecated. It will be refactored in a future version.
    **Do not use!**
"""


from .base import Compiled
from .dml import insert
from .dml import select
from .schema import Column 
from .schema import Table 
from .schema import MetaData
from .schema import Constraint 
from .schema import PrimaryKeyConstraint
from .reflection import ReflectedColumnInfo
from .reflection import ReflectedTableInfo
from .type_api import TypeEngine
from .type_api import Currency 
from .type_api import Number 
from .type_api import Integer 
from .type_api import Numeric 
from .type_api import Money 
from .type_api import String 
from .type_api import Boolean 
from .type_api import Date 

__all__ = [
    "Compiled",
    "insert",
    "select",
    "Column",
    "Table",
    "MetaData",
    "Constraint",
    "PrimaryKeyConstraint",
    "ReflectedColumnInfo",
    "ReflectedTableInfo",
    "TypeEngine",
    "Currency",
    "Number",
    "Integer",
    "Numeric",
    "Money",
    "String",
    "Boolean",
    "Date",
]