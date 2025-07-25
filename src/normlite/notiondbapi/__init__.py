# notiondbapi/__init__.py
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

from .dbapi2 import (
    DBAPIParamStyle, 
    Error, 
    InterfaceError,
    DatabaseError,
    InternalError,
    Cursor
) 

__all__ = [
    'DBAPIParamStyle',
    'Error',
    'InterfaceError',
    'DatabaseError',
    'InternalError',
    'Cursor'
]