# notiondbapi/dbapi2_consts.py
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
"""
Provide constants and definitions for the PEP 249 DBAPI compliant ``normlite`` implementation.

.. versionadded:: 0.8.0
 
"""
from enum import StrEnum


class DBAPITypeCode(StrEnum):
    """Enum for type codes used in cursor descriptions.
    
    .. versionadded:: 0.8.0
        With this enum the module :mod:`dbapi.py` is more PEP 249 compliant.
    """
    ID                 = 'object_id'
    TITLE              = 'title'
    CHECKBOX           = 'checkbox'
    NUMBER             = 'number'
    NUMBER_WITH_COMMAS = 'number_with_commas'
    NUMBER_DOLLAR      = 'dollar'
    RICH_TEXT          = 'rich_text'
    DATE               = 'date'



