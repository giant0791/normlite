# normlite/_constants.py
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
"""Provide library-wide constants.

This module implements constants to be used in other modules of ``normlite``.

.. versionadded:: 0.7.0

"""
from enum import StrEnum

class SpecialColumns(StrEnum):
    """Define enum constants for column names to access Notion-specific columns ("special columns").

    .. versionchanged:: 0.8.0
        This enum class adds parent id as important attribute for Notion pages and databases, so that these ids are
        readily available in their ``normlite`` counterparts.
    
    .. versionadded:: 0.7.0

    """
    NO_ID = "_no_id"
    """Notion "id" key for all objects."""

    NO_PID = "_no_parent_id"
    """Parent identifier for a Notion entity.
    
    The parent identifier is the "database_id" key for pages belonging to a database and
    the "page_id" for databases.

    .. versionadded:: 0.8.0
    
    """

    NO_TITLE = "_no_title"
    """Notion "title" key for database objects."""
    
    NO_ARCHIVED = "_no_archived"
    """Notion "archived" key for all objects."""
    
    NO_IN_TRASH = "_no_in_trash"
    """Notion "in_trash" key for all objects."""

    @classmethod
    def values(cls) -> tuple[str, ...]:
        """Provide all constants values as tuple.
        
        Helper class method for implementing is in tests.

        Example::

            >>> '_no_in_trash' in SpecialColumns.values()
            True

            >>> '_no_not_exists' in SpecialColumns.values()
            False

        """
        return tuple(m.value for m in cls)
