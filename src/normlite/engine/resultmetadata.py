# normlite/resultmetadata.py
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
import pdb
from typing import ClassVar, Mapping, NamedTuple, NoReturn, Optional, Sequence

from normlite.exceptions import ResourceClosedError

class _CursorColMapRecType(NamedTuple):
    """Helper record data structure to store column metadata.
    
    This class provides a description record to enable value and type conversions between DBAPI 2.0 rows and 
    higher level class:`Row` objects.

    .. version-removed:: 0.8.0
        The refactored :class:`_CursorColMapRecType` does not store the column ids anymore.
        These are delivered in the row data returned by the :class:`normlite.notiondbapi.dbapi2.Cursor` fetcher methods.
    
    .. versionchanged:: 0.7.0 
        This version adds support column identifiers.

    .. versionadded:: 0.5.0

    """
    column_name: str
    """The name of the column."""
    
    index: int
    """The column position in the description (first column --> index = 0)."""

    column_type: str
    """Currently, a string denoting the column type for conversion in the Python type system."""

_ColMapType = dict

class _NoCursorResultMetadata:
    returns_row: ClassVar[bool] = False
    """``True`` if the CursorResult returns row.
    This class attribute is set by the subclasses.
    """

    def _raise_error(self) -> NoReturn:
        raise ResourceClosedError(
            'This result object does not return rows or schemas. '
            'It has been automatically closed.'
        )
    
    @property
    def keys(self) -> Sequence[str]:
        self._raise_error()

    @property
    def key_to_index(self) -> Mapping[str, int]:
        self._raise_error()

    @property
    def index_for_key(self) -> Mapping[int, str]:
        self._raise_error()

# sentinel value for closed cursor results
_NO_CURSOR_RESULT_METADATA = _NoCursorResultMetadata()

class CursorResultMetaData(_NoCursorResultMetadata):
    """Provide helper metadata structures to access row data from low level 
    :class:`normlite.notionbdapi.dbapi2.Cursor` DBAPI 2.0.

    .. versionchanged:: 0.8.0
        This version now provides full support for constructing and accessing :class:`Row` objects with a subset of columns 
        as specified by the argument ``result_columns``.
        
        .. seealso::
            
            :mod:`normlite.notiondbapi.compiler.py` module
                Documentation explaining how retrieved Notion database objects are compiled.

    .. versionchanged:: 0.7.0 
        This version supports column identifiers.
    
    .. versionadded:: 0.5.0

    """
    
    returns_row = True
    """The associated cursor returns rows (e.g. ``SELECT`` statement)."""

    def __init__(
            self, 
            desc: Sequence[tuple], 
            is_ddl: bool,
            result_columns: Optional[Sequence[str]] = None
        ):
        self._full_map: _ColMapType = dict()
        """Mapping between column name and its description record :class:`_CursorColMapRecType` as described by the DBAPI description"""
        
        self._colmap: _ColMapType = dict()
        """Mapping representing the projected view on :attr:`_full_map` considering the result columns.
        
        .. versionadded: 0.8.0
            The projection map stores DBAPI indexes as :attr:`_full_map`, thus it guarantees index correctness by construction. 
        """

        self._is_ddl = is_ddl
        self._full_colmap = {
            col_name: _CursorColMapRecType(col_name, index, column_type)
            for index, (col_name, column_type, _, _, _, _, _) in enumerate(desc)
        }

        if result_columns is None:
            # the projected view is the same as the DBAPI truth
            self._colmap = self._full_colmap
        else:
            # The projection map is constructed using _full_map by accessing the records by key.
            # This ensures that the projection map uses the same DBAPI indexes as _full_map
            self._colmap = {
                name: self._full_colmap[name]
                for name in result_columns
                if name in self._full_colmap
            }

        self._key_to_index: Mapping[str, int] = dict()
        """Mapping between column name and its positional index."""
       
        self._key_to_index = {
            key: rec.index
            for key, rec in self._colmap.items()
        } 
        
        self._index_for_key: Mapping[int, str] = dict()
        """Mapping between column positional index and its name."""
        
        self._index_for_key = {
            rec.index: key
            for key, rec in self._colmap.items()
        }
 
        self._keys: Sequence[str] = [key for key in self._colmap.keys()]
        """A sequence containing all the column names."""

    @property
    def keys(self) -> Sequence[str]:
        """Provide all the column names for the described row."""
        return self._keys

    @property
    def key_to_index(self) -> Mapping[str, int]:
        """Provide the mapping between column name and its positional index."""
        return self._key_to_index
    
    @property
    def index_for_key(self) -> Mapping[int, str]:
        """Provid the mapping beween the positional index of a column and its name."""
        return self._index_for_key
    
    @property
    def is_ddl(self) -> bool:
        return self._is_ddl