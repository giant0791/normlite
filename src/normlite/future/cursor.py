# normlite/future/cursor.py
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
from typing import Any, List, Mapping, NamedTuple, NoReturn, Sequence, Tuple, Union

from normlite.cursor import CursorResultMetaData
from normlite.notiondbapi.dbapi2 import Cursor
    
class _FrozenAttributeMixin:
    """A mixin that prevents setting or deleting attributes listed in self._frozen_attributes.
    
    Uses object-level access to avoid recursion during attribute handling. It is used by :class:`Row` to 
    implement read-only attribute for the row columns to access the corresponding values
    """

    __slots__ = ("_frozen_attributes",)

    def __init__(self, *, frozen_attributes: set[str] = None):
       object.__setattr__(self, "_frozen_attributes", frozen_attributes or set())

    def __setattr__(self, name: str, value: Any) -> NoReturn:
        if name in ['__id__', '__archived__', '__in_trash__'] or name in self._get_frozen_attributes():
            raise AttributeError(f'Cannot modify read-only attribute: {name}')
        
        elif name.startswith('_'):
            super().__setattr__(name, value)
        
        else:
            raise AttributeError(f'Cannot modify read-only attribute: {name}') 

    def __delattr__(self, name: str) -> NoReturn:
        if name in self._get_frozen_attributes():
            raise AttributeError(f'Cannot delete read-only attribute: {name}')
        
        object.__delattr__(name)

    def _get_frozen_attributes(self) -> set[str]:
        try:
            return object.__getattribute__(self, "_frozen_attributes")
        except AttributeError:
            # we're not frozen yet → allow attribute.
            return set()

    def __getattr__(self, name: str) -> NoReturn:
        try:
            metadata = object.__getattribute__(self, "_metadata")
            values = object.__getattribute__(self, "_values")
        except AttributeError:
            raise (f"{type(self).__name__} is not fully initialized")
        
        if name in metadata.keys:
            idx = metadata.key_to_index[name]
            return values[idx]
        
        raise AttributeError(f"{type(self).__name__!r} object has no attribute: {name!r}")

class Row(_FrozenAttributeMixin):
    """Provide pythonic high level interface to a single SQL database row."""
    
    def __init__(self, metadata: CursorResultMetaData, row_data: tuple):
        # Use object.__setattr__ to bypass __setattr__ during initialization
        object.__setattr__(self, "_metadata", metadata)
        """The metadata object to process raw rows."""
        
        object.__setattr__(self, "_values", list(row_data))
        """Thew column values."""

        super().__init__(frozen_attributes=set(metadata.keys))

    def __getitem__(self, key_or_index: Union[str, int]) -> Any:
        try:
            metadata = object.__getattribute__(self, "_metadata")
            values = object.__getattribute__(self, "_values")
        except AttributeError:
            raise AttributeError(f"{type(self).__name__} is not fully initialized")

        if isinstance(key_or_index, int):
            try:
                return values[key_or_index]
            except IndexError:
                raise IndexError(f"{type(self).__name__} index out of range: {key_or_index}")
            
        elif isinstance(key_or_index, str):
            try:
                return values[metadata.key_to_index[key_or_index]]
            except KeyError:
                raise KeyError(f"{type(self).__name__} has no column named {key_or_index!r}")
            
        else:
            raise TypeError(
                f"{type(self).__name__} indices must be str (column name) or int (column index), "
                f"not {type(key_or_index).__name__}"
            )
        
    def mapping(self) -> dict:
        return RowMapping(self)

    def __repr__(self):
        return f"Row({{ {', '.join(f'{k!r}: {self[k]!r}' for k in self._metadata.key_to_index)} }})"
