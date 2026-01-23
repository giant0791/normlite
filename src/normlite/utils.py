# normlite/utils.py
# Copyright (C) 2026 Gianmarco Antonini
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
from collections.abc import Mapping
from typing import Iterator, Hashable, Any


class frozendict(Mapping):
    __slots__ = ("_data", "_hash")

    def __init__(self, *args, **kwargs):
        """
        frozendict()
        frozendict(mapping)
        frozendict(iterable)
        frozendict(**kwargs)
        frozendict(frozendict)
        """
        if len(args) > 1:
            raise TypeError(
                f"frozendict expected at most 1 argument, got {len(args)}"
            )

        if args:
            src = args[0]
            if isinstance(src, frozendict):
                # Fast path: reuse internal storage
                self._data = src._data
                self._hash = src._hash
                return
            else:
                data = dict(src)
        else:
            data = {}

        if kwargs:
            data.update(kwargs)

        self._data = data
        self._hash = None

    # ------------------------------------------------------------------
    # Mapping interface
    # ------------------------------------------------------------------

    def __getitem__(self, key: Hashable) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"frozendict({self._data!r})"

    # ------------------------------------------------------------------
    # Equality
    # ------------------------------------------------------------------

    def __eq__(self, other) -> bool:
        if isinstance(other, Mapping):
            return dict(self._data) == dict(other)
        return NotImplemented

    # ------------------------------------------------------------------
    # Hashing (only if all values are hashable)
    # ------------------------------------------------------------------

    def __hash__(self) -> int:
        if self._hash is None:
            try:
                items = frozenset(self._data.items())
            except TypeError as exc:
                raise TypeError(
                    "unhashable frozendict: one or more values are unhashable"
                ) from exc
            self._hash = hash(items)
        return self._hash

    # ------------------------------------------------------------------
    # Union operators (PEP 584)
    # ------------------------------------------------------------------

    def __or__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        merged = dict(self._data)
        merged.update(other)
        return frozendict(merged)

    def __ror__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        merged = dict(other)
        merged.update(self._data)
        return frozendict(merged)

    def __ior__(self, other):
        # |= must return a *new* frozendict
        return self | other

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def copy(self, **updates):
        """Return a new frozendict with optional updates."""
        if not updates:
            return self
        data = dict(self._data)
        data.update(updates)
        return frozendict(data)
