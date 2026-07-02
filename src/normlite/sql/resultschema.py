# sql/resultschema.py
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
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""Provide logical result-set representation (coming from compiled SQL).

.. note::
    In this version, the schema for the result-set faithfully represent
    **all** columns.
    Currently supported Notion object properties are:

    * "id" - string (UUIDv4), unique identifier of the page.
    * "created_time" - Python datetime normalized string from ISO 8601 ("+Z" replaced by "+0:00"),
      date and time when this page was created. 
    * "last_edited_time" - Python datetime normalized string from ISO 8601 ("+Z" replaced by "+0:00"),
      date and time when this page was updated. 
    * "archived" - boolean, backward compatible alias for "in_trash".
    * "in_trash" - boolean, whether the page has been trashed.

.. versionadded:: 0.9.0
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence
from normlite._constants import SpecialColumns
from normlite.exceptions import InvalidRequestError, NoSuchColumnError
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.functions import FunctionElement
from normlite.sql.schema import Column, Table

def _merge_names(
    execution_names: Optional[Sequence[str]],
    projected_names: Optional[Sequence[str]],
) -> List[str]:
    """Merge execution and projected column names into a single ordered list.

    Rules:
    - execution_names are included first (internal requirements)
    - projected_names follow (user projection)
    - duplicates are removed while preserving order
    - None is treated as an empty sequence

    Args:
        execution_names: Internal names required for execution.
        projected_names: User-requested names.

    Returns:
        List[str]: Ordered, de-duplicated sequence of names.
    """
    seen = set()
    result: List[str] = []

    for source in (execution_names or [], projected_names or []):
        for name in source:
            if name not in seen:
                seen.add(name)
                result.append(name)

    return result

@dataclass(frozen=True)
class ResultColumn:
    """Represent one column in the result set.

    ``name`` is the merged result key (qualified on a join collision, e.g.
    ``courses.title``). ``table`` and ``bare_name`` carry the column's
    *provenance* -- the owning table and its original, unqualified name --
    so the join pipeline can select columns by identity rather than by name,
    which is exactly what collisions break (see ADR-0009). Provenance is an
    invariant set by both :meth:`SchemaInfo.from_table` and
    :meth:`SchemaInfo.from_join`; it is excluded from equality and ``repr`` so
    the DBAPI ``description`` (:meth:`SchemaInfo.as_sequence`) is unchanged.
    """
    name: str
    type_code: DBAPITypeCode
    nullable: bool
    bare_name: str = field(compare=False, repr=False)
    table: Optional[Table] = None


@dataclass(frozen=True)
class SchemaInfo:
    """Represent all comuns in the result set (incl. special ones)."""
    columns: Sequence[ResultColumn]
    _index_map: Optional[Dict[str, int]] = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )

    @classmethod
    def from_table(
        cls,
        table: Table,
        *,
        execution_names: Optional[Sequence[str]] = None,
        projected_names: Optional[Sequence[str]] = None,
    ) -> SchemaInfo:
        """Build schema information from a :class:`normlite.sql.schema.Table`.

        The schema information merges the columns specified in the ``execution_names`` and ``projected_names``.

        Args:
            table (Table): The table representive the authoritative source of the schema.
            execution_names (Optional[Sequence[str]]): The ordered projection list of system columns (Notion key values) required for statement execution.
            projected_names (Optional[Sequence[str]]): The ordered projection list of columns (Notion key values **and** properties) the user wants to have in the returned rows.

        Raises:
            NoSuchColumnError: If any of the column names in ``projection_names`` could not be found in the table.

        Returns:
            SchemaInfo: A new schema information instance.

        .. versionadded:: 0.9.0
        """

        result_columns: List[ResultColumn] = []
        merged_columns = _merge_names(
            execution_names=execution_names,
            projected_names=projected_names,
        )

        for name in merged_columns:
            # Table-declared columns
            try:
                column = table.c[name]
            except KeyError:
                raise NoSuchColumnError(
                    f"Column '{name}' not found in table '{table.name}'."
                )

            result_columns.append(
                ResultColumn(
                    name=name,
                    type_code=column.type_.get_dbapi_type(),
                    nullable=None,
                    table=table,
                    bare_name=name,
                )
            )

        return cls(tuple(result_columns))
    
    @classmethod
    def from_join(
        cls, 
        left: Table,
        right: Table,
        *entities: Column
    ) -> SchemaInfo:
        fqname_by_col: dict[Column, str] = {}

        # find colliding column names:
        # same name in entities
        colliding: dict[str, list[Column]] = {}
        for ent in entities:
            if ent.name not in colliding:
                colliding[ent.name] = [ent]
            else:
                colliding[ent.name].append(ent)

        # fully qualify names for colliding only
        for cols in colliding.values():
            if len(cols) == 1:
                # skip, one column only does not need qualification
                continue

            for ent in cols:
                fqname_by_col[ent] = f"{ent.parent.name}.{ent.name}"

        result_cols = [
            ResultColumn(
                # take the FQ-name if it's a colliding column
                fqname_by_col.get(rc, rc.name),
                type_code=rc.type_.get_dbapi_type(),
                nullable=False,
                table=rc.parent,
                bare_name=rc.name,
            )
            for rc in entities
        ]
        return SchemaInfo(result_cols)
    
    @classmethod
    def from_aggregate(
        cls, 
        *entities: FunctionElement
    ) -> SchemaInfo:
        disambiguate_names: dict[FunctionElement, str] = {}

        # group entities by result key (function name, or the .label() override):
        # any key shared by 2+ entities is a collision
        colliding: dict[str, list[FunctionElement]] = {}
        for ent in entities:
            if ent.key not in colliding:
                colliding[ent.key] = [ent]
            else:
                colliding[ent.key].append(ent)

        # aggregates are provenance-free (table=None) so a collision can't be
        # qualified by table like from_join; disambiguate by ordinal suffix instead
        for cols in colliding.values():
            if len(cols) == 1:
                # a unique key stays bare, no disambiguation needed
                continue

            for i, ent in enumerate(cols):
                disambiguate_names[ent] = f"{ent.key}_{i + 1}"

        result_cols = [
            ResultColumn(
                disambiguate_names.get(ent, ent.key),
                type_code=ent.type_.get_dbapi_type(),
                nullable=True,
                table=None,
                # for colliding names, bare_name is not disambiguated
                # because aggregates are Table=None
                bare_name=ent.key
            )
            for ent in entities
        ]

        return SchemaInfo(result_cols)
        
    def as_sequence(self) -> Sequence[tuple]:
        """Provide the description for DBAPI cursors.
        
        This method is the official API for :class:`SchemaInfo`.
        """
        entries = []

        for col in self.columns:
            entry = (
                col.name,
                col.type_code,      
                None,                        # display_size
                None,                        # internal_size
                None,                        # precision
                None,                        # scale
                col.nullable,                # col.nullable for future versions.
            )

            entries.append(entry)

        return tuple(entries)

    def _ensure_index_map(self) -> None:
        if self._index_map is None:
            index_map = {
                col.name: idx
                for idx, col in enumerate(self.columns)
            }
            object.__setattr__(self, "_index_map", index_map)

    def column_index(self, name: str) -> int:
        if not self.columns:
            raise InvalidRequestError(
                "Cannot provide index on empty or uninitialized SchemaInfo object."
            )

        self._ensure_index_map()

        try:
            return self._index_map[name]
        except KeyError:
            raise NoSuchColumnError(f"Column: '{name}'")
        
    def column_getter(self, name: str) -> Callable[[Sequence[Any]], Any]:
        idx = self.column_index(name)

        def getter(row: Sequence[Any]) -> Any:
            return row[idx]

        return getter
    
    def __contains__(self, key: str) -> bool:
        if not isinstance(key, str):
            raise ArgumentError(
                "__contains__ requires a string argument"
            )
        
        if key not in self._index_map:
            return False
        
        return True
