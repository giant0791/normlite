from __future__ import annotations
from operator import itemgetter
import pdb
from typing import Any, Callable, Optional, Sequence

from normlite._constants import SpecialColumns
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode

_SYSTEM_COLUMNS_PAGE = {
    SpecialColumns.NO_ID: "id",                       # TODO: rename in "object_id"
    SpecialColumns.NO_ARCHIVED: "archived",           # TODO: rename in "is_archived"   
    SpecialColumns.NO_IN_TRASH: "in_trash",           # TODO: rename in "is_deleted"
    SpecialColumns.NO_CREATED_TIME: "created_time",   # TODO: rename in "created_at"
    #("updated_at", DBAPITypeCode.TIMESTAMP, "last_edited_time"),
}


class ResultSet:
    """DBAPI-level representation of a Notion result set."""


    _SYSTEM_COLUMNS_DATABASE = (
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, "id"),                              # TODO: rename in "object_id"
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.ARCHIVAL_FLAG, "archived"),       # TODO: rename in "is_archived"   
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.ARCHIVAL_FLAG, "in_trash"),       # TODO: rename in "is_deleted"
        (SpecialColumns.NO_CREATED_TIME, DBAPITypeCode.TIMESTAMP, "created_time"),   # TODO: rename in "created_at"
        (SpecialColumns.NO_TITLE, DBAPITypeCode.TITLE, "title"),                     # TODO: rename in "table_name"

        #("updated_at", DBAPITypeCode.TIMESTAMP, "last_edited_time"),
    )
 

    _METADATA = ("column_name", "column_type", "column_id", "metadata", "is_system")
    _pg_oid_getter = itemgetter(0)
    _db_oid_getter = itemgetter(3) 

    def __init__(
        self,
        description: tuple[tuple, ...],
        object_type: str,
        rows: list[tuple],
    ):
        self._index = 0
        self._description = description
        self._object_type = object_type
        self._rows = rows

    @classmethod
    def from_json(
        cls, 
        description: tuple[tuple, ...],
        notion_obj: dict
    ) -> ResultSet:
        if notion_obj["object"] == "list":
            results = notion_obj.get("results")
            object_type = notion_obj["type"]

        else:
            results = [notion_obj]
            object_type = notion_obj["object"]

        rows = []

        for obj in results:
            obj_type = obj["object"]

            if obj_type == "page":
                object_type = obj_type
                rows.append(cls._process_page(description, obj))

            elif obj_type == "database":
                object_type = obj_type
                rows.extend(cls._process_database(obj))

            else:
                raise NotImplementedError(obj_type)

        return cls(description, object_type, rows)

    def __iter__(self):
        return self

    def __next__(self):
        if self._index >= len(self._rows):
            raise StopIteration
        row = self._rows[self._index]
        self._index += 1
        return row

    def __len__(self) -> int:
        return len(self._rows)
    
    @property
    def description(self) -> Optional[list[tuple]]:
        if not self._rows:
            # previous operation returned no rows ([])
            return None

        if self._description is not None:
            # description was previously injected, the result set contains pages
            return self._description        
        
        # always construct the description for databases
        # DO NOT store, this signals the _rows contains column metadata
        desc = []

        for colname in self._METADATA:
            desc.append((colname, None, None, None, None, None, None,))

        return desc
    
    @property
    def last_inserted_rowids(self) -> Optional[list[tuple]]:
        if self._object_type == "database":
            # the result set contains columns metadata from a database:
            # the first row only provides the database id
            return None
        
        if self._rows == []:
            # no rows modified
            return None

        # the result set contains pages
        # each entry in the result set provides ids
        return [(self._pg_oid_getter(r),) for r in self._rows]
            
    @classmethod
    def _process_page(
        cls, 
        description: tuple[tuple, ...],        
        page: dict,
    ) -> tuple:
        """Normalize a page object."""

        row = []
        properties = page.get("properties", {})

        for desc_entry in description:
            col = desc_entry[0]

            if col in SpecialColumns:
                row.append(page.get(_SYSTEM_COLUMNS_PAGE[col]))
            else:
                prop = properties.get(str(col))
                typ = prop.get("type")
                row.append(prop.get(typ) if typ else None)

        return tuple(row)    
    
    @classmethod
    def _process_database(cls, database: dict) -> list[tuple]:
        """Normalize a database object."""

        rows = []

        # system columns
        for colname, coltype, field in cls._SYSTEM_COLUMNS_DATABASE:
            rows.append(
                (
                    colname,
                    coltype,
                    None,
                    database[field],
                    True,               # is system column
                )
            )

        # user defined columns
        for name, prop in database["properties"].items():
            typ = prop["type"]
            rows.append(
                (
                    name,
                    typ,
                    prop["id"],
                    prop[typ],
                    False,              # is user defined
                )
            )

        return rows

    @classmethod
    def _process_property(cls, prop: dict):
        """Generic property processor."""

        prop_type = prop["type"]
        return prop.get(prop_type)