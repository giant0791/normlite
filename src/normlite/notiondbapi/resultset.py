from operator import itemgetter
import pdb
from typing import Optional

from normlite._constants import SpecialColumns
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode


class ResultSet:
    """DBAPI-level representation of a Notion result set."""

    _SYSTEM_COLUMNS_PAGE = (
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, "id"),                              # TODO: rename in "object_id"
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.ARCHIVAL_FLAG, "archived"),       # TODO: rename in "is_archived"   
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.ARCHIVAL_FLAG, "in_trash"),       # TODO: rename in "is_deleted"
        (SpecialColumns.NO_CREATED_TIME, DBAPITypeCode.TIMESTAMP, "created_time"),   # TODO: rename in "created_at"

        #("updated_at", DBAPITypeCode.TIMESTAMP, "last_edited_time"),
    )    

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

    def __init__(self, notion_obj:dict, description: tuple[tuple, ...]):
        self._index = 0
        self._description = description
        self._object_type = None
        self._rows = self._from_json(notion_obj)

    def _from_json(self, notion_obj: dict) -> list[tuple]:
        if notion_obj["object"] == "list":
            results = notion_obj.get("results")

        else:
            results = [notion_obj]

        rows = []

        for obj in results:
            obj_type = obj["object"]

            if obj_type == "page":
                self._object_type = obj_type
                rows.append(self._process_page(obj))

            elif obj_type == "database":
                self._object_type = obj_type
                rows.extend(self._process_database(obj))

            else:
                raise NotImplementedError(obj_type)

        return rows

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
    def last_inserted_rowids(self) -> Optional[list[str]]:
        if self._object_type == "database":
            # the result set contains columns metadata from a database:
            # the first row only provides the database id
            return None
        
        if self._rows == []:
            # no rows modified
            return None

        # the result set contains pages
        # each entry in the result set provides ids
        return [self._pg_oid_getter(r) for r in self._rows]
            
    def _process_page(self, page: dict) -> tuple:
        """Normalize a page object."""

        get_col_name = itemgetter(0)
        get_syscol_name = itemgetter(2)
        columns = [
            get_col_name(d) 
            for d in self._description 
            if get_col_name(d) not in SpecialColumns
        ]
        
        sys_cols = [get_syscol_name(c) for c in self._SYSTEM_COLUMNS_PAGE]
        row = [page[col] for col in sys_cols]

        for col in columns:           
            prop = page["properties"][col]
            typ = prop.get("type")
            row.append(prop[typ] if typ else None)     # value is available only if the page is returned by databases.query or pages.retrieve

        return tuple(row)
    
    def _process_database(self, database: dict) -> list[tuple]:
        """Normalize a database object."""

        rows = []

        # system columns
        for colname, coltype, field in self._SYSTEM_COLUMNS_DATABASE:
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