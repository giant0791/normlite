import pdb
from sqlite3 import ProgrammingError

from normlite.notion_sdk.getters import get_checkbox_property_value, get_date_property_value, get_number_property_value, get_rich_text_property_value, get_title_property_value
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode


class ResultSet:
    """DBAPI-level representation of a Notion result set."""

    _TYPE_PROCESSORS = {
        "title": get_title_property_value,
        "rich_text": get_rich_text_property_value,
        "checkbox": get_checkbox_property_value,
        "date": get_date_property_value,                # this delivers a non-scalar value: dictionary with "start" and optionally "end"
        "number": get_number_property_value, 
    }

    _SYSTEM_COLUMNS = (
        ("object_id", DBAPITypeCode.ID, "id"),
        ("is_archived", DBAPITypeCode.ARCHIVAL_FLAG, "archived"),
        ("is_deleted", DBAPITypeCode.ARCHIVAL_FLAG, "in_trash"),
        ("created_at", DBAPITypeCode.TIMESTAMP, "created_time"),
        #("updated_at", DBAPITypeCode.TIMESTAMP, "last_edited_time"),
    )    

    _METADATA = ("column_name", "column_type", "column_id", "metadata", "is_system")

    def __init__(self, rows: list[dict], is_database: bool):
        self._rows = rows
        self._index = 0
        self._is_database = is_database

    @classmethod
    def from_json(cls, notion_obj: dict):

        if notion_obj["object"] == "list":
            results = notion_obj.get("results")
        else:
            results = [notion_obj]

        rows = []
        is_database = False

        for obj in results:
            obj_type = obj["object"]

            if obj_type == "page":
                rows.append(cls._process_page(obj))

            elif obj_type == "database":
                is_database = True
                rows.extend(cls._process_database(obj))

            else:
                raise NotImplementedError(obj_type)

        return cls(rows, is_database)

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
    
    
    def make_description(self) -> list[tuple]:
        if not self._is_database:
            raise ProgrammingError("Cannot make description, result set does not contain table metadata.")
        
        desc = []

        for colname in self._METADATA:
            desc.append((colname, None, None, None, None, None, None,))

        return desc
            
    @classmethod
    def _process_page(cls, page: dict) -> dict:
        """Normalize a page object."""

        row = {
            "id": page["id"],
            "archived": page["archived"],
            "in_trash": page["in_trash"],
            "created_time": page["created_time"],
        }
        for name, prop in page["properties"].items():
            row[name] = prop

        return row
    
    @classmethod
    def _process_database(cls, database: dict) -> list[dict]:
        """Normalize a database object."""

        rows = []

        # system columns
        for colname, coltype, field in cls._SYSTEM_COLUMNS:
            rows.append(
                (
                    colname,
                    coltype,
                    None,
                    database[field],
                    True,               # is_system
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