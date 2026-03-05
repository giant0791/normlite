from __future__ import annotations

from dataclasses import dataclass
import pdb
from typing import Any, Literal, Optional, Sequence

import pytest

from normlite._constants import SpecialColumns
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode

@pytest.fixture
def client() -> InMemoryNotionClient:
    new_client = InMemoryNotionClient()
    new_client._ensure_root()
    return new_client

def add_pages(client: InMemoryNotionClient, page_data: list[dict]) -> tuple[str, list[str]]:
    students_db = client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': client._ROOT_PAGE_ID_
        },
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "students",
                    "link": None
                },
                "plain_text": "students",
                "href": None
            }
        ],
        'properties': {
            'name': {'title': {}},
            'id': {'number': {}},
            'is_active': {'checkbox': {}},
            'start_on': {'date': {}},
            'grade': {'rich_text': {}},
        }
    })

    inserted_pages = []
    for data in page_data:
        page = client.pages_create(
            payload={
                'parent': {
                    'type': 'database_id',
                    'database_id': students_db['id']
                },
                'properties': {
                    'name': {'title': [{'text': {'content': data['name']}}]},
                    'id': {'number': data['id']},
                    'is_active': {'checkbox': data['is_active']},
                    'start_on': {'date': {'start': data['start_on']}},
                    'grade': {'rich_text': [{'text': {'content': data['grade']}}]},
                }
            }
        )
        inserted_pages.append(page)

    return students_db['id'], inserted_pages

def test_client_conforms_API_version_2022_06_28(client: InMemoryNotionClient):
    _, pages = add_pages(
        client, [
            {
                "name": "Galileo Galilei", 
                "id": 123456, 
                "is_active": False, 
                "start_on": "1581-01-01", 
                "grade": "A"
            }
        ]
    )

    properties = pages[0].get("properties")
    assert list(properties['name'].keys()) == ['id']
    assert list(properties['id'].keys()) == ['id']
    assert list(properties['is_active'].keys()) == ['id']
    assert list(properties['start_on'].keys()) == ['id']
    assert list(properties['grade'].keys()) == ['id']

@dataclass
class NotionProperty:
    key: str
    id: Optional[str]
    type_: Optional[str]
    value: Optional[Any]
    """Render either a Notion object value like "created_time" or a Notion property value."""

    @property
    def has_id_only(self) -> bool:
        return self.id is not None and self.type_ is None and self.value is None

    @property
    def is_database_property(self) -> bool:
        return self.id is not None and self.type_ is not None and self.value == {}

@dataclass
class NotionParent:
    type_: Literal["page_id", "database_id"]
    id: str

class NotionObject:
    def __init__(
        self,
        object_type: Literal["page", "database"],
        object_id: str,
        parent: NotionParent,
        in_trash: bool,
        archived: bool,
        properties: dict[str, NotionProperty],
        *,
        created_time: Optional[str] = None, 
        last_edited_time: Optional[str] = None,
    ) -> None:
        
        self.object_type = object_type
        self.object_id = object_id
        self.parent = parent
        self.in_trash = in_trash
        self.archived = archived
        self.properties = properties
        self.created_time = created_time
        self.last_edited_time = last_edited_time

    @property
    def is_page(self) -> bool:
        return self.object_type == "page"
    
    @property
    def is_database(self) -> bool:
        return self.object_type == "database"

    @property
    def is_page_created_or_updated(self) -> bool:
        if self.object_type != "page":
            return False
        
        return all([p.has_id_only for p in self.properties.values()])
    
    def infer_schema(self) -> Sequence[tuple]:
        """This is a smell: DBAPI cursor.description should **not** be inferred.
        
        cursor.description shall come from the authoritative Table construct and not inferred from the page object.
        There are unresolvable ambiguities inherent in the page property types.
        This triggers a big refactor.
        """
        schema = [
            self._add_not_used_seq((SpecialColumns.NO_ID, DBAPITypeCode.ID,)), 
            self._add_not_used_seq((SpecialColumns.NO_CREATE_TIME, DBAPITypeCode.DATETIME_ISO_8601)),
            self._add_not_used_seq((SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX,)),
            self._add_not_used_seq((SpecialColumns.NO_IN_TRASH, DBAPITypeCode.CHECKBOX,)),
        ]

        for key, prop in self.properties.items():
            type_ = prop.type_
            if prop.type_ == "number":
                type_ = DBAPITypeCode.NUMBER if isinstance(prop.value, int) else DBAPITypeCode.NUMBER
            
            schema.append(self._add_not_used_seq((key, type_)))

    def _add_not_used_seq(self, col_desc: tuple, count: int = 5) -> tuple:
        """Helper to fill in the missing elements with ``None`` values."""
        for _ in range(count):
            col_desc += (None,)

        return col_desc
        
    def __repr__(self):
        properties = ", ".join(repr(p) for p in self.properties.values())
        attrs = ", ".join([
            f"object='{self.object_type}'",
            f"id='{self.object_id}'",
            f"parent={self.parent}",
            f"created_time='{self.created_time}'",
            f"in_trash={self.in_trash}",
            f"archived={self.archived}",
            f"properties=({properties})"
        ])
        return f"NotionObject({attrs})"
    
    @classmethod
    def from_dict(cls, json_as_dict: dict) -> NotionObject:
        # parse parent object
        parent_type = json_as_dict["parent"]["type"]
        parent = NotionParent(
            parent_type,
            json_as_dict["parent"][parent_type]
        )

        # parse properties
        properties = {}
        for k, v in json_as_dict["properties"].items():
            key = k
            id = v.get("id")
            type_ = v.get("type")
            value = v.get(type_)
            properties[key] = NotionProperty(key, id, type_, value)

        return cls(
            json_as_dict["object"],
            json_as_dict["id"],
            parent,
            json_as_dict["in_trash"],
            json_as_dict["archived"],
            properties,
            created_time=json_as_dict.get("created_time", None),
            last_edited_time=json_as_dict.get("last_edited_time", None)
        )     

def test_parse_page_created_contains_property_ids_only(client: InMemoryNotionClient):
    _, pages = add_pages(
        client, [
            {
                "name": "Galileo Galilei", 
                "id": 123456, 
                "is_active": False, 
                "start_on": "1581-01-01", 
                "grade": "A"
            }
        ]
    )

    page_obj = NotionObject.from_dict(pages[0])

    assert page_obj.is_page_created_or_updated
    assert page_obj.object_id == pages[0].get("id")
    assert page_obj.object_type == "page" and page_obj.is_page
    assert page_obj.created_time == pages[0].get("created_time")
    assert page_obj.properties['name'].id == pages[0]['properties']['name']['id']
    assert page_obj.properties['id'].id ==  pages[0]['properties']['id']['id']
    assert page_obj.properties['is_active'].id == pages[0]['properties']['is_active']['id']
    assert page_obj.properties['start_on'].id == pages[0]['properties']['start_on']['id']
    assert page_obj.properties['grade'].id == pages[0]['properties']['grade']['id']

def test_parse_page_retrieved_contains_values(client: InMemoryNotionClient):
    db_id, pages = add_pages(
        client, [
            {
                "name": "Galileo Galilei", 
                "id": 123456, 
                "is_active": False, 
                "start_on": "1581-01-01", 
                "grade": "A"
            }
        ]
    )

    retrieved = client.databases_query(
        path_params={"database_id": db_id}      # retrieve all
    )
    
    page_props = pages[0]["properties"]
    results = retrieved["results"]
    assert len(results) == 1
    
    page_obj = NotionObject.from_dict(results[0])
    assert page_obj.is_page
    
    properties = page_obj.properties
    assert properties["name"].id == page_props["name"]["id"]
    assert properties["name"].type_ == "title"
    assert isinstance(properties["name"].value, list) 
    assert properties["name"].value[0]["plain_text"] == "Galileo Galilei"

    assert properties["id"].id == page_props["id"]["id"]
    assert properties["id"].type_ == "number"
    assert isinstance(properties["id"].value, int) 
    assert properties["id"].value == 123456


    