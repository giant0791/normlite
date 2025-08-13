from __future__ import annotations
import pdb
from typing import List, Optional, Sequence
from flask.testing import FlaskClient
import pytest

from normlite.notion_sdk.client import AbstractNotionClient
from normlite.notiondbapi.dbapi2 import Connection, Cursor, DBAPIParamStyle, CompositeCursor
from normlite.proxy.state import notion
from normlite.cursor import _NO_CURSOR_RESULT_METADATA, CursorResult, Row

@pytest.fixture
def data() -> List[dict]:
    """Simulate the data property returned in the JSON object by POST /transaction/<id/commit."""

    data = []
    data.append({
        "object": "page",
        "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
        "created_time": "2022-03-01T19:05:00.000Z",
        "archived": False,
        "properties": {
                "Store_availability": {
                "id": "%3AUPp"
            },
                "Food_group": {
                "id": "A%40Hk"
            },
                "Price": {
                "id": "BJXS"
            }
        }
    })
    data.append({
        "object": "list",
        "results": [{
            "object": "page",
            "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
            "created_time": "2022-03-01T19:05:00.000Z",
            "last_edited_time": "2022-07-06T20:25:00.000Z",
            "parent": {
                "type": "database_id",
                "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce"
            },
            "archived": False,
            "properties": {
                "Price": {
                    "id": "BJXS",
                    "type": "number",
                    "number": 2.5
                },
                "Description": {
                    "id": "_Tc_",
                    "type": "rich_text",
                    "rich_text": [{
                        "type": "text",
                        "text": {
                            "content": "A dark ",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "A dark ",
                        "href": None
                    },
                    {
                        "type": "text",
                        "text": {
                            "content": "green",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "green"
                        },
                        "plain_text": "green",
                        "href": None
                    },
                    {
                        "type": "text",
                        "text": {
                            "content": " leafy vegetable",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": " leafy vegetable",
                        "href": None
                    }]
                },
                "Name": {
                    "id": "title",
                    "type": "title",
                    "title": [{
                        "type": "text",
                        "text": {
                            "content": "Tuscan kale",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "Tuscan kale",
                        "href": None
                    }]
                }
            },
            "url": "https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5"
        },
        {
            "object": "page",
            "id": "59833787-2cf9-4fdf-8782-e53db20768a6",
            "created_time": "2022-03-01T19:05:00.000Z",
            "last_edited_time": "2022-07-06T20:25:00.000Z",
            "parent": {
                "type": "database_id",
                "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce"
            },
            "archived": False,
            "properties": {
                "Price": {
                    "id": "BJXS",
                    "type": "number",
                    "number": 3.5
                },
                "Description": {
                    "id": "_Tc_",
                    "type": "rich_text",
                    "rich_text": [{
                        "type": "text",
                        "text": {
                            "content": "A bright ",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "A bright ",
                        "href": None
                    },
                    {
                        "type": "text",
                        "text": {
                            "content": "red",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "red"
                        },
                        "plain_text": "red",
                        "href": None
                    },
                    {
                        "type": "text",
                        "text": {
                            "content": " long, fleshy and very tasty tomato",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": " long, fleshy and very tasty tomato",
                        "href": None
                    }]
                },
                "Name": {
                    "id": "title",
                    "type": "title",
                    "title": [{
                        "type": "text",
                        "text": {
                            "content": "Napolitan San Marzano tomato",
                            "link": None
                        },
                        "annotations": {
                            "bold": False,
                            "italic": False,
                            "strikethrough": False,
                            "underline": False,
                            "code": False,
                            "color": "default"
                        },
                        "plain_text": "Tuscan kale",
                        "href": None
                    }]
                }
            },
            "url": "https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5"
        }],
        "next_cursor": None,
        "has_more": False,
        "type": "page_or_database",
        "page_or_database": {}      
    })
    return data
    
class CompositeCursorResult(CursorResult):
    """Prototype"""
    def __init__(self, cursors: Sequence[Cursor]):
        self._cursors: List[Cursor] = list(cursors) 
        self._current_cursor_index = 0
        self._current_cursor = self._cursors[self._current_cursor_index]

class NewCursorResult:
    """Prototype for new and refactored CursorResult class with composite cursor feature."""

    # TODO: 
    # DECIDE: Composition over inheritance?
    # THINK: CursorResultBase or better just Result in case of composition?
    # --------------------------------------------------------------------------------------
    # 1. Put current CursorResult implementation into CursorResultBase
    # 2. Refactor CursorResult as subclass of CursorResultBase and use this implementation
    # 3. Add CursorResultBase.close() and CursorResult.close() methods 
    # --------------------------------------------------------------------------------------
    
    def __init__(self, dbapi_cursor: CompositeCursor):
        self._dbapi_cursor = dbapi_cursor
        self._current_result = CursorResult(self._dbapi_cursor._current_cursor)

    def next_result(self) -> bool:
        """Advance to the next cursor, if available."""
        if self._dbapi_cursor.nextset():
            # next result set is available
            # first close the current cursor result
            # TODO: Replace with self._current_cursor.close()
            self._current_result._metadata = _NO_CURSOR_RESULT_METADATA

            # update the current cursor result
            self._current_result = CursorResult(self._dbapi_cursor._current_cursor)
            return True
        
        # all result sets depleted
        # TODO: add self.close()
        return False
    
    def one(self) -> Row:
        return self._current_result.one()
    
    def all(self) -> Sequence[Row]:
        return self._current_result.all()

def test_fetch_from_compositecursor(proxy_client: FlaskClient, client: AbstractNotionClient, data: List[dict]):
    conn = Connection(proxy_client, client)
    
    # sketch implementation for the Connection.commit()
    # Connection.commit() builds a composite cursor
    conn._create_cursors(data)
    comp_cur = conn.cursor(composite=True)

    # init the cursor result with a composite cursor and
    # set the current result to the first cursor result set
    result = NewCursorResult(comp_cur)
    row = result.one()
    assert row['__id__'] == '59833787-2cf9-4fdf-8782-e53db20768a5'

    # move on to the next cursor, there shall be one more
    #pdb.set_trace()
    assert result.next_result()
    rows = result.all()
    assert len(rows) == 2







