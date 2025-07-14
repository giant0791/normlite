from __future__ import annotations
from typing import Dict, Any, List, Optional
from normlite.notion_sdk.client import FakeNotionClient


def create_engine(
        uri: str,
        host: str,
        database: str,
        api_key: str
) -> Engine:
    """Create a new engine object to get access to the Notion database.
    
    Each engine object represents a Notion page, where the all the database tables are 
    appended as children. 
    
    """
    return Engine(database, api_key)

class Engine:
    def __init__(self, database: str, api_key: str) -> None:
        self._database = database
        self._api_key = api_key

        # TODO: Implement real database discovery
        # This is a preliminary code implementation so that the code imports.
        client = FakeNotionClient(self._api_key) 
        page: Dict[str, Any] = client.pages_create({
            "Title": {"id": "title", "title": self._database}
        })

        self._database_id = page.get('id')
    
    @property
    def database(self) -> str:
        return self._database
    
    @property
    def database_id(self) -> str:
        return self._database_id

    def execute(self, statement: str, args: Optional[List[str]]) -> CursorResult:
        pass

class CursorResult:
    def __len__(self) -> int:
        return 1

