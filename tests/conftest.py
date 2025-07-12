import pytest
from normlite.notion_sdk.client import AbstractNotionClient, FakeNotionClient

@pytest.fixture(scope="session")
def client() -> AbstractNotionClient:
    return FakeNotionClient(auth='ntn_abc123def456ghi789jkl012mno345pqr')