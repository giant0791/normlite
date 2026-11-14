import pytest

from normlite.notion_sdk.client import AbstractNotionClient


def _stub(*args, **kwargs):
    raise NotImplementedError


def test_abstract_client_has_no_default_root_page_id():
    # Arrange: the smallest concrete client. It fills each abstract API call
    # with a stub and adds nothing else.
    MinimalClient = type(
        "MinimalClient",
        (AbstractNotionClient,),
        {name: _stub for name in AbstractNotionClient.__abstractmethods__},
    )
    client = MinimalClient()

    # Act
    root_page_id = client.default_root_page_id

    # Assert
    assert root_page_id is None
