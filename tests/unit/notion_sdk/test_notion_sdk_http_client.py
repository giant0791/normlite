import json

import pytest
import requests

from normlite.notion_sdk.client import HttpNotionClient


class FakeSession(requests.Session):
    """Record each prepared request and answer it with a canned response.

    The fake replaces only ``send()``, so no request goes to the network.
    It also records whether somebody closed it.
    """

    def __init__(self, response_body: dict | None = None):
        super().__init__()
        self.sent: list[requests.PreparedRequest] = []
        self.closed = False
        self._response_body = response_body or {}

    def close(self):
        self.closed = True
        super().close()

    def send(self, request, **kwargs):
        self.sent.append(request)
        response = requests.Response()
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"
        response._content = json.dumps(self._response_body).encode("utf-8")
        response.request = request
        response.url = request.url
        return response


def test_pages_retrieve_sends_one_get_with_the_notion_headers():
    # Arrange
    page_id = "680dee41-b447-451d-9d36-c6eaff13fb46"
    page = {"object": "page", "id": page_id, "properties": {}}
    session = FakeSession(response_body=page)
    client = HttpNotionClient("fake-token", session=session)

    # Act
    result = client.pages_retrieve({"page_id": page_id})

    # Assert
    assert len(session.sent) == 1
    sent = session.sent[0]
    assert sent.method == "GET"
    assert sent.url == f"https://api.notion.com/v1/pages/{page_id}"
    assert sent.headers["Authorization"] == "Bearer fake-token"
    assert sent.headers["Notion-Version"] == "2026-03-11"
    assert sent.headers["Content-Type"] == "application/json"
    assert result == page


def test_close_closes_the_session_that_the_client_created(monkeypatch):
    # Arrange: the client creates its own session. The fake replaces the
    # session class in the client module only, not in ``requests``.
    created: list[FakeSession] = []

    def make_session():
        session = FakeSession()
        created.append(session)
        return session

    monkeypatch.setattr("normlite.notion_sdk.client.Session", make_session)
    client = HttpNotionClient("fake-token")

    # Act
    client.close()

    # Assert
    assert len(created) == 1
    assert created[0].closed


def test_client_state_does_not_show_the_token():
    # Arrange
    client = HttpNotionClient("fake-token", session=FakeSession())

    # Act: show the client and all the state it keeps, as a debugger or a
    # traceback with local variables does.
    shown = repr(client) + repr(vars(client))

    # Assert
    assert "fake-token" not in shown


def test_close_does_not_close_an_injected_session():
    # Regression guard: the creator closes. The caller created this session,
    # so the caller closes it.
    # Arrange
    session = FakeSession()
    client = HttpNotionClient("fake-token", session=session)

    # Act
    client.close()

    # Assert
    assert not session.closed
