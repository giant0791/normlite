from operator import itemgetter
import pdb
import uuid

import pytest
from faker import Faker

from normlite.notion_sdk.client import InMemoryNotionClient, NotionError
from normlite.notion_sdk.getters import rich_text_to_plain_text
from normlite.notiondbapi.dbapi2 import (
    Connection,
    Cursor,
    DatabaseError,
    OperationalError,
    ProgrammingError,
)

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

fake = Faker()
Faker.seed(42)

def generate_pages(client: InMemoryNotionClient, n: int) -> tuple[str, list[dict]]:
    """
    Create a students database and populate it with n fake pages.

    Returns:
        (database_id, rows)
        rows = normalized dict representation of inserted pages
    """

    students_db = client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': client._ROOT_PAGE_ID_
        },
        "title": [{
            "type": "text",
            "text": {"content": "students"},
            "plain_text": "students",
            "href": None
        }],
        'properties': {
            'name': {'title': {}},
            'id': {'number': {}},
            'is_active': {'checkbox': {}},
            'start_on': {'date': {}},
            'grade': {'rich_text': {}},
        }
    })

    db_id = students_db["id"]
    rows = []

    for _ in range(n):

        data = {
            "name": fake.name(),
            "id": fake.random_int(100000, 999999),
            "is_active": False,
            "start_on": fake.date(),
            "grade": fake.random_element(["A", "B", "C", "D"]),
        }

        page = client.pages_create(
            payload={
                'parent': {
                    'type': 'database_id',
                    'database_id': db_id
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

        rows.append(page)

    return db_id, rows

def rows_to_tuples(rows: list[dict]) -> list[tuple]:
    return [
        (
            r["page_id"],
            r["archived"],
            r["in_trash"],
            r["created_time"],
            r["name"],
            r["id"],
            r["is_active"],
            r["start_on"],
            r["grade"],
        )
        for r in rows
    ]

def retrieve_database(client: InMemoryNotionClient) -> dict:
    return client._add('database', {
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


def execute_query_returns_no_rows(
        cursor: Cursor, 
        row_description: tuple[tuple, ...],
        database_id: str    
):
    cursor._inject_description(row_description)
    cursor.execute(
        operation={
            "endpoint": "databases",
            "request": "query"
        },
        parameters={
            "path_params": {"database_id": database_id},
            "payload":{
                "filter": {
                    "property": "is_active",
                    "checkbox": {
                        "equals": True
                    }
                }
            }
        }
    )

def execute_query_returns_all_rows(
        cursor: Cursor, 
        row_description: tuple[tuple, ...],
        database_id: str    
):
    cursor._inject_description(row_description)
    cursor.execute(
        operation={
            "endpoint": "databases",
            "request": "query"
        },
        parameters={
            "path_params": {"database_id": database_id},
            "payload":{
                "filter": {
                    "property": "is_active",
                    "checkbox": {
                        "does_not_equal": True
                    }
                }
            }
        }
    )

get_name = itemgetter(4)

#----------------------------------------------------------------
# Description tests
#----------------------------------------------------------------

def test_description_none_if_no_execute(client: InMemoryNotionClient):
    cursor = Cursor(Connection(client))

    assert cursor.description is None

def test_description_none_if_no_rows(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=10)
    execute_query_returns_no_rows(cursor, row_description, database_id)

    assert cursor.description is None

def test_description_none_on_closed_cursor(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=10)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    cursor.close()

    assert cursor.description is None

#----------------------------------------------------------------
# fetchone tests
#----------------------------------------------------------------

def test_fetchone_returns_none_if_no_rows_found(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=10)
    cursor._inject_description(row_description)
    execute_query_returns_no_rows(cursor, row_description, database_id)
    row = cursor.fetchone()

    assert row is None

def test_fetchone_returns_first_row_found(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=10)
    cursor._inject_description(row_description)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    first_page = client("pages", "retrieve", path_params={"page_id": pages[0]["id"]})
    first = cursor.fetchone()

    assert rich_text_to_plain_text(get_name(first)["title"]) == rich_text_to_plain_text(
        first_page["properties"]["name"]["title"]
    )
    assert not cursor.closed

def test_fetchone_consumes_cursor(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=3)
    cursor._inject_description(row_description)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    first = cursor.fetchone()
    second = cursor.fetchone()
    third = cursor.fetchone()
    first_page = client("pages", "retrieve", path_params={"page_id": pages[0]["id"]})
    second_page = client("pages", "retrieve", path_params={"page_id": pages[1]["id"]})
    third_page = client("pages", "retrieve", path_params={"page_id": pages[2]["id"]})

    should_be_none = cursor.fetchone()

    assert rich_text_to_plain_text(get_name(first)["title"]) == rich_text_to_plain_text(
        first_page["properties"]["name"]["title"]
    )
    assert rich_text_to_plain_text(get_name(second)["title"]) == rich_text_to_plain_text(
        second_page["properties"]["name"]["title"]
    )
    assert rich_text_to_plain_text(get_name(third)["title"]) == rich_text_to_plain_text(
        third_page["properties"]["name"]["title"]
    )
    assert should_be_none is None
    assert not cursor.closed

def test_fetchone_raises_on_closed_cursor(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n=3)
    cursor._inject_description(row_description)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    cursor.close()

    with pytest.raises(ProgrammingError):
        _ = cursor.fetchone()

def test_fetchone_raises_if_no_execute(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(Connection(client))
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchone()

#----------------------------------------------------------------
# fetchall tests
#----------------------------------------------------------------

def test_fetchall_returns_all_rows_found(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=100)
    cursor._inject_description(row_description)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    first_page = client("pages", "retrieve", path_params={"page_id": pages[0]["id"]})
    last_page = client("pages", "retrieve", path_params={"page_id": pages[-1]["id"]})
    rows = cursor.fetchall()
    should_be_none = cursor.fetchone()

    assert len(rows) == 100
    assert rich_text_to_plain_text(get_name(rows[0])["title"]) == rich_text_to_plain_text(
        first_page["properties"]["name"]["title"]
    )
    assert rich_text_to_plain_text(get_name(rows[-1])["title"]) == rich_text_to_plain_text(
        last_page["properties"]["name"]["title"]
    )
    assert should_be_none is None
    assert not cursor.closed

def test_fetchall_raises_if_no_execute(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(Connection(client))
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchall()

def test_fetchall_raises_on_closed_cursor(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(Connection(client))
    cursor.close()
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchall()


#----------------------------------------------------------------
# fetchmany tests
#----------------------------------------------------------------

def test_fetch_many_returns_arrasize_multiples_of_rows(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    cursor = Cursor(Connection(client))
    database_id, inserted_rows = generate_pages(client, 16)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    cursor.arraysize = 8
    batch_1 = inserted_rows[:8]
    batch_2 = inserted_rows[8:]
    rows_1 = cursor.fetchmany()
    rows_2 = cursor.fetchmany()

    assert len(batch_1) == len(rows_1)
    assert len(batch_2) == len(rows_2)
    assert cursor.fetchone() is None

def test_fetchmany_raises_if_no_execute(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(Connection(client))
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchmany()

def test_fetchmany_raises_on_closed_cursor(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(Connection(client))
    cursor.close()
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchmany()

#----------------------------------------------------------------
# rowcount tests
#----------------------------------------------------------------

def test_rowcount_returns_minus_one_if_no_execute(client):
    cursor = Cursor(Connection(client))
    assert cursor.rowcount == -1

def test_rowcount_returns_zero_if_no_rows_found(        
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    cursor = Cursor(Connection(client))
    database_id, inserted_rows = generate_pages(client, 1000)
    execute_query_returns_no_rows(cursor, row_description, database_id)
    
    assert cursor.rowcount == 0

def test_rowcount_returns_count_of_all_rows_found(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    n_pages = 100
    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n_pages)
    execute_query_returns_all_rows(cursor, row_description, database_id)

    assert n_pages == cursor.rowcount

#----------------------------------------------------------------
# drain-all pagination tests (issue #325)
#----------------------------------------------------------------

def test_execute_drains_every_page_into_one_result_set(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    # Arrange: 5 matching rows, but force the backend to hand them back two at a
    # time. page_size lives in the caller payload here purely to provoke a 2+2+1
    # token walk cheaply — in production callers omit it and the client defaults
    # to 100, where the same drain loop still applies above 100 rows.
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=5)
    cursor._inject_description(row_description)

    payload = {
        "page_size": 2,
        "filter": {
            "property": "is_active",
            "checkbox": {"does_not_equal": True},
        },
    }

    # Act: a SINGLE execute() must transparently follow next_cursor across all
    # three pages — the caller never sees pagination.
    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={
            "path_params": {"database_id": database_id},
            "payload": payload,
        },
    )
    rows = cursor.fetchall()

    # Assert: all five rows surfaced, in insertion order, as ONE result set.
    # (If a page were modelled as its own ResultSet, fetchall would see only the
    # first 2 rows even though rowcount summed to 5 — so the names pin order AND
    # the single-result-set shape, not just the count.)
    # pages_create strips properties to ids only, so retrieve each created page
    # to recover the title values for the expected side.
    created_pages = [
        client("pages", "retrieve", path_params={"page_id": p["id"]}) for p in pages
    ]
    expected_names = [
        rich_text_to_plain_text(p["properties"]["name"]["title"]) for p in created_pages
    ]
    actual_names = [rich_text_to_plain_text(get_name(r)["title"]) for r in rows]
    assert actual_names == expected_names
    assert cursor.rowcount == 5
    assert cursor.fetchone() is None      # result set fully consumed, no second set

    # And the drain rebuilt the body per fetch: the moving start_cursor was never
    # smeared onto the caller's shared payload dict.
    assert "start_cursor" not in payload

#----------------------------------------------------------------
# lazy streaming pagination tests (issue #326)
#----------------------------------------------------------------

class _CallCountingClient:
    """Transparent proxy that counts databases.query calls and delegates the rest.

    Laziness is only observable by *counting backend calls* — row totals look
    identical whether we drained eagerly or streamed. So we spy on the one call
    the cursor makes (``self._client(endpoint, request, ...)``) and forward
    everything else (``_add``, ``pages_create``, ``_ensure_root`` …) untouched.
    """

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.query_calls = 0

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "databases" and request == "query":
            self.query_calls += 1
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class _PayloadSpyClient:
    """Transparent proxy that records the payload of each databases.query call.

    ``page_size`` is internal (derived from ``yield_per``), so the only way to
    observe it is to capture the request body the backend actually received.
    """

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.query_payloads = []

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "databases" and request == "query":
            self.query_payloads.append(payload)
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def test_streaming_execute_fetches_only_first_page(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    # Arrange: 5 matching rows handed back two at a time (page_size=2 forces a
    # 2+2+1 token walk). A counting proxy records how many databases.query calls
    # actually reach the backend.
    database_id, _ = generate_pages(client, n=5)
    counting = _CallCountingClient(client)
    cursor = Cursor(Connection(counting))
    cursor._inject_description(row_description)

    payload = {
        "page_size": 2,
        "filter": {
            "property": "is_active",
            "checkbox": {"does_not_equal": True},
        },
    }

    # Act: stream the result — execute() must pull page 1 only.
    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={
            "path_params": {"database_id": database_id},
            "payload": payload,
        },
        stream_results=True,
        yield_per=2,
    )

    # Assert: exactly one backend call so far. Drain-all would have made three
    # (2+2+1); streaming defers pages 2 and 3 until they're fetched.
    assert counting.query_calls == 1


def test_streaming_pulls_next_page_on_demand(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    # Arrange: 5 rows, page_size 2 → backend pages of 2, 2, 1. Stream them.
    database_id, _ = generate_pages(client, n=5)
    counting = _CallCountingClient(client)
    cursor = Cursor(Connection(counting))
    cursor._inject_description(row_description)
    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={
            "path_params": {"database_id": database_id},
            "payload": {
                "page_size": 2,
                "filter": {"property": "is_active", "checkbox": {"does_not_equal": True}},
            },
        },
        stream_results=True,
        yield_per=2,
    )

    # The first page (2 rows) is already buffered — draining it makes no new call.
    assert cursor.fetchone() is not None
    assert cursor.fetchone() is not None
    assert counting.query_calls == 1

    # The third row lives on page 2: fetching it must pull exactly one more page,
    # and surface real data (not None — which is what dropping the iterator gives).
    third = cursor.fetchone()
    assert third is not None
    assert counting.query_calls == 2


def test_streaming_rowcount_is_unknown_until_fully_drained(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    # rowcount must NOT leak the buffered page length while a stream is mid-flight.
    # #326 contract: rowcount stays -1 ("unknown") until every page has been pulled,
    # then reports the true sum. 5 rows, page_size 2 → pages of 2, 2, 1.
    database_id, _ = generate_pages(client, n=5)
    counting = _CallCountingClient(client)
    cursor = Cursor(Connection(counting))
    cursor._inject_description(row_description)
    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={
            "path_params": {"database_id": database_id},
            "payload": {
                "page_size": 2,
                "filter": {"property": "is_active", "checkbox": {"does_not_equal": True}},
            },
        },
        stream_results=True,
        yield_per=2,
    )

    # Mid-stream: only page 1 (2 rows) is buffered, but rowcount must read -1 —
    # NOT 2. Reporting the buffered length is the explicit failure mode in #326.
    assert cursor.rowcount == -1

    # Drain the whole stream one row at a time (fetchone drives the lazy page pull).
    drained = 0
    while cursor.fetchone() is not None:
        drained += 1
    assert drained == 5

    # Fully retrieved: rowcount now reports the true total across all pages.
    assert cursor.rowcount == 5


def test_yield_per_implies_streaming(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    # Specifying a batch size is itself the opt-in to lazy streaming: passing
    # yield_per WITHOUT stream_results must still defer pages 2 and 3. 5 rows,
    # page_size 2 → a drain-all would make three calls (2+2+1).
    database_id, _ = generate_pages(client, n=5)
    counting = _CallCountingClient(client)
    cursor = Cursor(Connection(counting))
    cursor._inject_description(row_description)
    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={
            "path_params": {"database_id": database_id},
            "payload": {
                "page_size": 2,
                "filter": {"property": "is_active", "checkbox": {"does_not_equal": True}},
            },
        },
        yield_per=2,
    )

    # No stream_results kwarg, yet only one backend call so far: yield_per implied it.
    assert counting.query_calls == 1


def test_streaming_fetchall_returns_all_rows_in_order(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    # End-to-end: a streamed read must surface EVERY row across EVERY page, in
    # insertion order — the same observable result as a drain-all, just pulled
    # lazily. 5 rows, page_size 2 → pages of 2, 2, 1. fetchall() is the canonical
    # full read, so it must drive the lazy page pull to completion and NOT stop at
    # the buffered first page (which is what list(current_rs) gives today).
    cursor = Cursor(Connection(client))
    database_id, pages = generate_pages(client, n=5)
    cursor._inject_description(row_description)

    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={
            "path_params": {"database_id": database_id},
            "payload": {
                "page_size": 2,
                "filter": {"property": "is_active", "checkbox": {"does_not_equal": True}},
            },
        },
        stream_results=True,
        yield_per=2,
    )

    rows = cursor.fetchall()

    # pages_create strips properties to ids only, so retrieve each created page to
    # recover the title values for the expected side (same shape as the drain-all test).
    created_pages = [
        client("pages", "retrieve", path_params={"page_id": p["id"]}) for p in pages
    ]
    expected_names = [
        rich_text_to_plain_text(p["properties"]["name"]["title"]) for p in created_pages
    ]
    actual_names = [rich_text_to_plain_text(get_name(r)["title"]) for r in rows]
    assert actual_names == expected_names
    assert cursor.rowcount == 5
    assert cursor.fetchone() is None      # fully consumed across all pages


class _FailOnPageNClient:
    """Transparent proxy that raises a NotionError on the ``fail_on``-th query.

    Same wrap-and-delegate shape as ``_CallCountingClient``, but the chosen
    ``databases.query`` call fails instead of returning a page — reproducing a
    backend failure that strikes *mid-stream*, when a later page is pulled.
    """

    def __init__(self, wrapped, fail_on=2, code="rate_limited"):
        self._wrapped = wrapped
        self.fail_on = fail_on
        self.code = code
        self.query_calls = 0

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "databases" and request == "query":
            self.query_calls += 1
            if self.query_calls == self.fail_on:
                raise NotionError("rate limited", status_code=429, code=self.code)
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def test_streaming_fetchall_propagates_a_mid_stream_page_failure_as_dbapi_error(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    """``Cursor.fetchall`` drains a stream to completion — and when a page it must
    pull fails, it surfaces the *mapped* DBAPI error, not the raw NotionError.

    The first two mid-stream slices fixed the lazy pull (``fetchone`` /
    ``_try_fetch_next``) and the engine drain façade (``_iter_all``). This pins the
    *third* page-pull loop: ``Cursor.fetchall``'s own ``while not exhausted`` drain
    (``dbapi2.py`` ~582). No engine ``CursorResult`` surface routes through it —
    ``all()``/``fetchall()``/iteration all go via ``_iter_all`` — but it is the
    PEP 249 public ``fetchall``, so a direct DBAPI consumer streaming a result and
    calling it must get the same honest propagation contract as every other pull
    point: ``rate_limited`` -> ``OperationalError`` with the ``NotionError`` as
    ``__cause__`` (routed through ``Cursor._translate_notion_error``).

    5 rows, ``page_size``/``yield_per`` 2 -> backend pages of 2, 2, 1; the fake
    fails the 2nd ``databases.query``. ``execute`` buffers page 1; ``fetchall``
    then drains and hits the injected failure pulling page 2.

    Failure modes fenced off (each collapses the ``raises`` assertion):
        - **Raw leak** (today): the drain's ``next(self._page_iter)`` is unwrapped,
          so the unmapped ``NotionError`` escapes -> not an ``OperationalError``.
        - **Swallow-to-truncated**: the drain stops quietly at the failed page and
          ``fetchall`` returns just the 2 buffered rows -> nothing raises.
        - **Skip-and-continue**: page 2 is dropped, the drain resumes at page 3 ->
          rows come back instead of an error.
    """
    failing = _FailOnPageNClient(client, fail_on=2)
    cursor = Cursor(Connection(failing))
    database_id, _ = generate_pages(client, n=5)
    cursor._inject_description(row_description)

    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={
            "path_params": {"database_id": database_id},
            "payload": {
                "page_size": 2,
                "filter": {"property": "is_active", "checkbox": {"does_not_equal": True}},
            },
        },
        stream_results=True,
        yield_per=2,
    )

    # Page 1 buffered (1 call); fetchall must drain, and pulling page 2 fails.
    with pytest.raises(OperationalError) as excinfo:
        cursor.fetchall()

    assert isinstance(excinfo.value.__cause__, NotionError)
    assert excinfo.value.__cause__.code == "rate_limited"


class _MalformedPageClient:
    """Minimal client whose ``databases.query`` returns a malformed page.

    ``has_more=True`` with ``next_cursor=None`` is the contradiction
    ``PageIterator`` rejects: it can't honor "there are more pages" without a
    cursor to fetch them. The iterator raises ``ValueError`` on the pull, which
    ``execute()`` must translate into a DBAPI ``DatabaseError`` rather than leak.
    """

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "databases" and request == "query":
            return {"object": "list", "results": [], "has_more": True, "next_cursor": None}
        raise AssertionError(f"unexpected backend call: {endpoint}.{request}")


def test_execute_translates_malformed_page_into_dbapi_database_error(
    row_description: tuple[tuple, ...],
):
    """A malformed page during ``execute()``'s eager drain surfaces as a
    ``DatabaseError`` carrying a *descriptive* message, not a bare error.

    This pins the Slice-2 contract (a malformed Notion result becomes a DBAPI
    Error instead of leaking ``ValueError``) — a translation path that lived only
    in ``execute()``'s inline ``except ValueError`` and had no direct test. It
    also guards the consolidation that routes ``execute()`` through
    ``_translate_notion_error``: that helper's ``ValueError`` branch produces the
    descriptive message, so this test is the discriminator between the old bare
    ``DatabaseError()`` and the unified message.

    ``has_more=True`` + ``next_cursor=None`` makes ``PageIterator.__next__`` raise
    ``ValueError`` on the very first (page-1) pull inside ``execute()``'s drain.

    Failure modes fenced off:
        - **Raw leak**: the ``ValueError`` escapes untranslated -> not a
          ``DatabaseError``.
        - **Uninformative wrap**: a bare ``DatabaseError()`` with no message ->
          the ``"has_more"``/``"start_cursor"`` assertions fail, leaving operators
          to guess what broke.
    """
    cursor = Cursor(Connection(_MalformedPageClient()))
    cursor._inject_description(row_description)

    with pytest.raises(DatabaseError) as excinfo:
        cursor.execute(
            operation={"endpoint": "databases", "request": "query"},
            parameters={
                "path_params": {"database_id": "db-1"},
                "payload": {"filter": {"property": "is_active", "checkbox": {"equals": True}}},
            },
        )

    # The malformed page is the documented cause; the message must name the defect.
    message = str(excinfo.value)
    assert "has_more" in message
    assert "start_cursor" in message
    assert isinstance(excinfo.value.__cause__, ValueError)


def test_yield_per_sets_clamped_request_page_size(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...],
):
    # yield_per IS the internal Notion page_size, clamped to the API max of 100:
    # page_size = min(yield_per or 100, 100). The caller carries NO page_size in
    # the payload — the cursor injects it from yield_per, on a per-request copy
    # (never smearing it onto the caller's dict).
    database_id, _ = generate_pages(client, n=3)
    spy = _PayloadSpyClient(client)
    cursor = Cursor(Connection(spy))
    cursor._inject_description(row_description)

    payload = {"filter": {"property": "is_active", "checkbox": {"does_not_equal": True}}}

    # Sub-cap: yield_per passes straight through as the request page_size.
    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={"path_params": {"database_id": database_id}, "payload": payload},
        yield_per=2,
    )
    assert spy.query_payloads[-1].get("page_size") == 2
    # internal page_size must not be smeared onto the caller's payload dict.
    assert "page_size" not in payload

    # Above-cap: clamped to the Notion API maximum of 100 (so yield_per > 100
    # pulls multiple Notion pages per logical batch).
    cursor.execute(
        operation={"endpoint": "databases", "request": "query"},
        parameters={"path_params": {"database_id": database_id}, "payload": payload},
        yield_per=200,
    )
    assert spy.query_payloads[-1].get("page_size") == 100

#----------------------------------------------------------------
# lastrowid tests
#----------------------------------------------------------------

def test_lastrowid_returns_last_inserted_id(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    n_pages = 100
    cursor = Cursor(Connection(client))
    database_id, inserted_rows = generate_pages(client, n_pages)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    # check generate_pages() for how inserted rows are returned
    last_inserted_rowid = uuid.UUID(inserted_rows[-1]["id"]).int

    assert last_inserted_rowid == cursor.lastrowid

def test_lastrowid_returns_none_if_table_metadata_retrieved(
    client: InMemoryNotionClient,
):
    cursor = Cursor(Connection(client))
    database_id, inserted_rows = generate_pages(client, n=10)
    # retrieve "students" database
    cursor.execute(
        operation={"endpoint": "databases", "request": "retrieve"},
        parameters={"path_params": {"database_id": database_id}}
    )

    metadata = cursor.fetchall()
    assert len(metadata) == 10      # 9 + 1 title property for database name
    assert cursor.lastrowid is None

def test_lastrowid_returns_none_if_no_rows_modified(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    cursor = Cursor(Connection(client))
    database_id, inserted_rows = generate_pages(client, n=10)
    execute_query_returns_no_rows(cursor, row_description, database_id)
    assert len(cursor.fetchall()) == 0
    assert cursor.lastrowid is None

#----------------------------------------------------------------
# executemany / multiple result sets tests
#----------------------------------------------------------------

def build_trash_operation(page_id: str) -> dict:
    return {
        "path_params": {"page_id": page_id},
        "payload": {"in_trash": True},
    }

def return_all_pages_in_database(client: InMemoryNotionClient, database_id: str) -> list[dict]:
    result = client.databases_query(
        path_params={"database_id": database_id},
    )

    return result["results"]

def test_executemany_executes_all_operations(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    """
    executemany() should execute the same operation for all parameter sets.
    """

    # pre-fill the client
    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n=10)
    
    # get all pages belonging to the database
    pages = return_all_pages_in_database(client, database_id)
    params = [build_trash_operation(p["id"]) for p in pages]
    cursor._inject_description(row_description)

    # execute the operation on all pages retrieved by the query
    cursor.executemany(
        operation={
            "endpoint": "pages",
            "request": "update"
        },
        parameters=params
    )

    # verify all pages were modified
    for p in pages:
        stored = client.pages_retrieve({"page_id": p["id"]})
        assert stored["in_trash"] is True


def test_executemany_produces_multiple_result_sets(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    """
    executemany() should produce one result set per operation.
    """

    # pre-fill the client
    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n=10)
    
    pages = return_all_pages_in_database(client, database_id)
    params = [build_trash_operation(p["id"]) for p in pages]

    cursor._inject_description(row_description)

    cursor.executemany(
        operation={
            "endpoint": "pages",
            "request": "update"
        },
        parameters=params
    )

    rows = cursor.fetchall()

    # first result set should contain exactly one updated page
    assert len(rows) == 1


def test_executemany_default_errorhandler_raises_translated_notion_error(
    client: InMemoryNotionClient,
):
    """A NotionError during executemany must route through the cursor's default
    errorhandler (inherited from the connection) and surface as the translated
    DBAPI error.

    Regression for the ``Cursor(connection)`` refactor: the cursor never
    initialised ``_errorhandler`` (the getter would raise AttributeError) and
    the getter read the connection's misspelled ``error_handler`` attribute.
    With no per-cursor handler set, ``cursor.errorhandler`` must fall back to
    ``connection.errorhandler`` (the default), which re-raises ``errorvalue``.
    """
    # no per-cursor errorhandler override -> must use the connection's default
    cursor = Cursor(Connection(client))

    # retrieving a non-existent page makes the client raise a
    # NotionError(code="object_not_found"), translated to ProgrammingError
    with pytest.raises(ProgrammingError):
        cursor.executemany(
            operation={"endpoint": "pages", "request": "retrieve"},
            parameters=[{"path_params": {"page_id": "does-not-exist"}}],
        )


def test_next_moves_to_next_result_set(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    """
    next() should move the cursor to the next result set.
    """

    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n=10)
    pages = return_all_pages_in_database(client, database_id)
    params = [build_trash_operation(p["id"]) for p in pages]

    cursor._inject_description(row_description)

    cursor.executemany(
        operation={
            "endpoint": "pages",
            "request": "update"
        },
        parameters=params
    )

    first = cursor.fetchone()

    cursor.nextset()

    second = cursor.fetchone()

    assert first != second


def test_next_allows_iterating_all_result_sets(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    """
    All result sets should be reachable through next().
    """

    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n=100)
    pages = return_all_pages_in_database(client, database_id)
    params = [build_trash_operation(p["id"]) for p in pages]

    cursor._inject_description(row_description)

    cursor.executemany(
        operation={
            "endpoint": "pages",
            "request": "update"
        },
        parameters=params
    )

    seen_ids = []

    for _ in range(len(pages)):
        row = cursor.fetchone()
        seen_ids.append(row[0])
        cursor.nextset()

    assert len(seen_ids) == len(pages)


def test_next_on_last_result_set_exhausts_results(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    """
    Calling next() after the last result set should exhaust the cursor.
    """

    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n=100)
    pages = return_all_pages_in_database(client, database_id)
    params = [build_trash_operation(p["id"]) for p in pages]

    cursor._inject_description(row_description)

    cursor.executemany(
        operation={
            "endpoint": "pages",
            "request": "update"
        },
        parameters=params
    )

    for _ in range(len(pages)):
        _ = cursor.fetchall()
        cursor.nextset()

    # no more result sets
    assert cursor.fetchone() is None

def test_rowcount_returns_sum_of_resultsets(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    cursor = Cursor(Connection(client))
    database_id, _ = generate_pages(client, n=100)
    pages = return_all_pages_in_database(client, database_id)
    params = [build_trash_operation(p["id"]) for p in pages]

    cursor._inject_description(row_description)

    cursor.executemany(
        operation={
            "endpoint": "pages",
            "request": "update"
        },
        parameters=params
    )

    assert cursor.rowcount == len(pages)