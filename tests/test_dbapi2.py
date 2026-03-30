from operator import itemgetter
import pdb
import uuid

import pytest
from faker import Faker

from normlite._constants import SpecialColumns
from normlite.notion_sdk.client import InMemoryNotionClient
from normlite.notion_sdk.getters import rich_text_to_plain_text
from normlite.notiondbapi.dbapi2 import Cursor, ProgrammingError
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode

@pytest.fixture
def row_description() -> tuple[tuple, ...]:
    return (
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None,),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
        (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
        (SpecialColumns.NO_CREATED_TIME, DBAPITypeCode.TIMESTAMP, None, None, None, None, None,),
        ("name", DBAPITypeCode.TITLE, None, None, None, None, None,),
        ("id", DBAPITypeCode.NUMBER, None, None, None, None, None,),
        ("is_active", DBAPITypeCode.CHECKBOX, None, None, None, None, None,),
        ("start_on", DBAPITypeCode.DATE, None, None, None, None, None,),
        ("grade", DBAPITypeCode.RICH_TEXT, None, None, None, None, None,),
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
    cursor = Cursor(client)

    assert cursor.description is None

def test_description_none_if_no_rows(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(client)
    database_id, pages = generate_pages(client, n=10)
    execute_query_returns_no_rows(cursor, row_description, database_id)

    assert cursor.description is None

def test_description_none_on_closed_cursor(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(client)
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
    cursor = Cursor(client)
    database_id, pages = generate_pages(client, n=10)
    cursor._inject_description(row_description)
    execute_query_returns_no_rows(cursor, row_description, database_id)
    row = cursor.fetchone()

    assert row is None

def test_fetchone_returns_first_row_found(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(client)
    database_id, pages = generate_pages(client, n=10)
    cursor._inject_description(row_description)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    first_page = client("pages", "retrieve", path_params={"page_id": pages[0]["id"]})
    first = cursor.fetchone()

    assert rich_text_to_plain_text(get_name(first)) == rich_text_to_plain_text(
        first_page["properties"]["name"]["title"]
    )
    assert not cursor.closed

def test_fetchone_consumes_cursor(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(client)
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

    assert rich_text_to_plain_text(get_name(first)) == rich_text_to_plain_text(
        first_page["properties"]["name"]["title"]
    )
    assert rich_text_to_plain_text(get_name(second)) == rich_text_to_plain_text(
        second_page["properties"]["name"]["title"]
    )
    assert rich_text_to_plain_text(get_name(third)) == rich_text_to_plain_text(
        third_page["properties"]["name"]["title"]
    )
    assert should_be_none is None
    assert not cursor.closed

def test_fetchone_raises_on_closed_cursor(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(client)
    database_id, _ = generate_pages(client, n=3)
    cursor._inject_description(row_description)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    cursor.close()

    with pytest.raises(ProgrammingError):
        _ = cursor.fetchone()

def test_fetchone_raises_if_no_execute(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(client)
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchone()

#----------------------------------------------------------------
# fetchall tests
#----------------------------------------------------------------

def test_fetchall_returns_all_rows_found(
    client: InMemoryNotionClient, 
    row_description: tuple[tuple, ...],
):
    cursor = Cursor(client)
    database_id, pages = generate_pages(client, n=100)
    cursor._inject_description(row_description)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    first_page = client("pages", "retrieve", path_params={"page_id": pages[0]["id"]})
    last_page = client("pages", "retrieve", path_params={"page_id": pages[-1]["id"]})
    rows = cursor.fetchall()
    should_be_none = cursor.fetchone()

    assert len(rows) == 100
    assert rich_text_to_plain_text(get_name(rows[0])) == rich_text_to_plain_text(
        first_page["properties"]["name"]["title"]
    )
    assert rich_text_to_plain_text(get_name(rows[-1])) == rich_text_to_plain_text(
        last_page["properties"]["name"]["title"]
    )
    assert should_be_none is None
    assert not cursor.closed

def test_fetchall_raises_if_no_execute(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(client)
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchall()

def test_fetchall_raises_on_closed_cursor(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(client)
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
    cursor = Cursor(client)
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
    cursor = Cursor(client)
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchmany()

def test_fetchmany_raises_on_closed_cursor(
    client: InMemoryNotionClient, 
):
    cursor = Cursor(client)
    cursor.close()
    with pytest.raises(ProgrammingError):
        _ = cursor.fetchmany()

#----------------------------------------------------------------
# rowcount tests
#----------------------------------------------------------------

def test_rowcount_returns_minus_one_if_no_execute(client):
    cursor = Cursor(client)
    assert cursor.rowcount == -1

def test_rowcount_returns_zero_if_no_rows_found(        
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    cursor = Cursor(client)
    database_id, inserted_rows = generate_pages(client, 1000)
    execute_query_returns_no_rows(cursor, row_description, database_id)
    
    assert cursor.rowcount == 0

def test_rowcount_returns_count_of_all_rows_found(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    n_pages = 100
    cursor = Cursor(client)
    database_id, _ = generate_pages(client, n_pages)
    execute_query_returns_all_rows(cursor, row_description, database_id)

    assert n_pages == cursor.rowcount

#----------------------------------------------------------------
# lastrowid tests
#----------------------------------------------------------------

def test_lastrowid_returns_last_inserted_id(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    n_pages = 100
    cursor = Cursor(client)
    database_id, inserted_rows = generate_pages(client, n_pages)
    execute_query_returns_all_rows(cursor, row_description, database_id)
    # check generate_pages() for how inserted rows are returned
    last_inserted_rowid = uuid.UUID(inserted_rows[-1]["id"]).int

    assert last_inserted_rowid == cursor.lastrowid

def test_lastrowid_returns_none_if_table_metadata_retrieved(
    client: InMemoryNotionClient,
):
    cursor = Cursor(client)
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
    cursor = Cursor(client)
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
    cursor = Cursor(client)
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
    cursor = Cursor(client)
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


def test_next_moves_to_next_result_set(
    client: InMemoryNotionClient,
    row_description: tuple[tuple, ...]
):
    """
    next() should move the cursor to the next result set.
    """

    cursor = Cursor(client)
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

    cursor = Cursor(client)
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

    cursor = Cursor(client)
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
    cursor = Cursor(client)
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