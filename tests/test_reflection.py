import json
import pdb
import pytest

from normlite._constants import SpecialColumns
from normlite.engine.base import Engine, Inspector, create_engine
from normlite.engine.context import ExecutionContext
from normlite.engine.cursor import CursorResult
from normlite.engine.reflection import ReflectedTableInfo
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.base import DDLCompiled
from normlite.sql.schema import MetaData, Table

@pytest.fixture
def database_retrieved() -> dict:
    json_obj = """
    {
        "object": "database",
        "id": "bc1211ca-e3f1-4939-ae34-5260b16f627c",
        "created_time": "2021-07-08T23:50:00.000Z",
        "last_edited_time": "2021-07-08T23:50:00.000Z",
        "cover": {
            "type": "external",
            "external": {
                "url": "https://website.domain/images/image.png"
            }
        },
        "url": "https://www.notion.so/bc1211cae3f14939ae34260b16f627c",
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "Grocery List",
                    "link": null
                },
                "annotations": {
                    "bold": false,
                    "italic": false,
                    "strikethrough": false,
                    "underline": false,
                    "code": false,
                    "color": "default"
                },
                "plain_text": "Grocery List",
                "href": null
            }
        ],
        "description": [
            {
                "type": "text",
                "text": {
                    "content": "Grocery list for just kale",
                    "link": null
                },
                "annotations": {
                    "bold": false,
                    "italic": false,
                    "strikethrough": false,
                    "underline": false,
                    "code": false,
                    "color": "default"
                },
                "plain_text": "Grocery list for just kale",
                "href": null
            }
        ],
        "properties": {
            "Price": {
                "id": "evWq",
                "name": "Price",
                "type": "number",
                "number": {
                    "format": "dollar"
                }
            },
            "Description": {
                "id": "V}lX",
                "name": "Description",
                "type": "rich_text",
                "rich_text": {}
            },
            "Name": {
                "id": "title",
                "name": "Name",
                "type": "title",
                "title": {}
            }
        },
        "parent": {
            "type": "page_id",
            "page_id": "98ad959b-2b6a-4774-80ee-00246fb0ea9b"
        },
        "archived": false,
        "in_trash": false,
        "is_inline": false,
        "public_url": null
    }
    """
    return json.loads(json_obj)

@pytest.fixture
def engine() -> Engine:
    return create_engine(
        'normlite:///:memory:',
        _mock_ws_id = '12345678-0000-0000-1111-123456789012',
        _mock_ischema_page_id = 'abababab-3333-3333-3333-abcdefghilmn',
        _mock_tables_id = '66666666-6666-6666-6666-666666666666',
        _mock_db_page_id = '12345678-9090-0606-1111-123456789012'
    )

@pytest.fixture
def inspector(engine: Engine) -> Inspector:
    return engine.inspect()

def create_students_db(engine: Engine) -> None:
    # create a new table students in memory
    db = engine._client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': engine._db_page_id
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
            'student_id': {'number': {}},
            'name': {'title': {}},
            'grade': {'rich_text': {}},
            'is_active': {'checkbox': {}}
        }
    })

    # add the students to tables
    engine._client._add('page', {
        'parent': {
            'type': 'database_id',
            'database_id': engine._tables_id
        },
        'properties': {
            'table_name': {'title': [{'text': {'content': 'students'}}]},
            'table_schema': {'rich_text': [{'text': {'content': ''}}]},
            'table_catalog': {'rich_text': [{'text': {'content': 'memory'}}]},
            'table_id': {'rich_text': [{'text': {'content': db.get('id')}}]}
        }
    })

def make_result_set(dbapi_cursor: Cursor) -> Cursor:
    dbapi_cursor._parse_result_set({
        "object": "database",
        "id": '680dee41-b447-451d-9d36-c6eaff13fb45',
        "archived": False,
        "in_trash": False,
        "title": [{"text": {"content": "students"}}],
        "properties": {
            "id": {"id": "%3AUPp","type": "number", "number": {}},
            "grade": {"id": "A%40Hk", "type": "rich_text", "rich_text": {}},
            "name": {"id": "BJXS", "type": "title", "title": {}},
        },
    })

def test_reflect_table_info(database_retrieved: dict):
    reflected_table_info = ReflectedTableInfo.from_dict(database_retrieved)
    
    assert reflected_table_info.id == 'bc1211ca-e3f1-4939-ae34-5260b16f627c'
    assert reflected_table_info.name == 'Grocery List'
    assert len(reflected_table_info.get_columns()) == 7
    assert reflected_table_info.get_column_names(include_all=False) == ['Price', 'Description', 'Name']
  
def test_rows_as_reflected_columns_no_comp(engine: Engine):
    dbapi_cursor = engine.raw_connection().cursor()
    dbapi_cursor._description = [
        ('column_name', DBAPITypeCode.META_COL_NAME, None, None, None, None, None,),
        ('column_type', DBAPITypeCode.META_COL_TYPE, None, None, None, None, None,),
        ('column_id', DBAPITypeCode.META_COL_ID, None, None, None, None, None,),
        ('column_value', DBAPITypeCode.META_COL_VALUE, None, None, None, None, None,),
    ]

    dbapi_cursor._result_set = [
        (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, "bc1211ca-e3f1-4939-ae34-5260b16f627c",),
        (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX, None, False),
        (SpecialColumns.NO_TITLE, DBAPITypeCode.TITLE, None, {"title": [{"text": {"content": "Grocery List"}}]},),
        ("Name", DBAPITypeCode.TITLE, "title", {},),
        ("Price", DBAPITypeCode.NUMBER_DOLLAR, "evWq", {},),
        ("Description", DBAPITypeCode.RICH_TEXT, "V}lX", {}),
    ]

    compiled = DDLCompiled(None, {'parameters': {}})
    result = CursorResult(dbapi_cursor, compiled)
    rows = result.all()
    assert len(rows) == 6
    assert rows[0].column_name == SpecialColumns.NO_ID
    assert rows[0].column_value == 'bc1211ca-e3f1-4939-ae34-5260b16f627c'
    assert rows[1].column_name == SpecialColumns.NO_ARCHIVED
    assert not rows[1].column_value
    assert rows[3].column_name == 'Name'

def test_rows_as_reflected_columns_w_comp(engine: Engine):
    dbapi_cursor = engine.raw_connection().cursor()
    make_result_set(dbapi_cursor)
    compiled = DDLCompiled(None, {'parameters': {}})
    result = CursorResult(dbapi_cursor, compiled)
    rows = result.all()
    assert len(rows) == 7
    assert rows[0].column_name == SpecialColumns.NO_ID
    assert rows[0].column_value == '680dee41-b447-451d-9d36-c6eaff13fb45'
    assert rows[1].column_name == SpecialColumns.NO_ARCHIVED
    assert not rows[1].column_value
    assert rows[4].column_name == 'id'

def test_get_columns_sys_tables(inspector: Inspector):
    sys_tables_columns = inspector.get_columns('tables')
    assert sys_tables_columns[0].value == '66666666-6666-6666-6666-666666666666'

def test_reflect_sys_table_w_ddl(inspector: Inspector):
    metadata = MetaData()
    sys_tables = Table('tables', metadata)
    reflected_table_info = inspector.reflect_table(sys_tables)

    assert 'table_name' in sys_tables.c
    assert 'table_schema' in sys_tables.c
    assert 'table_catalog' in sys_tables.c
    assert 'table_id' in sys_tables.c
    assert len(sys_tables.primary_key.c) == 1
    assert sys_tables.primary_key.c._no_id == sys_tables.c._no_id

def test_reflect_user_table_w_ddl(inspector: Inspector):
    create_students_db(inspector._engine)
    metadata = MetaData()
    students = Table('students', metadata)
    reflected_table_info = inspector.reflect_table(students)

    assert 'student_id' in students.c
    assert 'name' in students.c
    assert 'grade' in students.c
    assert 'is_active' in students.c
    assert len(students.primary_key.c) == 1
    assert students.primary_key.c._no_id == students.c._no_id

def test_reflect_sys_tables_w_autoload(engine: Engine):
    metadata = MetaData()
    sys_tables = Table('tables', metadata, autoload_with=engine)
    assert 'table_name' in sys_tables.c
    assert 'table_schema' in sys_tables.c
    assert 'table_catalog' in sys_tables.c
    assert 'table_id' in sys_tables.c
    assert len(sys_tables.primary_key.c) == 1
    assert sys_tables.primary_key.c._no_id == sys_tables.c._no_id

def test_reflect_user_table_w_autoload(engine: Engine):
    create_students_db(engine)
    metadata = MetaData()
    students = Table('students', metadata, autoload_with=engine)

    assert 'student_id' in students.c
    assert 'name' in students.c
    assert 'grade' in students.c
    assert 'is_active' in students.c
    assert len(students.primary_key.c) == 1
    assert students.primary_key.c._no_id == students.c._no_id
