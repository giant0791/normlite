from datetime import date
import json
from pathlib import Path
from typing import Literal
import pytest
from normlite._constants import SpecialColumns
from normlite.engine.base import Engine, create_engine
from normlite.notion_sdk.client import AbstractNotionClient, InMemoryNotionClient
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.base import _CompileState, CompilerState
from normlite.sql.compiler import NotionCompiler
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

# conftest.py

def pytest_configure(config):
    # The first argument is the section ('markers')
    # The second argument is the marker name and its description
    config.addinivalue_line(
        "markers", "filter_proc: marks tests related to filter value processing"
    )

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
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData) -> Table:
    return Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("id", Integer()),
        Column("is_active", Boolean()),
        Column("start_on", Date()),
        Column("grade", String()),
    )

@pytest.fixture
def insert_values() -> dict:
    return dict(
        name="Galileo Galilei",
        id=123456,
        is_active=False,
        start_on=date(1690, 1, 1),
        grade="A",
    )

@pytest.fixture
def engine() -> Engine:
    engine = create_engine("normlite:///:memory:")
    engine.execution_options(
        preserve_rowcount=True, 
        preserve_rowid=False,
        isolation_level="AUTOCOMMIT",
        implicit_returning=False,
        page_size=100,
    )
    return engine

# fixture for testing the expression compiler
@pytest.fixture
def prod_compiler() -> NotionCompiler:
    nc = NotionCompiler()
    # IMPORTANT: Set the correct compile state expected in the visit_* method
    nc._compiler_state = CompilerState(compile_state=_CompileState.COMPILING_WHERE)
    return nc


@pytest.fixture(scope="session")
def api_key() -> str:
    # This is a fake key, read the real one from env variable
    return 'ntn_abc123def456ghi789jkl012mno345pqr'

@pytest.fixture(scope="session")
def ischema_page_id() -> str:
    # This is a fake page id for the info schema page, read the real one from an env variable.
    # Remember that as of today, the Notion API only supports creating pages into 
    # **existing** pages.
    return '680dee41-b447-451d-9d36-c6eaff13fb46'

@pytest.fixture(scope="session")
def client() -> AbstractNotionClient:
    return InMemoryNotionClient()

# Load the fixture file once
@pytest.fixture(scope="module")
def json_fixtures():
    fixture_path = Path(__file__).parent / "fixtures.json"
    with fixture_path.open() as f:
        return json.load(f)
    

# =================================================================
# Helper accessor methods
# =================================================================
class PropertyAccessor:
    """Helper class for accessing property values to be used in a session fixture."""
    def get_text_property_value(self, name: str, type_: Literal['title', 'rich_text'], obj: dict) -> str:
        return obj['properties'][name][type_][0]['text']['content']

    def get_number_property_value(self, name: str, obj: dict) -> int:
        return obj['properties'][name]['number']

    def get_db_prop_type(self, name: str, obj: dict) -> str:
        return obj['properties'][name].get('type', None)

    def get_page_title(self, obj: dict) -> str:
        return obj['properties']['Title']['title'][0]['text']['content']

@pytest.fixture(scope='session')
def paccessor() -> PropertyAccessor:
    return PropertyAccessor()
