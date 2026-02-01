import pdb
import uuid
import pytest
from normlite.engine.base import create_engine, Engine
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String

@pytest.fixture
def mocked_db_id() -> str:
    return str(uuid.uuid4())

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData, mocked_db_id: str) -> Table:
    students = Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
    )
    students.set_oid(mocked_db_id)      # ensure table is in reflected state
    return students

def create_students_db(engine: Engine) -> str:
    # create a new table students under the user tables page
    db = engine._client._add('database', {
        'parent': {
            'type': 'page_id',
            'page_id': engine._user_tables_page_id
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

    return db.get('id')

def test_engine_execution_options_are_used_as_defaults(students: Table):
    """Engine-level default execution options

    Invariant tested
        - Engine defaults are visible if nothing overrides them.

    """
    engine = create_engine(
        "normlite:///:memory:",
        execution_options={"page_size": 100, "preserve_rowid": False},
    )

    with engine.connect() as conn:
        stmt = select(students)
        result = conn.execute(stmt)

        ctx = result.context

        assert ctx.execution_options["page_size"] == 100
        assert ctx.execution_options["preserve_rowid"] is False

def test_connection_execution_options_override_engine(students: Table):
    """Connection-level overrides engine options.
    
    Invariant tested
        - Connection overrides engine defaults
        - Engine options remain unchanged
    
    """
    engine = create_engine(
        "normlite:///:memory:",
        execution_options={"page_size": 100},
    )

    with engine.connect() as conn:
        conn = conn.execution_options(page_size=50)

        stmt = select(students)
        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["page_size"] == 50

def test_statement_execution_options_override_connection(students: Table):
    """Statement-level execution options override connection.
    
    Invariant tested
        - Statement options override both engine and connection
    """
    engine = create_engine(
        "normlite:///:memory:",
        execution_options={"page_size": 100},
    )

    with engine.connect() as conn:
        conn = conn.execution_options(page_size=50)

        stmt = select(students).execution_options(page_size=10)

        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["page_size"] == 10

def test_execute_time_execution_options_have_highest_precedence(students: Table):
    """execute()-time options override everything.
    
    Invariant tested
        - execute(..., execution_options=...) wins over all others
    """
    engine = create_engine(
        "normlite:///:memory:",
        execution_options={"page_size": 100},
    )

    with engine.connect() as conn:
        conn = conn.execution_options(page_size=50)
        stmt = select(students).execution_options(page_size=10)

        result = conn.execute(stmt, execution_options={"page_size": 5})
        ctx = result.context

        assert ctx.execution_options["page_size"] == 5

def test_execution_options_are_merged_not_replaced(students: Table):
    """Partial override does not erase unrelated options.
    
    Invariant tested
        - Resolution is a merge, not a replacement
    """
    engine = create_engine(
        "normlite:///:memory:",
        execution_options={
            "page_size": 100,
            "preserve_rowid": True,
        },
    )

    with engine.connect() as conn:
        stmt = select(students).execution_options(page_size=10)

        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["page_size"] == 10
        assert ctx.execution_options["preserve_rowid"] is True

def test_execution_options_are_immutable_in_context(students: Table):
    """Execution options are immutable once resolved.

    Invariant tested
        - ExecutionContext freezes execution configuration
    """
    engine = create_engine("normlite:///:memory:")

    with engine.connect() as conn:
        stmt = select(students)
        result = conn.execute(stmt)
        ctx = result.context

        with pytest.raises(TypeError):
            ctx.execution_options["page_size"] = 999

def test_execution_options_are_isolated_per_execution(students: Table):
    """Execution options do not leak between executions
    
    Invariant tested
        - Statement-level options do not persist across executions
    """

    engine = create_engine("normlite:///:memory:")

    with engine.connect() as conn:
        stmt1 = select(students).execution_options(page_size=10)
        stmt2 = select(students)

        ctx1 = conn.execute(stmt1).context
        ctx2 = conn.execute(stmt2).context

        assert ctx1.execution_options.get("page_size") == 10
        assert "page_size" not in ctx2.execution_options

def test_insert_execution_options_are_resolved_correctly(students: Table):
    """Insert-specific execution option example (returning / preserve_rowid)."""
    engine = create_engine(
        "normlite:///:memory:",
        execution_options={"preserve_rowid": True},
    )

    db_id = create_students_db(engine)
    students.set_oid(db_id)

    with engine.connect() as conn:
        stmt = insert(students).execution_options(preserve_rowid=False)

        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["preserve_rowid"] is False

