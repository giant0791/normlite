import pdb
import uuid
import pytest
from normlite.engine.base import create_engine, Engine
from normlite.exceptions import ArgumentError
from normlite.sql.dml import insert, select
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String

from tests.utils.db_helpers import create_students_db, attach_table_oid, populate_students

def test_engine_execution_options_defaults(engine: Engine, students: Table):
    """Engine-level default execution options

    Invariant tested
        - Engine defaults are visible if nothing overrides them.

    """

    opts = engine.get_execution_options()
    assert opts["page_size"] == 100
    assert opts["preserve_rowid"] is False
    assert opts["preserve_rowcount"] is True
    assert opts["implicit_returning"] is False

def test_engine_execution_options_are_used_as_defaults(engine: Engine, students: Table):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with engine.connect() as conn:
        stmt = select(students)
        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["page_size"] == 100
        assert ctx.execution_options["preserve_rowid"] is False
        assert ctx.execution_options["preserve_rowcount"] is True
        assert ctx.execution_options["implicit_returning"] is False

def test_connection_execution_options_overrides_engine(engine: Engine, students: Table):
    """Connection-level overrides engine options.
    
    Invariant tested
        - Connection overrides engine defaults
        - Engine options remain unchanged
    
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with engine.connect() as conn:
        conn = conn.execution_options(page_size=50, preserve_rowcount=False)
        stmt = select(students)
        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["page_size"] == 50
        assert ctx.execution_options["preserve_rowcount"] is False

def test_statement_execution_options_overrides_connection(engine: Engine, students: Table):
    """Statement-level execution options override connection.
    
    Invariant tested
        - Statement options override both engine and connection
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with engine.connect() as conn:
        conn = conn.execution_options(page_size=50)

        stmt = select(students).execution_options(page_size=10, preserve_rowid=True)

        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["page_size"] == 10
        assert ctx.execution_options["preserve_rowid"] is True

def test_execute_time_execution_options_have_highest_precedence(engine: Engine, students: Table):
    """execute()-time options override everything.
    
    Invariant tested
        - execute(..., execution_options=...) wins over all others
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with engine.connect() as conn:
        conn = conn.execution_options(page_size=50)
        stmt = select(students).execution_options(page_size=10)

        result = conn.execute(stmt, execution_options={"page_size": 5})
        ctx = result.context

        assert ctx.execution_options["page_size"] == 5

def test_execution_options_are_merged_not_replaced(engine: Engine, students: Table):
    """Partial override does not erase unrelated options.
    
    Invariant tested
        - Resolution is a merge, not a replacement
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with engine.connect() as conn:
        stmt = select(students).execution_options(page_size=10)

        result = conn.execute(stmt)
        ctx = result.context

        assert ctx.execution_options["page_size"] == 10
        assert ctx.execution_options["preserve_rowid"] is False

def test_execution_options_are_immutable_in_context(engine: Engine, students: Table):
    """Execution options are immutable once resolved.

    Invariant tested
        - ExecutionContext freezes execution configuration
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with engine.connect() as conn:
        stmt = select(students)
        result = conn.execute(stmt)
        ctx = result.context

        with pytest.raises(TypeError):
            ctx.execution_options["page_size"] = 999

def test_execution_options_are_isolated_per_execution(engine: Engine, students: Table):
    """Execution options do not leak between executions
    
    Invariant tested
        - Statement-level options do not persist across executions
    """

    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with engine.connect() as conn:
        stmt1 = select(students).execution_options(page_size=10)
        stmt2 = select(students)

        ctx1 = conn.execute(stmt1).context
        ctx2 = conn.execute(stmt2).context

        assert ctx1.execution_options.get("page_size") == 10
        assert ctx2.execution_options.get("page_size") == 100

@pytest.mark.parametrize('option', 
    [
        {"isolation_level": "AUTOCOMMIT"},
        {"compiled_cache": {"some_cache": []}},
    ], 
    ids=lambda opt: list(opt.keys())[0]
)
def test_setting_option_at_wrong_level_raises(option: dict, engine: Engine, students: Table):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)

    with pytest.raises(ArgumentError):
        stmt = insert(students).execution_options(**option)
