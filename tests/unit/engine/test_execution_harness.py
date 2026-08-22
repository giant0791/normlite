"""The shared test harness must dispatch like ``Connection._execute_context``.

``tests/utils/execution.py:run_context`` exists so a test can drive the whole
execution pipeline and keep the :class:`ExecutionContext` afterwards, which
``Connection.execute()`` does not hand back. It pays for that by **duplicating**
the connection's dispatch on ``ctx.execution_style`` — and a duplicate only
stays honest while it covers the same styles.

It does not: it knows ``EXECUTE`` and treats *everything else* as
``EXECUTEMANY``. ``ExecutionStyle.EXECUTEQUERYPLAN`` (#364, joins and
aggregates) therefore falls into the ``else`` and calls ``do_executemany`` with
a ``None`` ``bulk_operation``. The gap is latent only because no ``run_context``
caller has yet passed a join or an aggregate.

This matters beyond tidiness: #384 / C2 step 2b routes *plain* ``SELECT``s
through the planner, at which point most existing ``run_context`` callers hit
this branch at once (measured: 19 of the 24 reds the routing flip produces).
"""

from normlite.engine.base import Engine
from normlite.engine.context import ExecutionStyle
from normlite.sql.dml import select
from normlite.sql.functions import func
from normlite.sql.schema import Table

from tests.utils.db_helpers import (
    attach_table_oid,
    create_students_db,
    populate_students,
)
from tests.utils.execution import run_context, run_execute


def test_run_context_dispatches_a_query_plan_statement_like_connection_execute(
    engine: Engine,
    students: Table,
):
    """A statement routed to ``EXECUTEQUERYPLAN`` produces the same rows through
    the harness as through the real ``Connection.execute()``.

    ``select(func.count()).select_from(students)`` is the cheapest statement
    that reaches ``ExecutionStyle.EXECUTEQUERYPLAN`` today (``context.py``:
    ``stmt.is_select and (stmt._joins or stmt._is_aggregate)``), so it pins the
    branch without needing a join fixture. The ``select_from`` is not optional:
    a columnless ``COUNT(*)`` has no FROM to infer and the compiler rejects it.

    The production result is the oracle, not a hard-coded 3: the claim is
    *"the harness dispatches like the connection"*, and comparing against
    ``run_execute`` is what makes a wrong-branch dispatch visible as a wrong
    **answer** rather than merely as an exception.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    stmt = select(func.count()).select_from(students)

    expected = run_execute(engine, stmt).all()

    result, ctx = run_context(engine, stmt)

    assert ctx.execution_style is ExecutionStyle.EXECUTEQUERYPLAN
    assert [tuple(row) for row in result.all()] == [tuple(row) for row in expected]
