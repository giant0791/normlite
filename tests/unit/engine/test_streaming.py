"""Engine-level streaming behavior (issue #327, Slice 4 of ADR-0010).

Slice 3 taught the DBAPI ``Cursor`` to stream over token pagination, but the
opt-in lived entirely on ``Cursor.execute(..., stream_results=, yield_per=)``.
Slice 4 plumbs that opt-in down from the engine's ``ExecutionOptions`` cascade
and gates it to the user-facing ``Select`` path. These tests pin the engine-level
observable behavior.
"""

import pytest

from normlite import ForeignKey, Relation
from normlite.engine.base import Engine
from normlite.engine.context import ExecutionContext
from normlite.engine.interfaces import _distill_params
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection, OperationalError
from normlite.notion_sdk.client import NotionError
from normlite.sql.dml import delete, insert, select, update
from normlite.sql.functions import func
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import String

from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)


class _CallCountingClient:
    """Transparent proxy that counts ``data_sources.query`` calls and delegates the rest.

    Laziness is only observable by *counting backend calls* — the row totals look
    identical whether we drained eagerly or streamed. Same idiom as the Slice-3
    DBAPI tests (``tests/unit/notiondbapi/test_dbapi2.py``), lifted to the engine.
    """

    def __init__(self, wrapped):
        self._wrapped = wrapped
        self.query_calls = 0

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "data_sources" and request == "query":
            self.query_calls += 1
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


class _FailOnPageNClient:
    """Transparent proxy that injects a backend failure on the *N*-th page pull.

    Same wrap-and-delegate shape as ``_CallCountingClient``, but the ``fail_on``-th
    ``data_sources.query`` call raises a ``NotionError`` instead of returning a page.
    This is how Slice 5 reproduces a failure that strikes *mid-stream* — after
    ``execute()`` has already handed back page 1, when a later lazy page pull is
    the thing that goes wrong.
    """

    def __init__(self, wrapped, fail_on=2, code="rate_limited"):
        self._wrapped = wrapped
        self.fail_on = fail_on
        self.code = code
        self.query_calls = 0

    def __call__(self, endpoint, request, path_params=None, query_params=None, payload=None):
        if endpoint == "data_sources" and request == "query":
            self.query_calls += 1
            if self.query_calls == self.fail_on:
                raise NotionError("rate limited", status_code=429, code=self.code)
        return self._wrapped(endpoint, request, path_params, query_params, payload)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)


def test_streaming_select_propagates_a_mid_stream_page_failure_as_dbapi_error(
    engine: Engine,
    students: Table,
):
    """A backend failure on page 2 of a streamed ``Select`` surfaces *honestly* at
    the iteration boundary — page-1 rows already in hand, then a mapped DBAPI error.

    Invariant tested (the Slice-5 headline user story)
        - A failure that strikes *after* ``execute()`` — when a later page is
          lazily pulled — must **propagate**, not silently truncate the stream.
        - It must arrive as the **mapped DBAPI class** (``rate_limited`` ->
          ``OperationalError``), with the raw ``NotionError`` preserved as
          ``__cause__`` — i.e. routed through ``Cursor._translate_notion_error``,
          exactly as a page-1 failure raised inside ``execute()`` already is.
        - Rows the caller already pulled (page 1) **stay pulled**: the failure does
          not retroactively empty or rewind the result it already handed back.

    6 rows, ``yield_per=2`` -> backend pages of 2, 2, 2; the fake fails the 2nd
    ``data_sources.query``. The first ``fetchmany(2)`` is satisfied entirely from the
    buffered page 1 (no pull). The second ``fetchmany(2)`` drains the buffer and
    must pull page 2 — which is where the injected failure lands.

    Three failure modes are fenced off (the anti-patterns the slice must avoid):
        - **Raw leak** (today): the page pull raises the unmapped ``NotionError``
          straight up -> ``isinstance(exc, OperationalError)`` is False, no
          ``__cause__`` chain.
        - **Swallow-to-empty**: the pull failure is caught and the stream ends
          quietly -> the second ``fetchmany`` returns ``[]`` and nothing raises.
        - **Skip-and-continue** (``executemany``'s pattern, wrong here): the failed
          page is dropped and iteration jumps to page 3 -> rows surface instead of
          an error; later pages are unreachable past a failure anyway.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    failing = _FailOnPageNClient(engine._client, fail_on=2)
    engine._dbapi_connection = DBAPIConnection(failing)

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(select(students))

        # Page 1 is buffered by execute(): the caller gets these rows cleanly.
        page_one = result.fetchmany(2)
        assert len(page_one) == 2

        # Crossing into page 2 triggers the injected backend failure. It must
        # propagate as the mapped DBAPI class, not the raw NotionError, not [].
        with pytest.raises(OperationalError) as excinfo:
            result.fetchmany(2)

    # The raw backend error is preserved on the chain (raise ... from ne).
    assert isinstance(excinfo.value.__cause__, NotionError)
    assert excinfo.value.__cause__.code == "rate_limited"


def test_streaming_select_all_propagates_a_mid_stream_page_failure_as_dbapi_error(
    engine: Engine,
    students: Table,
):
    """The ``CursorResult.all()`` "materialize everything" façade fails *loudly* when
    a page it must drain goes wrong — same honest propagation as the lazy path.

    Where the first mid-stream test drove the *incremental* surface
    (``fetchmany``, which pulls through ``Cursor.fetchone`` / ``_try_fetch_next``),
    this drives the *drain-to-completion* surface: ``CursorResult.all()`` (and its
    synonyms ``fetchall()`` / ``for row in result``) all funnel through
    ``Cursor._iter_all`` — a *separate* page-pull loop that the first slice did not
    touch.

    Invariant tested
        - ``all()`` is all-or-nothing: if a page it must pull fails, the whole call
          raises the **mapped DBAPI class** (``rate_limited`` -> ``OperationalError``)
          with the raw ``NotionError`` as ``__cause__`` — routed through
          ``Cursor._translate_notion_error`` exactly like the lazy path now is.
        - It must NOT hand back a *truncated* result (page-1 rows dressed up as the
          complete set) and swallow the failure: ``all()`` promises *every* row or
          an error, never a silent short read.

    6 rows, ``yield_per=2`` -> backend pages of 2, 2, 2; the fake fails the 2nd
    ``data_sources.query``. ``execute`` buffers page 1; ``all()`` then drives the drain
    and hits the injected failure pulling page 2.

    Failure modes fenced off (all collapse the ``raises`` assertion):
        - **Raw leak** (today): ``_iter_all``'s ``next(self._page_iter)`` is
          unwrapped, so the unmapped ``NotionError`` escapes -> not an
          ``OperationalError``, no ``__cause__`` chain.
        - **Swallow-to-truncated**: the drain stops quietly at the failed page and
          ``all()`` returns just the 2 buffered rows -> nothing raises.
        - **Skip-and-continue**: page 2 is dropped and the drain resumes at page 3
          -> ``all()`` returns rows instead of raising.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    failing = _FailOnPageNClient(engine._client, fail_on=2)
    engine._dbapi_connection = DBAPIConnection(failing)

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(select(students))

        # all() must drain every page; pulling page 2 hits the injected failure.
        # It surfaces as the mapped DBAPI class, never a truncated 2-row result.
        with pytest.raises(OperationalError) as excinfo:
            result.all()

    assert isinstance(excinfo.value.__cause__, NotionError)
    assert excinfo.value.__cause__.code == "rate_limited"


def test_streaming_select_fetchmany_pulls_only_the_pages_it_needs(
    engine: Engine,
    students: Table,
):
    """A streamed ``Select`` drives ``fetchmany`` across a page boundary, pulling
    exactly the pages it needs and no more.

    Invariant tested
        - ``yield_per`` cascades engine -> connection -> DBAPI cursor (Slice 4
          job 1): the ``Select`` streams instead of draining every page up front.
        - ``fetchmany(n)`` pulls forward across page boundaries to satisfy ``n``
          (closes the Slice-3 gap where ``Cursor.fetchmany`` only read the buffer),
          while leaving later pages unfetched.

    6 rows, ``yield_per=2`` -> request page_size 2 -> backend pages of 2, 2, 2.
    ``fetchmany(3)`` needs the 3rd row, which lives on page 2, so it must pull
    page 2 (the 2nd call) — but page 3 (rows 5-6) must stay unfetched.

    Two failure modes are fenced off by the ``query_calls == 2`` assertion:
        - ``yield_per`` not cascaded: ``page_size`` stays at the engine default
          (100), so all 6 rows arrive in a single page -> only 1 call (today's red).
        - ``Cursor.fetchmany`` buffer-only: it can't reach page 2, so it surfaces
          just the 2 buffered rows (``len(first_three) == 2``).
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    # Count backend page pulls. Seeding is done; only the read below counts.
    counting = _CallCountingClient(engine._client)
    engine._dbapi_connection = DBAPIConnection(counting)

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(select(students))

        first_three = result.fetchmany(3)

    # The 3rd row lives on page 2: fetchmany had to pull it.
    assert len(first_three) == 3
    # Exactly two pages pulled (1 + 2); page 3 (rows 5-6) stays unfetched.
    assert counting.query_calls == 2


def test_streaming_select_all_materializes_by_draining_every_page(
    engine: Engine,
    students: Table,
):
    """A streamed ``Select`` consumed via ``CursorResult.all()`` materializes
    *every* row by draining all remaining pages.

    Invariant tested
        - ``all()`` is the "give me everything" façade: on a streamed result it
          must drive the lazy ``PageIterator`` to exhaustion, not just hand back
          the single page buffered by ``execute`` (Slice-4 failure mode: ``all()``
          short-circuiting the drain).

    6 rows, ``yield_per=2`` -> backend pages of 2, 2, 2. ``execute`` buffers only
    page 1; ``all()`` must pull pages 2 and 3 to return all 6 rows.

    Failure mode fenced off:
        - ``all()`` reads only the buffered result set (today ``Cursor._iter_all``
          iterates ``_result_sets`` and never touches ``_page_iter``): it surfaces
          just the 2 page-1 rows (``len(rows) == 2``, ``query_calls == 1``).
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    counting = _CallCountingClient(engine._client)
    engine._dbapi_connection = DBAPIConnection(counting)

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(select(students))

        rows = result.all()

    # all() materialized the whole stream, not just the buffered first page.
    assert len(rows) == 6
    # It drained by pulling every page (1 + 2 + 3).
    assert counting.query_calls == 3


def test_streaming_survives_a_where_clause(
    engine: Engine,
    students: Table,
):
    """A streamed ``Select`` **with a WHERE** pulls only the pages it needs.

    This is a **safety net**, green today, written to make one thing measurable
    that no other test can see: every streaming test above selects *everything*
    (``select(students)``), so the whole suite is blind to the intersection
    **streaming x pushed predicate**.

    That intersection is precisely where #384 / C2 lands. The recheck needs a
    plain ``SELECT`` to reach the query planner (step 2b), and the planner path
    (``Connection._execute_query_plan``) drains its plan into a list without
    reading ``context.execution_options``. Any routing rule that sends
    *predicated* ``SELECT``s to the planner therefore trades ADR-0010's laziness
    for the recheck — and with only ``select(students)`` under test, it would
    make that trade **silently, with a green suite**.

    Invariant tested
        - The ADR-0010 guarantee is a property of ``SELECT``, not of
          *unfiltered* ``SELECT``: ``yield_per`` still sizes the backend page and
          ``fetchmany`` still stops pulling once satisfied, WHERE or no WHERE.

    Same arithmetic as ``…fetchmany_pulls_only_the_pages_it_needs``, so the two
    differ in exactly one variable: 6 rows all matching ``is_active IS true``,
    ``yield_per=2`` -> pages of 2, 2, 2; ``fetchmany(3)`` reaches into page 2 and
    must leave page 3 alone.

    Failure mode fenced off:
        - The statement is routed somewhere that materializes eagerly: all three
          pages are pulled up front (``query_calls == 3``) even though the caller
          asked for 3 rows.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6, is_active=True)

    counting = _CallCountingClient(engine._client)
    engine._dbapi_connection = DBAPIConnection(counting)

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(
            select(students).where(students.c.is_active.is_(True))
        )

        first_three = result.fetchmany(3)

    # The predicate keeps all 6 rows, so the 3rd still lives on page 2.
    assert len(first_three) == 3
    # Exactly two pages pulled (1 + 2); page 3 stays unfetched.
    assert counting.query_calls == 2


def test_do_execute_ignores_streaming_unless_caller_declares_streamable(
    engine: Engine,
    students: Table,
):
    """``do_execute`` honors the streaming opt-in only when the caller declares
    the statement streamable — the Select-only gate (ADR-0010).

    Invariant tested
        - Streaming is **opt-in and Select-only**: honored only on the
          user-facing ``Select`` read path. Internal full-scan consumers —
          two-phase ``Delete``/``Update`` and join phase-1 (``dml.py`` ~776/857/939)
          — call ``do_execute`` *without* declaring streamability, so an inherited
          ``stream_results=True`` must NOT make them stream. Phase 1 must drain the
          full match set before phase 2 can run.
        - The gate is **explicit**, not green-by-omission: ``do_execute`` itself
          refuses to stream unless told ``streamable``, so a future ``dml.py`` edit
          that forwards ``execution_options`` into a phase-1 call still cannot
          silently start streaming.

    This drives the ``do_execute`` chokepoint directly (mirroring the
    ``dml.py`` phase-1 callers via ``ExecutionContext``): it forwards streaming
    options but leaves ``streamable`` at its default. The operation is a paginated
    ``data_sources.query`` (the only paginated endpoint) purely so streaming-vs-draining
    is observable.

    6 rows, ``yield_per=2`` -> request page_size 2 -> backend pages of 2, 2, 2.
    Drain-all keeps the ``rowcount`` contract: accurate immediately. Streaming
    would leave the page iterator unexhausted -> ``rowcount == -1``.

    Failure mode fenced off:
        - No gate (today): ``do_execute`` passes ``stream_results``/``yield_per``
          straight through, so ``execute`` buffers only page 1 and the page
          iterator stays unexhausted -> ``rowcount == -1``.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    # Build the paginated data_sources.query operation + a description-injected
    # cursor the way the real phase-1 callers do, via the execution context.
    compiled = select(students).compile(engine._sql_compiler)
    cursor = engine.raw_connection().cursor()
    ctx = ExecutionContext(
        engine,
        engine.connect(),
        cursor=cursor,
        compiled=compiled,
        distilled_params=_distill_params(None),
        execution_options={},
    )
    ctx.pre_exec()
    ctx.invoked_stmt._setup_execution(ctx)

    # The caller forwards the inherited streaming options but does NOT declare
    # the statement streamable — exactly the Delete/Update/join phase-1 contract.
    engine.do_execute(
        cursor,
        ctx.operation,
        ctx.parameters,
        execution_options={"stream_results": True, "yield_per": 2},
    )

    # Drain-all preserved: the count is accurate immediately, not -1.
    assert cursor.rowcount == 6
    assert len(cursor.fetchall()) == 6


def test_delete_ignores_streaming_and_reports_accurate_rowcount(
    engine: Engine,
    students: Table,
):
    """A ``Delete`` on a ``stream_results=True, yield_per=2`` connection does not
    stream. It deletes every matched row and reports the full ``rowcount``.

    This is the headline user story -- *streaming never corrupts a mutation* --
    driven end-to-end through the real ``conn.execute`` pipeline.

    Invariant tested (ADR-0023 item 7)
        - Mutations never stream. A ``Delete`` executes through the Query plan
          (``ModifyTable -> [Filter] -> Scan``). The engine streams only when the
          statement ``is_select``, so it drains the plan eagerly and ``rowcount``
          is known when ``execute()`` returns.
        - The DELETE Scan does not take ``yield_per``. It sends one request with
          the default ``page_size`` (100), so the 6 rows arrive in one page.

    Two discriminators, one per half of item 7
        - Engine half: a streamed DELETE result leaves the plan undrained, so
          ``rowcount`` answers ``-1``, not ``6``.
        - Planner half: a Scan that took ``yield_per=2`` would send ``page_size`` 2
          and pull pages 1, 2, 3, so ``query_calls == 3``, not ``1``.
          ``rowcount`` cannot see this leak: ``ModifyTable`` drains its child in
          ``open()``, so every row is still deleted.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    counting = _CallCountingClient(engine._client)
    engine._dbapi_connection = DBAPIConnection(counting)

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(delete(students).where(students.c.is_active.is_(True)))

    # Every matched row was deleted, and the engine drained the plan eagerly.
    assert result.rowcount == 6
    # The Scan sent one request (page_size 100): the connection's yield_per
    # never reached the mutation's read. A leak would surface as 3 pulls.
    assert counting.query_calls == 1


class _QueryingCursorRecordingConnection(DBAPIConnection):
    """DBAPI connection that keeps every cursor that executes an operation.

    Both ``execute()`` (a ``Scan``) and ``executemany()`` (a ``ModifyTable``) count.
    """

    def __init__(self, client):
        super().__init__(client)
        self.querying_cursors = []

    def cursor(self):
        cursor = super().cursor()
        execute = cursor.execute
        executemany = cursor.executemany

        def recording_execute(*args, **kwargs):
            self.querying_cursors.append(cursor)
            return execute(*args, **kwargs)

        def recording_executemany(*args, **kwargs):
            self.querying_cursors.append(cursor)
            return executemany(*args, **kwargs)

        cursor.execute = recording_execute
        cursor.executemany = recording_executemany
        return cursor


def test_closing_a_half_read_streamed_result_closes_the_cursor_that_queried_notion(
    engine: Engine,
    students: Table,
):
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    recording = _QueryingCursorRecordingConnection(engine._client)
    engine._dbapi_connection = recording

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(select(students))
        result.fetchmany(2)
        result.close()

    assert len(recording.querying_cursors) == 1
    assert recording.querying_cursors[0].closed


def test_closing_a_half_read_streamed_join_ordered_by_a_right_key_closes_both_cursors_that_queried_notion(
    engine: Engine,
):
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        physics_oid = (
            conn.execute(
                insert(courses).values(title="Physics").returning(courses.c.object_id)
            ).first().object_id
        )
        astronomy_oid = (
            conn.execute(
                insert(courses).values(title="Astronomy").returning(courses.c.object_id)
            ).first().object_id
        )
        conn.execute(
            insert(students).values(name="Galileo", enrolled_in=[physics_oid, astronomy_oid])
        )

    recording = _QueryingCursorRecordingConnection(engine._client)
    engine._dbapi_connection = recording

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=1)
        result = conn.execute(
            select(students, courses)
            .join(students.c.enrolled_in)
            .order_by(courses.c.title.asc())
        )
        result.fetchmany(1)
        result.close()

    # one cursor queried the left table, one queried the right table
    assert len(recording.querying_cursors) == 2
    assert all(cursor.closed for cursor in recording.querying_cursors)


def test_closing_an_eager_join_result_twice_keeps_both_cursors_that_queried_notion_closed(
    engine: Engine,
):
    """Regression pin, green on arrival (#393).

    The eager path closes the plan after its drain, and the result cursor closes it
    again on ``result.close()``. Every ``close()`` in the chain must therefore be safe
    to call twice. ORDER BY on a right key puts ``Sort`` in the chain too.
    """
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        physics_oid = (
            conn.execute(
                insert(courses).values(title="Physics").returning(courses.c.object_id)
            ).first().object_id
        )
        astronomy_oid = (
            conn.execute(
                insert(courses).values(title="Astronomy").returning(courses.c.object_id)
            ).first().object_id
        )
        conn.execute(
            insert(students).values(name="Galileo", enrolled_in=[physics_oid, astronomy_oid])
        )

    recording = _QueryingCursorRecordingConnection(engine._client)
    engine._dbapi_connection = recording

    with engine.connect() as conn:
        result = conn.execute(
            select(students, courses)
            .join(students.c.enrolled_in)
            .order_by(courses.c.title.asc())
        )
        rows = result.fetchall()
        result.close()
        result.close()

    assert [row.title for row in rows] == ["Astronomy", "Physics"]
    assert len(recording.querying_cursors) == 2
    assert all(cursor.closed for cursor in recording.querying_cursors)


@pytest.mark.parametrize(
    ("make_stmt", "n_querying_cursors"),
    [
        # Project -> Filter -> Scan
        (lambda t: select(t).where(t.c.is_active.is_(True)), 1),
        # Aggregate -> Scan
        (lambda t: select(func.count()).select_from(t), 1),
        # ModifyTable -> Filter -> Scan: the Scan reads, the ModifyTable writes
        (lambda t: delete(t).where(t.c.is_active.is_(True)), 2),
        (lambda t: update(t).values(grade="Z").where(t.c.is_active.is_(True)), 2),
    ],
    ids=["select-where", "aggregate", "delete-where", "update-where"],
)
def test_closing_a_result_closes_every_cursor_its_plan_used_to_reach_notion(
    engine: Engine,
    students: Table,
    make_stmt,
    n_querying_cursors,
):
    """Regression pin, green on arrival (#451).

    ``@abstractmethod`` only checks that an operator defines ``close()``. It does not
    check that ``close()`` forwards to the source. A ``def close(self): pass`` on a
    compound operator still leaks the cursor below it. This pin closes the result of
    each single-table plan shape and checks every cursor the plan sent to Notion.
    The join shape (``HashJoin``, ``Sort``) is pinned by the #393 tests above.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    recording = _QueryingCursorRecordingConnection(engine._client)
    engine._dbapi_connection = recording

    with engine.connect() as conn:
        result = conn.execute(make_stmt(students))
        result.close()

    assert len(recording.querying_cursors) == n_querying_cursors
    assert all(cursor.closed for cursor in recording.querying_cursors)


def test_a_failed_page_pull_during_an_eager_select_closes_the_cursor_that_queried_notion(
    engine: Engine,
    students: Table,
):
    """Leak point A (#453).

    The eager path drains every page before ``execute()`` returns. A failure on
    page 2 raises inside ``execute()``, so the caller gets no result to close.
    The caller must get the mapped DBAPI error, and the cursor must be closed.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    # 150 rows and a Scan page size of 100: page 2 is pulled during the drain.
    populate_students(engine, students, n=150)

    failing = _FailOnPageNClient(engine._client, fail_on=2)
    recording = _QueryingCursorRecordingConnection(failing)
    engine._dbapi_connection = recording

    with engine.connect() as conn:
        with pytest.raises(OperationalError) as excinfo:
            conn.execute(select(students))

    assert isinstance(excinfo.value.__cause__, NotionError)
    assert failing.query_calls == 2
    assert len(recording.querying_cursors) == 1
    assert recording.querying_cursors[0].closed


def test_a_failed_page_pull_during_a_delete_closes_the_cursor_that_queried_notion(
    engine: Engine,
    students: Table,
):
    """Regression pin, green on arrival (#453).

    Leak point A for a DELETE. A DELETE never streams, so it drains every page
    of its Scan before it sends a write. A failure on page 2 raises inside
    ``execute()``, so the caller gets no result to close. The caller must get
    the mapped DBAPI error, and the cursor must be closed.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    # 150 rows and a Scan page size of 100: page 2 is pulled during the drain.
    populate_students(engine, students, n=150)

    failing = _FailOnPageNClient(engine._client, fail_on=2)
    recording = _QueryingCursorRecordingConnection(failing)
    engine._dbapi_connection = recording

    with engine.connect() as conn:
        with pytest.raises(OperationalError) as excinfo:
            conn.execute(delete(students))

    assert isinstance(excinfo.value.__cause__, NotionError)
    assert failing.query_calls == 2
    # The read failed before the first write, so only the Scan cursor queried Notion.
    assert len(recording.querying_cursors) == 1
    assert recording.querying_cursors[0].closed

def test_an_eager_select_closes_the_cursor_that_queried_notion_before_the_caller_closes_the_result(
    engine: Engine,
    students: Table,
):
    """Regression pin, green on arrival (#453).

    The eager path drains every page inside ``execute()``. After the drain, the
    plan has no more work, so it must close its cursor. The caller does not have
    to call ``result.close()`` to release it. An error-only cleanup in
    ``_execute_query_plan`` drops this close, and no other test sees it.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    # 150 rows and a Scan page size of 100: the drain pulls 2 pages.
    populate_students(engine, students, n=150)

    recording = _QueryingCursorRecordingConnection(engine._client)
    engine._dbapi_connection = recording

    with engine.connect() as conn:
        result = conn.execute(select(students))

        assert len(recording.querying_cursors) == 1
        assert recording.querying_cursors[0].closed
        assert len(result.fetchall()) == 150
