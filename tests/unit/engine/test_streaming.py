"""Engine-level streaming behavior (issue #327, Slice 4 of ADR-0010).

Slice 3 taught the DBAPI ``Cursor`` to stream over token pagination, but the
opt-in lived entirely on ``Cursor.execute(..., stream_results=, yield_per=)``.
Slice 4 plumbs that opt-in down from the engine's ``ExecutionOptions`` cascade
and gates it to the user-facing ``Select`` path. These tests pin the engine-level
observable behavior.
"""

import pytest

from normlite.engine.base import Engine
from normlite.engine.context import ExecutionContext
from normlite.engine.interfaces import _distill_params
from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection, OperationalError
from normlite.notion_sdk.client import NotionError
from normlite.sql.dml import delete, select
from normlite.sql.schema import Table

from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)


class _CallCountingClient:
    """Transparent proxy that counts ``databases.query`` calls and delegates the rest.

    Laziness is only observable by *counting backend calls* — the row totals look
    identical whether we drained eagerly or streamed. Same idiom as the Slice-3
    DBAPI tests (``tests/unit/notiondbapi/test_dbapi2.py``), lifted to the engine.
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


class _FailOnPageNClient:
    """Transparent proxy that injects a backend failure on the *N*-th page pull.

    Same wrap-and-delegate shape as ``_CallCountingClient``, but the ``fail_on``-th
    ``databases.query`` call raises a ``NotionError`` instead of returning a page.
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
        if endpoint == "databases" and request == "query":
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
    ``databases.query``. The first ``fetchmany(2)`` is satisfied entirely from the
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
    ``databases.query``. ``execute`` buffers page 1; ``all()`` then drives the drain
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
    ``databases.query`` (the only paginated endpoint) purely so streaming-vs-draining
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

    # Build the paginated databases.query operation + a description-injected
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


def test_two_phase_delete_ignores_streaming_and_reports_accurate_rowcount(
    engine: Engine,
    students: Table,
):
    """A two-phase ``Delete`` on a ``stream_results=True`` connection still drains
    its full phase-1 match set as one page and deletes every matched row.

    This is the slice's headline user story — *streaming never corrupts a
    mutation* — driven end-to-end through the real ``conn.execute`` pipeline (not
    the synthetic ``do_execute`` call of the gate test above).

    Invariant tested
        - Streaming is **Select-only** (ADR-0010). A ``Delete`` is dispatched on
          the EXECUTEMANY channel; its phase-1 read (``dml.py`` ~939) calls
          ``do_execute`` *without* declaring streamability, so the connection's
          inherited ``stream_results``/``yield_per`` must NOT reach it. Phase 1
          must run as a single drain-all page (request ``page_size`` 100), never
          fragmented by ``yield_per``.
        - Correctness: every matched row reaches phase 2, so ``rowcount`` is the
          full match count, not just one streamed page.

    Why ``query_calls`` is the discriminator, not ``rowcount`` alone
        Phase 1 immediately ``fetchall()``s its rows (``dml.py:947``), and
        ``fetchall`` drains the page iterator to exhaustion. So even a *broken*
        gate that streamed phase 1 would recover all rows on that drain and still
        report ``rowcount == 6`` — ``rowcount`` can't see the leak. The leak shows
        up only in *how many backend pages* phase 1 pulled: an inherited
        ``yield_per=2`` would fragment the request into ``page_size`` 2, so the
        ``fetchall`` drain would pull pages 2 and 3 as well.

    6 active rows, connection ``yield_per=2``:
        - gate honored (today): phase-1 request ``page_size`` 100 -> all 6 rows in
          one page -> ``query_calls == 1``.
        - gate broken: phase-1 forwards ``yield_per=2`` -> ``page_size`` 2 ->
          ``fetchall`` drains pages 1, 2, 3 -> ``query_calls == 3``.

    Failure mode fenced off:
        - Select-only gate bypassed for mutations: a future ``dml.py`` edit that
          threads ``execution_options``/``streamable`` into the Delete phase-1
          call fragments the mutation's read -> ``query_calls == 3``.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=6)

    counting = _CallCountingClient(engine._client)
    engine._dbapi_connection = DBAPIConnection(counting)

    with engine.connect() as conn:
        conn = conn.execution_options(stream_results=True, yield_per=2)
        result = conn.execute(delete(students).where(students.c.is_active.is_(True)))

    # Every matched row was deleted: phase 1 saw the whole match set, not a page.
    assert result.rowcount == 6
    # Phase 1 drained as a single page (page_size 100): the connection's yield_per
    # never reached the mutation's read. A leak would surface as 3 pulls.
    assert counting.query_calls == 1
