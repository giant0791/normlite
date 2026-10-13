import pytest

from normlite.notiondbapi.dbapi2 import Connection as DBAPIConnection, ProgrammingError
from normlite.sql.dml import select
from normlite.sql.queryplan import ModifyTable, Scan
from normlite.sql.compiler import ROWS_UPDATE
from normlite.sql.resultschema import SchemaInfo

from tests.utils.spies import PagesUpdateRecordingClient
from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)


class _BatchedSource:
    """A ``VolcanoOperator`` that yields fixed batches under a fixed schema.

    It stands in for ``[Filter] -> Scan`` under a ``ModifyTable``. More than one
    batch lets a test see when the operator above reads them.
    """

    def __init__(self, schema: SchemaInfo, batches: list[list[tuple]]) -> None:
        self._schema = schema
        self._batches = batches
        self._pending = []
        self.closed = False

    def open(self, connection) -> None:
        self._pending = [list(batch) for batch in self._batches]

    def next(self):
        if not self._pending:
            return None
        return self._pending.pop(0)

    def close(self) -> None:
        self.closed = True

    @property
    def result_schema(self) -> SchemaInfo:
        return self._schema


def test_modify_table_sends_every_write_before_open_returns(engine, students):
    # ADR-0023 item 2. The writes are complete when `open()` returns. No
    # consumer -- a stream, an early close -- may leave a DELETE half done, so
    # the count of `pages.update` requests is read right after `open()`,
    # before any `next()`.
    #
    # Arrange: five real pages, and a child that yields their object_ids in
    # three batches (2, 2, 1). A ModifyTable that writes one batch per
    # `next()` sends 0 writes by this point; one that writes only the first
    # batch in `open()` sends 2.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=5)

    with engine.connect() as conn:
        result = conn.execute(select(students.c.object_id))
        object_ids = [row.object_id for row in result]

    schema = SchemaInfo.from_table(students, execution_names=["object_id"])
    child = _BatchedSource(
        schema,
        [
            [(object_ids[0],), (object_ids[1],)],
            [(object_ids[2],), (object_ids[3],)],
            [(object_ids[4],)],
        ],
    )
    modify = ModifyTable(
        child,
        operation=ROWS_UPDATE,
        parameters={"payload": {"in_trash": True}},
        schema=schema,
    )

    spy = PagesUpdateRecordingClient(engine._client)
    connection = DBAPIConnection(spy)

    # Act: open the operator, and nothing else.
    modify.open(connection)

    # Assert: one write per page the child yields is already sent.
    assert len(spy.updates_seen) == 5


def test_modify_table_yields_one_returning_row_per_written_page(engine, students):
    # ADR-0023 items 3 and 4. `next()` gives back one row per written page.
    # The rows come from the `pages.update` responses, in the RETURNING
    # schema -- never from the child.
    #
    # Arrange: three real pages, so three writes. The child knows only the
    # object_ids, at index 0. The RETURNING schema asks for the title too, and
    # puts `object_id` at index 1. The titles can only come from Notion.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    with engine.connect() as conn:
        result = conn.execute(select(students.c.object_id))
        object_ids = [row.object_id for row in result]

    child_schema = SchemaInfo.from_table(students, execution_names=["object_id"])
    returning_schema = SchemaInfo.from_table(
        students, projected_names=["name", "object_id"]
    )
    child = _BatchedSource(
        child_schema,
        [
            [(object_ids[0],), (object_ids[1],)],
            [(object_ids[2],)],
        ],
    )
    modify = ModifyTable(
        child,
        operation=ROWS_UPDATE,
        parameters={"payload": {"in_trash": True}},
        schema=returning_schema,
    )

    connection = DBAPIConnection(engine._client)

    # Act: open the operator, then read every batch until `None`.
    modify.open(connection)
    rows = []
    while (batch := modify.next()) is not None:
        rows.extend(batch)
    modify.close()

    # Assert: one row per written page, keyed by object_id, with the title
    # cell that Notion sent back.
    get_object_id = returning_schema.column_getter("object_id")
    get_name = returning_schema.column_getter("name")

    assert len(rows) == 3
    assert sorted(get_object_id(row) for row in rows) == sorted(object_ids)
    assert sorted(
        get_name(row)["title"][0]["plain_text"] for row in rows
    ) == ["name_0", "name_1", "name_2"]


def test_modify_table_sends_the_bound_payload_with_each_page_id(engine, students):
    # ADR-0023 item 5. The Planner binds the payload once. ModifyTable adds
    # only `{"page_id": object_id}` per row: each child row gives exactly one
    # `pages.update`, with its own page_id and the bound payload unchanged.
    #
    # Arrange: three real pages. The child rows carry `id` before `object_id`,
    # so the page_id must be read by name, not by position.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=3)

    with engine.connect() as conn:
        result = conn.execute(select(students.c.id, students.c.object_id))
        child_rows = [(row.id, row.object_id) for row in result]

    child_schema = SchemaInfo.from_table(
        students, execution_names=["id", "object_id"]
    )
    returning_schema = SchemaInfo.from_table(students, execution_names=["object_id"])
    child = _BatchedSource(child_schema, [child_rows[:2], child_rows[2:]])

    parameters = {"payload": {"in_trash": True}}
    modify = ModifyTable(
        child,
        operation=ROWS_UPDATE,
        parameters=parameters,
        schema=returning_schema,
    )

    spy = PagesUpdateRecordingClient(engine._client)
    connection = DBAPIConnection(spy)

    # Act
    modify.open(connection)
    modify.close()

    # Assert: one request per page, each with its own page_id and the bound
    # payload. The caller's parameters are not changed.
    expected = sorted(
        (({"page_id": object_id}, {"in_trash": True}) for _, object_id in child_rows),
        key=lambda update: update[0]["page_id"],
    )
    sent = sorted(spy.updates_seen, key=lambda update: update[0]["page_id"])

    assert sent == expected
    assert parameters == {"payload": {"in_trash": True}}


def test_modify_table_writes_every_page_when_the_child_pages_through_notion(engine, students):
    # ADR-0023 item 2, the Halloween problem. The child here is a real Scan
    # that asks Notion for 2 rows per page. Notion paginates by offset over a
    # result it computes again on every request, and it drops trashed pages.
    # If a page is trashed before the Scan asks for the next page, the offset
    # moves past rows that were never read, and those rows are never written.
    #
    # Arrange: five real pages; a Scan over all of them, 2 rows per page.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=5)

    schema = SchemaInfo.from_table(students, execution_names=["object_id"])
    scan = Scan(
        {"endpoint": "data_sources", "request": "query"},
        {
            "path_params": {"data_source_id": students.get_data_source_id()},
            "payload": {},
        },
        schema=schema,
        yield_per=2,
    )
    modify = ModifyTable(
        scan,
        operation=ROWS_UPDATE,
        parameters={"payload": {"in_trash": True}},
        schema=schema,
    )

    # Act: the DELETE runs to the end.
    modify.open(DBAPIConnection(engine._client))
    while modify.next() is not None:
        pass
    modify.close()

    # Assert: no page survives.
    with engine.connect() as conn:
        survivors = conn.execute(select(students.c.object_id)).all()

    assert survivors == []


def test_modify_table_open_raises_on_a_failed_write_and_keeps_the_earlier_writes(engine, students):
    # ADR-0023 item 6. ModifyTable is not atomic. A failed write raises at
    # once from `open()`, through the default errorhandler. The writes before
    # it stay, and no write after it is sent.
    #
    # Arrange: two real pages, and a child that yields a page that does not
    # exist between them.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=2)

    with engine.connect() as conn:
        result = conn.execute(select(students.c.object_id))
        first, second = [row.object_id for row in result]

    missing = "00000000-0000-0000-0000-000000000000"
    schema = SchemaInfo.from_table(students, execution_names=["object_id"])
    child = _BatchedSource(schema, [[(first,), (missing,), (second,)]])
    modify = ModifyTable(
        child,
        operation=ROWS_UPDATE,
        parameters={"payload": {"in_trash": True}},
        schema=schema,
    )

    spy = PagesUpdateRecordingClient(engine._client)

    # Act / Assert: the failed write raises from `open()`.
    with pytest.raises(ProgrammingError):
        modify.open(DBAPIConnection(spy))

    # Assert: the write before the failure stays; the one after is never sent.
    sent = [path_params["page_id"] for path_params, _ in spy.updates_seen]
    assert sent == [first, missing]

    with engine.connect() as conn:
        survivors = [row.object_id for row in conn.execute(select(students.c.object_id))]

    assert survivors == [second]


def test_modify_table_close_closes_its_child(engine, students):
    # Volcano protocol: `close()` on an operator closes its child too. The
    # engine closes only the root of a plan, so a ModifyTable that forgets its
    # child leaves the `[Filter] -> Scan` below it open.
    #
    # Arrange: one real page, and a child that records when it is closed.
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=1)

    with engine.connect() as conn:
        (object_id,) = [row.object_id for row in conn.execute(select(students.c.object_id))]

    schema = SchemaInfo.from_table(students, execution_names=["object_id"])
    child = _BatchedSource(schema, [[(object_id,)]])
    modify = ModifyTable(
        child,
        operation=ROWS_UPDATE,
        parameters={"payload": {"in_trash": True}},
        schema=schema,
    )
    modify.open(DBAPIConnection(engine._client))
    while modify.next() is not None:
        pass

    # Act
    modify.close()

    # Assert: the child is closed.
    assert child.closed is True


def test_modify_table_with_a_child_that_has_no_rows_writes_nothing(engine, students):
    # A DELETE whose WHERE matches no page. The child yields no rows, so
    # ModifyTable sends no `pages.update`, and its first `next()` gives
    # `None` -- never `[]`. The engine stops reading only on `None`.
    #
    # Arrange: a child with no batches at all.
    schema = SchemaInfo.from_table(students, execution_names=["object_id"])
    child = _BatchedSource(schema, [])
    modify = ModifyTable(
        child,
        operation=ROWS_UPDATE,
        parameters={"payload": {"in_trash": True}},
        schema=schema,
    )

    spy = PagesUpdateRecordingClient(engine._client)

    # Act
    modify.open(DBAPIConnection(spy))
    first_batch = modify.next()
    modify.close()

    # Assert: no write is sent, and there is nothing to read.
    assert spy.updates_seen == []
    assert first_batch is None
