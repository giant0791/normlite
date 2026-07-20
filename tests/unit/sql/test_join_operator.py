"""Pure-compute tests for the Join Volcano operator (issue #363).

The Join operator is a VolcanoOperator: it is driven only through the
``open`` / ``next`` / ``close`` contract. In #363 it does NOT drive any I/O —
its two inputs are supplied by child operators, and the test feeds rows
through trivial in-memory sources (no cursor, no engine, no port). The row
shapes mirror ``tests/unit/sql/test_join_execution.py`` — the pure-compute
arrange pattern the ADR mandates for #363.
"""

from normlite import Relation, ForeignKey
from normlite.sql.dml import Join
from normlite.sql.queryplan import HashJoin
from normlite.sql.resultschema import SchemaInfo
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Integer, String


class _RowSource:
    """In-memory VolcanoOperator child: yields one fixed batch, then exhaustion.

    Stands in for whatever operator produces this side's rows (a Scan, a
    parametrised retrieve, …) so the Join can be driven as pure compute.
    """

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self._drained = False

    def open(self, connection) -> None:
        pass

    def execute_with(self, parameters) -> None:
        # A parametrised leaf (the right retrieve) is driven by execute_with
        # instead of open; a plain source ignores the batch.
        pass

    def next(self):
        if self._drained:
            return None
        self._drained = True
        return self._rows

    def close(self) -> None:
        pass


class _RecordingSource:
    """VolcanoOperator child that records the lifecycle calls it receives, in
    order, so the way a parent drives it can be observed.

    A spy across the open/next/close contract — there is no other way to see
    whether a parent opened and closed its child.
    """

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows
        self._drained = False
        self.events: list[str] = []
        self.received_params = None

    def open(self, connection) -> None:
        self.events.append("open")

    def execute_with(self, parameters) -> None:
        # The parametrised drive: the retrieve batch arrives here, mid-parent
        # next(), NOT at open() — its params depend on the drained left side.
        self.events.append("execute_with")
        self.received_params = parameters

    def next(self):
        self.events.append("next")
        if self._drained:
            return None
        self._drained = True
        return self._rows

    def close(self) -> None:
        self.events.append("close")


def test_join_operator_merges_child_rows_into_joined_tuples_as_one_batch():
    # Arrange: a students->courses inner join whose two sides are supplied by
    # two in-memory child operators. The left source yields raw phase-1 student
    # rows — "Galileo" enrolled in a real course ("c-astro"), "Phantom" pointing
    # at a dangling id ("c-ghost") with no matching course. The right source
    # yields the single course that comes back ("Astronomy" / "c-astro").
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
    join = Join(students, courses, students.c.enrolled_in)

    # The projection a join select carries: all user columns, left then right.
    projection = [*students.uc, *courses.uc]

    left_schema = SchemaInfo.from_table(
        students,
        execution_names=[students.c.object_id.name],
        projected_names=[c.name for c in students.uc],
    )
    right_schema = SchemaInfo.from_table(
        courses,
        execution_names=[courses.c.object_id.name],
        projected_names=[c.name for c in courses.uc],
    )

    def left_row(name: str, oids: list[str], oid: str) -> tuple:
        cells = [None] * len(left_schema.columns)
        cells[left_schema.column_index("name")] = {"title": name}
        cells[left_schema.column_index("enrolled_in")] = {
            "relation": [{"id": o} for o in oids]
        }
        cells[left_schema.column_index("object_id")] = oid
        return tuple(cells)

    def right_row(title: str, oid: str) -> tuple:
        cells = [None] * len(right_schema.columns)
        cells[right_schema.column_index("title")] = title
        cells[right_schema.column_index("object_id")] = oid
        return tuple(cells)

    left_source = _RowSource([
        left_row("Galileo Galilei", ["c-astro"], "s-1"),
        left_row("Phantom Student", ["c-ghost"], "s-2"),  # dangling FK
    ])
    right_source = _RowSource([
        right_row("Astronomy", "c-astro"),
    ])

    # Act: build the Join operator over its two child operators and drive it
    # through the VolcanoOperator contract only — the in-memory children ignore
    # the connection, so None stands in for it (no I/O here).
    join_op = HashJoin(
        left_source,
        right_source,
        join,
        projection,
        right_filter=None,
        right_sorts=None,
    )
    join_op.open(None)
    first = join_op.next()
    second = join_op.next()
    join_op.close()

    # Assert: the whole merged result arrives as a single batch — exactly one
    # row, Galileo paired with Astronomy; the dangling Phantom is dropped by
    # inner-join semantics — and then the operator reports exhaustion.
    assert first is not None
    assert len(first) == 1
    assert {"title": "Galileo Galilei"} in first[0]
    assert "Astronomy" in first[0]
    assert second is None


def test_join_operator_opens_both_children_but_runs_the_right_only_via_execute_with():
    # Arrange: a students->courses join whose two sides are recording child
    # operators. The rows are irrelevant here (both empty), so the merge is a
    # no-op — this test watches only the lifecycle. The two sides are driven
    # DIFFERENTLY, because the right leaf's pages.retrieve parameters do not
    # exist until the left side has been drained and prepared:
    #   - the LEFT leaf is a self-contained scan: open() it (which mints its own
    #     cursor and executes it), then pull it;
    #   - the RIGHT leaf is parametrised: open() mints and shapes its OWN cursor
    #     but does NOT execute (it is an EXECUTEMANY leaf whose retrieve params
    #     aren't known until the left drains) — the leaf is then RUN by
    #     execute_with(batch) mid-next(), before it is pulled.
    # HashJoin.open() forwards the same connection to both leaves; each owns its
    # cursor. Both are closed when the parent closes. So open() no longer means
    # "execute" for the right leaf: it mints/shapes, and the run is deferred to
    # execute_with.
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
    join = Join(students, courses, students.c.enrolled_in)
    projection = [*students.uc, *courses.uc]

    left_source = _RecordingSource([])
    right_source = _RecordingSource([])

    # Act: drive the full VolcanoOperator lifecycle once.
    join_op = HashJoin(left_source, right_source, join, projection)
    join_op.open(None)
    join_op.next()
    join_op.close()

    # Assert: the left leaf is opened before it is pulled and closed at the end.
    assert "open" in left_source.events
    assert left_source.events.index("open") < left_source.events.index("next")
    assert left_source.events[-1] == "close"

    # The right leaf IS opened — but as an EXECUTEMANY leaf open() only mints and
    # shapes its cursor, it does not execute: its retrieve params aren't known
    # until the left drains, so its RUN is deferred to execute_with, which lands
    # before the pull.
    assert "open" in right_source.events
    assert "execute_with" in right_source.events
    assert right_source.events.index("open") < right_source.events.index("execute_with")
    assert right_source.events.index("execute_with") < right_source.events.index("next")
    assert right_source.events[-1] == "close"


def test_join_operator_surfaces_the_merged_collision_qualified_schema():
    # Arrange: a students->courses join where BOTH sides carry a user column
    # named "title". Once the two sides are assembled the merged layout cannot
    # keep two bare "title" columns, so the joined schema qualifies each by its
    # table ("students.title" / "courses.title") while the unique "enrolled_in"
    # stays bare. That collision-qualified layout is a property of the join
    # itself, computed when it assembles its output — it must not be thrown away.
    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
    )
    students = Table(
        "students",
        metadata,
        Column("title", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )
    join = Join(students, courses, students.c.enrolled_in)
    projection = [*students.uc, *courses.uc]

    left_schema = SchemaInfo.from_table(
        students,
        execution_names=[students.c.object_id.name],
        projected_names=[c.name for c in students.uc],
    )
    right_schema = SchemaInfo.from_table(
        courses,
        execution_names=[courses.c.object_id.name],
        projected_names=[c.name for c in courses.uc],
    )

    def left_row(title: str, oids: list[str], oid: str) -> tuple:
        cells = [None] * len(left_schema.columns)
        cells[left_schema.column_index("title")] = {"title": title}
        cells[left_schema.column_index("enrolled_in")] = {
            "relation": [{"id": o} for o in oids]
        }
        cells[left_schema.column_index("object_id")] = oid
        return tuple(cells)

    def right_row(title: str, oid: str) -> tuple:
        cells = [None] * len(right_schema.columns)
        cells[right_schema.column_index("title")] = title
        cells[right_schema.column_index("object_id")] = oid
        return tuple(cells)

    left_source = _RowSource([left_row("Galileo Galilei", ["c-astro"], "s-1")])
    right_source = _RowSource([right_row("Astronomy", "c-astro")])

    # Act: drive the join once, then ask it for the schema of what it produced.
    join_op = HashJoin(left_source, right_source, join, projection)
    join_op.open(None)
    join_op.next()
    join_op.close()

    # Assert: the merged schema is surfaced, in projection order, with the two
    # colliding "title" columns qualified by their table and "enrolled_in" bare.
    names = [entry[0] for entry in join_op.result_schema.as_sequence()]
    assert names == ["students.title", "enrolled_in", "courses.title"]


def test_filter_operator_keeps_passing_rows_and_drops_failures_and_phantoms():
    # Arrange: a students->courses OUTER join's already-merged rows, fed by an
    # in-memory child (the merge itself is not under test here). The residual
    # right-side WHERE `courses.rank > 100` must be answered over the RIGHT slice
    # of each merged row, keyed by the bare property name the compiled Notion
    # filter references. Three rows exercise the three outcomes:
    #   - Astronomy (rank 150) passes and is kept,
    #   - Botany (rank 50) fails the predicate and is dropped,
    #   - an outer-join phantom (right slice all None) is dropped BEFORE the
    #     predicate even runs, by the all-None guard (a NULL right side fails
    #     every right-side predicate; see ADR-0005).
    # The row cells are the raw phase shapes production actually merges (title as
    # the retrieve list-of-text shape, number as {"number": n}) so the operator's
    # page-building ({"type": typ, **cell}) sees real input.
    from normlite.sql.queryplan import Filter

    metadata = MetaData()
    courses = Table(
        "courses",
        metadata,
        Column("title", String(is_title=True)),
        Column("rank", Integer()),
    )
    students = Table(
        "students",
        metadata,
        Column("name", String(is_title=True)),
        Column("enrolled_in", Relation(), ForeignKey("courses.object_id")),
    )

    # The projection a join select carries, and the merged (joined) schema the
    # rows below are laid out under — both pure, computed without any rows.
    projection = [*students.uc, *courses.uc]
    merged_schema = SchemaInfo.from_join(students, courses, *projection)

    def merged_row(name, oids, title, rank) -> tuple:
        cells = [None] * len(merged_schema.columns)
        cells[merged_schema.column_index("name")] = {"title": name}
        cells[merged_schema.column_index("enrolled_in")] = {
            "relation": [{"id": o} for o in oids]
        }
        cells[merged_schema.column_index("title")] = (
            None if title is None else {"title": [{"text": {"content": title}}]}
        )
        cells[merged_schema.column_index("rank")] = (
            None if rank is None else {"number": rank}
        )
        return tuple(cells)

    rows = [
        merged_row("Galileo Galilei", ["c-astro"], "Astronomy", 150),  # keep
        merged_row("Kepler", ["c-bot"], "Botany", 50),                 # drop: fails
        merged_row("Phantom Student", ["c-ghost"], None, None),        # drop: phantom
    ]
    source = _RowSource(rows)

    # The bound Notion filter dict for `courses.c.rank > 100`.
    right_filter = {"property": "rank", "number": {"greater_than": 100}}

    # Act: drive the Filter over its child through the VolcanoOperator contract.
    filter_op = Filter(
        source,
        filter=right_filter,
        schema=merged_schema,
        table=courses,
    )
    filter_op.open(cursor=None)
    first = filter_op.next()
    second = filter_op.next()
    filter_op.close()

    # Assert: only the passing row survives; the failure and the phantom are gone;
    # then the operator reports exhaustion.
    assert first is not None
    assert len(first) == 1
    assert {"title": "Galileo Galilei"} in first[0]
    assert second is None


def test_join_operator_feeds_the_right_child_the_retrieve_batch_it_computes_mid_next():
    # THE DRIVING SEAM. The right leaf is a dependent scan: its pages.retrieve
    # parameters are the deduped set of target ids the LEFT rows point at, which
    # only exist after the left side is drained. JoinExecution.prepare(left_rows)
    # already computes exactly that batch -- and today HashJoin.next() throws its
    # return away (`_ = ...prepare(...)`). The join must instead CAPTURE it and
    # hand it to the right child via execute_with, before pulling the right child.
    #
    # Two students both enrolled in the SAME course ("c-astro") prove the batch is
    # prepare()'s output, not a raw echo: the two identical FKs must collapse to a
    # SINGLE retrieve envelope. The right child is a spy: it records the batch it
    # is handed and yields one canned course row so the merge can still be checked.
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
    join = Join(students, courses, students.c.enrolled_in)
    projection = [*students.uc, *courses.uc]

    left_schema = SchemaInfo.from_table(
        students,
        execution_names=[students.c.object_id.name],
        projected_names=[c.name for c in students.uc],
    )
    right_schema = SchemaInfo.from_table(
        courses,
        execution_names=[courses.c.object_id.name],
        projected_names=[c.name for c in courses.uc],
    )

    def left_row(name: str, oids: list[str], oid: str) -> tuple:
        cells = [None] * len(left_schema.columns)
        cells[left_schema.column_index("name")] = {"title": name}
        cells[left_schema.column_index("enrolled_in")] = {
            "relation": [{"id": o} for o in oids]
        }
        cells[left_schema.column_index("object_id")] = oid
        return tuple(cells)

    def right_row(title: str, oid: str) -> tuple:
        cells = [None] * len(right_schema.columns)
        cells[right_schema.column_index("title")] = title
        cells[right_schema.column_index("object_id")] = oid
        return tuple(cells)

    left_source = _RowSource([
        left_row("Galileo Galilei", ["c-astro"], "s-1"),
        left_row("Johannes Kepler", ["c-astro"], "s-2"),  # same course as Galileo
    ])
    right_source = _RecordingSource([right_row("Astronomy", "c-astro")])

    # Act: drive the join. open() opens the right child (forwarding the
    # connection) but must NOT run it — its retrieve params don't exist yet; the
    # batch is computed and delivered via execute_with inside next().
    join_op = HashJoin(left_source, right_source, join, projection)
    join_op.open(None)

    # the right leaf IS opened at open() — but as an EXECUTEMANY leaf it only
    # mints/shapes its cursor; its execute_with (the actual run) still waits for
    # next(), below.
    assert "open" in right_source.events
    assert "execute_with" not in right_source.events  # opened, not yet run

    first = join_op.next()
    second = join_op.next()
    join_op.close()

    # Assert: the right child was handed EXACTLY the deduped retrieve batch that
    # prepare() computes from the left rows -- one envelope for "c-astro", not two
    # -- and it was handed that batch before it was pulled.
    assert right_source.received_params == [{"path_params": {"page_id": "c-astro"}}]
    assert right_source.events.index("execute_with") < right_source.events.index("next")

    # ... and the merge still produces both students joined to the one course.
    assert first is not None
    assert len(first) == 2
    assert all("Astronomy" in row for row in first)
    assert second is None
