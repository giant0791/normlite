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
from normlite.sql.queryplan import HashJoin, Sort
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


class _PaginatedSource:
    """In-memory child that yields its rows across SEVERAL next() batches, one
    per call, then exhaustion -- the way a real Scan paginates a >100-row store
    100 rows at a time (a full scan of 150 rows surfaces as 100 then 50).

    Stands in for a multi-page left leaf so the Join's drain behaviour is
    observable: a source that returned everything in one batch (``_RowSource``)
    could never tell whether the parent drains or stops at the first page.
    """

    def __init__(self, batches: list[list[tuple]]) -> None:
        self._batches = list(batches)
        self._i = 0

    def open(self, connection) -> None:
        pass

    def execute_with(self, parameters) -> None:
        pass

    def next(self):
        if self._i >= len(self._batches):
            return None
        batch = self._batches[self._i]
        self._i += 1
        return batch

    def close(self) -> None:
        pass


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


def test_hash_join_owns_the_merge_without_a_joinexecution_seam():
    # THE FOLD (#378 STEP 2 / ADR-0021 Decision "JoinExecution is deleted"). Slice
    # 1 made HashJoin drain both leaves, but the merge still lived in a
    # JoinExecution the operator held as `self._join_exec` and drove through the
    # dead two-phase `prepare`/`assemble` split. Step 2 folds the surviving merge
    # (`_merge_rows` / `_project_join_row` / the `SchemaInfo.from_join` build)
    # INTO HashJoin, so it merges with no seam object and no prepare.
    #
    # This is a structural red guarded by the merge oracle: the same
    # students->courses inner join the pure-compute tests above drive must keep
    # producing the identical merged row (Galileo/Astronomy, dangling Phantom
    # dropped) AND the operator must no longer hold a JoinExecution. Behaviour is
    # frozen; only the seam disappears.
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
        left_row("Phantom Student", ["c-ghost"], "s-2"),  # dangling FK
    ])
    right_source = _RowSource([
        right_row("Astronomy", "c-astro"),
    ])

    # Act: drive the join once through the Volcano contract.
    join_op = HashJoin(left_source, right_source, join, projection)
    join_op.open(None)
    first = join_op.next()
    second = join_op.next()
    join_op.close()

    # Assert (behaviour oracle): the merge is unchanged — exactly one row,
    # Galileo paired with Astronomy, the dangling Phantom dropped, then exhaustion.
    assert first is not None
    assert len(first) == 1
    assert {"title": "Galileo Galilei"} in first[0]
    assert "Astronomy" in first[0]
    assert second is None

    # Assert (structural): the JoinExecution seam is gone — HashJoin owns the
    # merge itself and holds no `self._join_exec`.
    assert not hasattr(join_op, "_join_exec")


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
    filter_op.open(None)
    first = filter_op.next()
    second = filter_op.next()
    filter_op.close()

    # Assert: only the passing row survives; the failure and the phantom are gone;
    # then the operator reports exhaustion.
    assert first is not None
    assert len(first) == 1
    assert {"title": "Galileo Galilei"} in first[0]
    assert second is None


def test_join_operator_drains_every_left_page_not_just_the_first():
    # THE BLOCKING-JOIN LANDMINE (#364 step 4). A real left Scan paginates: a
    # >100-row left store surfaces as 100 rows, then the rest, then None. But a
    # Join BLOCKS -- it must see EVERY left row before it can build the deduped
    # retrieve batch, exactly as the old dml.py phase-1 did with fetchall(). If
    # HashJoin pulls the left child ONCE (a single fetchmany page) it silently
    # drops every left row past the first page.
    #
    # And the loss is total, not partial: the right retrieve source is
    # single-shot (its pages.retrieve runs once, on the batch prepare() computes
    # from the FIRST left page). So a second left page arrives after the right
    # side is already spent -- there is nothing to join it against, and those
    # students vanish. The Join must DRAIN the left side fully in one next(),
    # prepare the retrieve over ALL left rows, then assemble once.
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

    # Three students all enrolled in the ONE course, split across two left pages
    # (two on the first, one on the second) -- the shape a >100-row scan takes.
    left_source = _PaginatedSource([
        [
            left_row("Galileo Galilei", ["c-astro"], "s-1"),
            left_row("Johannes Kepler", ["c-astro"], "s-2"),
        ],
        [
            left_row("Isaac Newton", ["c-astro"], "s-3"),
        ],
    ])
    right_source = _RowSource([right_row("Astronomy", "c-astro")])

    # Act: drive the join once.
    join_op = HashJoin(left_source, right_source, join, projection)
    join_op.open(None)
    first = join_op.next()
    second = join_op.next()
    join_op.close()

    # Assert: ALL THREE students are joined to Astronomy -- the second left page
    # was drained and merged too, not dropped. A single-page pull would yield
    # only the two first-page students (and the third would be unjoinable, the
    # right side already spent).
    assert first is not None
    assert len(first) == 3
    assert all("Astronomy" in row for row in first)
    for name in ("Galileo Galilei", "Johannes Kepler", "Isaac Newton"):
        assert any({"title": name} in row for row in first)
    assert second is None


class _ScanSource:
    """In-memory stand-in for the NEW right leaf under ADR-0021: a plain, full
    ``data_sources.query`` ``Scan`` of the whole right data source.

    It speaks ONLY the Volcano ``open`` / ``next`` / ``close`` contract and
    paginates its rows one batch per ``next()`` -- exactly like the left Scan.
    Crucially it has **no** ``execute_with``: the right side no longer depends on
    the left (there is no retrieve-by-id batch to bind), so a symmetric drain-both
    ``HashJoin`` must drive it the same way it drives the left, with no parametrised
    hand-off. A source that carried a no-op ``execute_with`` would hide that.
    """

    def __init__(self, batches: list[list[tuple]]) -> None:
        self._batches = list(batches)
        self._i = 0

    def open(self, connection) -> None:
        pass

    def next(self):
        if self._i >= len(self._batches):
            return None
        batch = self._batches[self._i]
        self._i += 1
        return batch

    def close(self) -> None:
        pass


def test_join_operator_drains_every_right_page_not_just_the_first():
    # THE SYMMETRIC-DRAIN LANDMINE (#378 / ADR-0021). The mirror of
    # test_join_operator_drains_every_left_page_not_just_the_first: now the RIGHT
    # side is a full `data_sources.query` Scan of the whole right table, matched
    # client-side by object_id -- NOT a retrieve-by-id of just the referenced
    # pages. A full right scan paginates exactly like the left: a >100-row right
    # store surfaces as 100 rows, then the rest, then None.
    #
    # The old asymmetric HashJoin drained the left, then pulled the right EXACTLY
    # ONCE (one `pages.retrieve` result via `execute_with`), so it only ever saw
    # the right's FIRST page -- and it reached for `execute_with`, which a plain
    # Scan leaf does not have. A symmetric drain-both HashJoin must instead open
    # BOTH independent leaves, DRAIN BOTH fully (the right across all its pages),
    # build the right-by-object_id hash, and probe with each left row's relation
    # array. No `prepare`, no `execute_with`, no single-shot right.
    #
    # Two courses live on DIFFERENT right pages (Astronomy on page 1, Physics on
    # page 2); two students each point at one of them, split across two LEFT pages
    # too. Only a join that drains BOTH sides fully pairs Newton (left page 2) with
    # Physics (right page 2). A single right page (or a single left page) loses one
    # of the two matches.
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

    # Both sides paginate across two pages -- the shape a full scan of a >100-row
    # store takes. The right leaf is a plain Scan (open/next/close), NOT a
    # parametrised retrieve.
    left_source = _ScanSource([
        [left_row("Galileo Galilei", ["c-astro"], "s-1")],
        [left_row("Isaac Newton", ["c-phys"], "s-2")],
    ])
    right_source = _ScanSource([
        [right_row("Astronomy", "c-astro")],
        [right_row("Physics", "c-phys")],
    ])

    # Act: drive the join once. open() opens BOTH leaves the same way; next()
    # drains both fully and merges.
    join_op = HashJoin(left_source, right_source, join, projection)
    join_op.open(None)
    first = join_op.next()
    second = join_op.next()
    join_op.close()

    # Assert: BOTH matches survive -- Galileo/Astronomy (both page 1) AND
    # Newton/Physics (both page 2). Draining only the first right page would drop
    # Newton/Physics; draining only the first left page would drop it too. The
    # whole merged result rides in one batch, then exhaustion.
    assert first is not None
    assert len(first) == 2
    assert any({"title": "Galileo Galilei"} in row and "Astronomy" in row for row in first)
    assert any({"title": "Isaac Newton"} in row and "Physics" in row for row in first)
    assert second is None


def test_sort_operator_orders_merged_rows_by_the_residual_right_key_descending():
    # The residual right-side ORDER BY is held back through the join and applied
    # as a Sort operator ON TOP of the merged stream (ADR-0018 operator tree).
    # JoinExecution used to fold this into assemble(); the tree gives ORDER BY
    # its own operator. Feed pre-merged rows in scrambled title order; Sort must
    # reorder them by courses.title DESCENDING, reading the key off the merged
    # (qualified) schema and result-processing the raw phase-2 title cell.
    #
    # Mirrors the Filter operator's shape: Sort(source, schema, sorts, table),
    # where `sorts` is the compile_residual_sorts list and `table` scopes the
    # sort property to the right side (by bare_name), exactly as assemble() did.
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
    merged_schema = SchemaInfo.from_join(join.left, join.right, *projection)

    name_idx = merged_schema.column_index("name")
    title_idx = merged_schema.column_index("title")

    def merged_row(student: str, course: str) -> tuple:
        # A joined tuple as it leaves the merge: the left title is the decoded
        # {"title": name} left cell; the right title keeps its raw phase-2
        # retrieve shape, which the sort key result-processes to compare.
        cells = [None] * len(merged_schema.columns)
        cells[name_idx] = {"title": student}
        cells[title_idx] = {"title": [{"text": {"content": course}}]}
        return tuple(cells)

    # Scrambled input order (ascending by title): Astronomy, Calculus, Physics.
    source = _RowSource([
        merged_row("Galileo Galilei", "Astronomy"),
        merged_row("Johannes Kepler", "Calculus"),
        merged_row("Isaac Newton", "Physics"),
    ])

    sorts = [{"property": "title", "direction": "descending"}]

    # Act: drive the Sort operator once.
    sort_op = Sort(source, schema=merged_schema, sorts=sorts, table=join.right)
    sort_op.open(None)
    rows = sort_op.next()
    second = sort_op.next()
    sort_op.close()

    # Assert: rows come back by title DESCENDING (Physics > Calculus > Astronomy),
    # i.e. Newton, then Kepler, then Galileo. The whole ordering rides in one
    # batch, then exhaustion.
    assert rows is not None
    assert [r[name_idx]["title"] for r in rows] == [
        "Isaac Newton",     # Physics
        "Johannes Kepler",  # Calculus
        "Galileo Galilei",  # Astronomy
    ]
    assert second is None


def _students_courses(isouter: bool):
    """Build the standard students->courses join plus the schemas and raw-row
    builders shared by the outer-join merge tests below.

    Migrated from the retired ``test_join_execution.py`` (which drove the merge
    through ``JoinExecution.prepare``/``assemble``). The merge now lives in
    ``HashJoin``, so these drive the operator through the Volcano contract with
    two ``_RowSource`` children -- the pure-compute pattern of this module.
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
    join = Join(students, courses, students.c.enrolled_in, isouter=isouter)
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

    return join, projection, left_row, right_row


def _drive_once(join, projection, left_rows, right_rows):
    """Drive a HashJoin over two single-batch in-memory leaves and return the
    one merged batch (the join blocks, so everything arrives in one next())."""
    join_op = HashJoin(
        _RowSource(left_rows),
        _RowSource(right_rows),
        join,
        projection,
    )
    join_op.open(None)
    merged = join_op.next()
    exhausted = join_op.next()
    join_op.close()
    assert exhausted is None
    return merged


def test_hash_join_outer_none_fills_a_dangling_left_row_that_inner_drops():
    # Arrange: an OUTER join. "Galileo" resolves to a real course; "Phantom"
    # points at a dangling id. Inner would drop Phantom; outer must keep it with
    # the right side None-filled (ADR-0007 lax-FK / ADR-0005 phantom). This is
    # the pure-compute oracle for the `if not matched and isouter` site the merge
    # fold preserved verbatim. Migrated from test_join_execution.py.
    join, projection, left_row, right_row = _students_courses(isouter=True)
    left_rows = [
        left_row("Galileo Galilei", ["c-astro"], "s-1"),
        left_row("Phantom Student", ["c-ghost"], "s-2"),  # dangling FK
    ]
    right_rows = [right_row("Astronomy", "c-astro")]

    # Act
    merged_rows = _drive_once(join, projection, left_rows, right_rows)

    # Assert: both left rows survive; the dangling one is None-filled on the
    # right, not dropped and not left-filled.
    assert len(merged_rows) == 2
    matched = [r for r in merged_rows if "Astronomy" in r]
    assert len(matched) == 1
    assert {"title": "Galileo Galilei"} in matched[0]
    phantom = [r for r in merged_rows if {"title": "Phantom Student"} in r]
    assert len(phantom) == 1
    assert "Astronomy" not in phantom[0]
    assert None in phantom[0]


def test_hash_join_outer_none_fills_a_left_row_with_an_empty_relation():
    # Arrange: an OUTER join with a left row enrolled in NOTHING (empty relation,
    # zero oids). Not a dangling FK -- there is no id to resolve -- but outer
    # semantics must still preserve it, right side None-filled. Migrated from
    # test_join_execution.py.
    join, projection, left_row, right_row = _students_courses(isouter=True)
    left_rows = [left_row("Hermit Student", [], "s-1")]
    right_rows = [right_row("Astronomy", "c-astro")]

    # Act
    merged_rows = _drive_once(join, projection, left_rows, right_rows)

    # Assert: exactly one row, left intact, right None-filled.
    assert len(merged_rows) == 1
    assert {"title": "Hermit Student"} in merged_rows[0]
    assert "Astronomy" not in merged_rows[0]
    assert None in merged_rows[0]


def test_hash_join_outer_does_not_none_fill_a_row_that_already_matched():
    # Arrange: ONE left row with two oids -- one real ("c-astro") and one
    # dangling ("c-ghost"). The row already matched, so outer must NOT bolt on a
    # None-filled row for the unmatched id: None-fill is a whole-row fallback,
    # not per-oid. Migrated from test_join_execution.py.
    join, projection, left_row, right_row = _students_courses(isouter=True)
    left_rows = [left_row("Galileo Galilei", ["c-astro", "c-ghost"], "s-1")]
    right_rows = [right_row("Astronomy", "c-astro")]

    # Act
    merged_rows = _drive_once(join, projection, left_rows, right_rows)

    # Assert: exactly one row -- the real match, with no None-fill anywhere.
    assert len(merged_rows) == 1
    assert {"title": "Galileo Galilei"} in merged_rows[0]
    assert "Astronomy" in merged_rows[0]
    assert None not in merged_rows[0]


def test_joinexecution_seam_is_deleted():
    # #378 STEP 2 / ADR-0021 Decision "JoinExecution is deleted". With the merge
    # folded into HashJoin (5c369bf), the two-phase prepare/assemble seam has no
    # remaining purpose: prepare built the now-gone pages.retrieve dedup batch,
    # and assemble's residual right WHERE/ORDER BY blocks are dead (the live
    # residuals are the Filter/Sort operators). Pin the removal so the dead class
    # cannot quietly return.
    import normlite.sql.dml as dml

    assert not hasattr(dml, "JoinExecution")
