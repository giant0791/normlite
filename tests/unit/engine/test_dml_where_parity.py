"""SELECT, DELETE and UPDATE select the same rows for every WHERE predicate (#414).

The parity suite runs each predicate shape over one cast with valueless cells.
SELECT gives the reference ``object_id`` set. Each DML statement must touch
exactly that set.

Every parity shape stays within the filter depth limit. A too-deep WHERE is
pruned (#417). The last test covers a prune that leaves nothing to push.
"""
from datetime import date

import pytest

from normlite.sql.dml import delete, insert, select, update
from normlite.sql.elements import not_

from tests.utils.db_helpers import (
    create_students_db,
    attach_table_oid,
    populate_students,
)
from tests.utils.spies import QueryPayloadRecordingClient


@pytest.fixture
def cast(engine, students):
    """Ten valued rows plus three rows that make the predicates disagree.

    * ``name_0`` .. ``name_9``: ``id`` 0..9, ``grade == "A"``.
    * ``Nullius``: valueless ``id``.
    * ``Blank``: empty ``grade``.
    * ``Bee``: ``grade == "B"``.
    """
    db_id = create_students_db(engine)
    attach_table_oid(students, db_id)
    populate_students(engine, students, n=10)
    with engine.connect() as conn:
        for name, id_, grade in [
            ("Nullius", None, "A"),
            ("Blank", 100, ""),
            ("Bee", 50, "B"),
        ]:
            conn.execute(insert(students).values(
                name=name, id=id_, is_active=True,
                start_on=date(1600, 1, 1), grade=grade,
            ))
    return students


# Each shape builds a NEW WHERE per statement: one WHERE object cannot serve two
# statements (#421).
PREDICATE_SHAPES = {
    "not_equal_over_valueless": lambda t: t.c.id != 1,
    "root_not": lambda t: not_(t.c.id == 1),
    "negated_conjunct": lambda t: (t.c.id < 3) & not_(t.c.id == 1),
    "or_with_negated_arm": lambda t: (t.c.id == 1) | not_(t.c.grade == "A"),
    "equal_none": lambda t: t.c.id == None,
    "not_equal_none": lambda t: t.c.id != None,
    "equal_empty_string": lambda t: t.c.grade == "",
    "not_equal_empty_string": lambda t: t.c.grade != "",
    "is_empty": lambda t: t.c.id.is_empty(),
    "is_not_empty": lambda t: t.c.id.is_not_empty(),
}


def object_ids(engine, stmt) -> set:
    with engine.connect() as conn:
        return {row.object_id for row in conn.execute(stmt).all()}


@pytest.mark.parametrize("shape", PREDICATE_SHAPES.values(), ids=PREDICATE_SHAPES.keys())
def test_delete_trashes_exactly_the_rows_select_returns(engine, cast, shape):
    every_row = object_ids(engine, select(cast.c.object_id))
    selected = object_ids(engine, select(cast.c.object_id).where(shape(cast)))

    trashed = object_ids(
        engine, delete(cast).where(shape(cast)).returning(cast.c.object_id)
    )

    survivors = object_ids(engine, select(cast.c.object_id))
    assert trashed == selected
    assert survivors == every_row - selected


@pytest.mark.parametrize("shape", PREDICATE_SHAPES.values(), ids=PREDICATE_SHAPES.keys())
def test_update_changes_exactly_the_rows_select_returns(engine, cast, shape):
    # Every row starts with is_active True, and no shape reads is_active.
    selected = object_ids(engine, select(cast.c.object_id).where(shape(cast)))

    changed = object_ids(
        engine,
        update(cast).values(is_active=False).where(shape(cast)).returning(cast.c.object_id),
    )

    # Table state, not only RETURNING: it catches a write with a wrong value.
    inactive = object_ids(
        engine, select(cast.c.object_id).where(cast.c.is_active == False)
    )
    assert changed == selected
    assert inactive == selected


# Each builder makes a DML statement for one WHERE, with RETURNING object_id.
DML_STATEMENTS = {
    "delete": lambda t, where: delete(t).where(where).returning(t.c.object_id),
    "update": lambda t, where: (
        update(t).values(is_active=False).where(where).returning(t.c.object_id)
    ),
}


@pytest.mark.parametrize("build", DML_STATEMENTS.values(), ids=DML_STATEMENTS.keys())
def test_a_dml_where_that_prunes_to_nothing_pushes_no_filter(
    engine, cast, build, monkeypatch
):
    """A too-deep WHERE whose prune leaves nothing sends no filter (#417).

    The WHERE nests ``or -> and -> or``::

        WHERE ((id = 1 OR id = 2) AND (grade = 'A' OR grade = 'B')) OR name = 'Bee'

    The two inner ``or`` compounds sit at level 3, under an ``and``. The prune
    drops both, the ``and`` is left with nothing, and the root ``or`` cannot
    absorb an empty child. Nothing remains to push.

    The statement must send NO ``filter`` key, not an empty one. The in-memory
    client accepts ``"filter": {}``, so only the payload shows the difference.
    The recheck then decides: SQL selects ``name_1``, ``name_2`` and ``Bee``.
    """
    def where():
        return (
            ((cast.c.id == 1) | (cast.c.id == 2))
            & ((cast.c.grade == "A") | (cast.c.grade == "B"))
        ) | (cast.c.name == "Bee")

    selected = object_ids(engine, select(cast.c.object_id).where(where()))

    spy = QueryPayloadRecordingClient(engine._client)
    monkeypatch.setattr(engine.raw_connection(), "_client", spy)

    touched = object_ids(engine, build(cast, where()))

    assert spy.payloads_seen, "the DML statement must query the data source"
    assert all("filter" not in p for p in spy.payloads_seen), (
        f"a WHERE pruned to nothing must push no filter: {spy.payloads_seen}"
    )
    assert len(selected) == 3
    assert touched == selected
