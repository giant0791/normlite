import uuid
from datetime import date

import pytest

from normlite.exceptions import UnsupportedCompilationError
from normlite.sql.compiler import _depth, _is_pushable, _normalize_filter, NotionCompiler, _prune
from normlite.sql.dml import select
from normlite.sql.schema import Column
from normlite.sql.type_api import Integer, String
from tests.utils.compiler import ReferenceCompiler, assert_compile_equal, reference_compile
from tests.utils.db_helpers import attach_table_oid

@pytest.fixture
def name_col() -> Column:
    return Column(
        'name',
        String(is_title=True)
    )

@pytest.fixture
def id_col() -> Column:
    return Column(
        'id',
        Integer()
    )

@pytest.fixture
def grade_col() -> Column:
    return Column(
        'name',
        String()
    )

@pytest.fixture
def ref_compiler() -> ReferenceCompiler:
    return ReferenceCompiler()


#-------------------------------------------
# Leaf axioms
#-------------------------------------------
def test_compile_does_not_leak_bind_values(name_col):
    """Literal value must never appear in JSON."""
    expr = name_col == "Galilei"

    compiled = reference_compile(expr)

    assert "Galilei" not in str(compiled)
    assert ":param_" in str(compiled)

def test_compile_leaf_equals(name_col, ref_compiler, prod_compiler):
    """Differential test: leaf equality."""
    expr = name_col == "Galilei"
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

def test_compile_leaf_greater_than(id_col, ref_compiler, prod_compiler):
    """Differential test: numeric comparison."""
    expr = id_col > 100
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

#-------------------------------------------
# Boolean operator axioms
#-------------------------------------------
def test_and_compiles_to_list(name_col, grade_col):
    """Axiom: AND produces a list."""
    expr = (name_col == "A") & (grade_col == "B")

    compiled = reference_compile(expr)

    assert "and" in compiled
    assert isinstance(compiled["and"], list)
    assert len(compiled["and"]) == 2

def test_compile_and(name_col, grade_col, ref_compiler, prod_compiler):
    """Differential test: AND."""
    expr = (name_col == "A") & (grade_col == "B")
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

def test_compile_or(name_col, grade_col, ref_compiler, prod_compiler):
    """Differential test: OR."""
    expr = (name_col == "A") | (grade_col == "B")
    assert_compile_equal(expr, ref_compiler.process, prod_compiler.process)

#-------------------------------------------
# NOT operator axioms
#-------------------------------------------
def test_reference_refuses_a_not(name_col):
    """Axiom: a Notion filter has no "not", so the reference emits nothing for it."""
    expr = ~(name_col == "Galilei")

    with pytest.raises(UnsupportedCompilationError):
        reference_compile(expr)

def test_compile_not(name_col, ref_compiler, prod_compiler):
    """Differential test: both compilers refuse a NOT, and both gates reject it."""
    expr = ~(name_col == "Galilei")

    assert ref_compiler.is_pushable(expr) is False
    assert _is_pushable(expr) is False

    with pytest.raises(UnsupportedCompilationError):
        ref_compiler.process(expr)

    with pytest.raises(UnsupportedCompilationError):
        prod_compiler.process(expr)


@pytest.mark.parametrize("column", ["name_col", "grade_col"], ids=["title", "rich_text"])
@pytest.mark.parametrize(
    "build",
    [
        lambda c: c == "",
        lambda c: c != "",
        lambda c: c.in_(""),
        lambda c: c.not_in(""),
        lambda c: c.startswith(""),
        lambda c: c.endswith(""),
    ],
    ids=["equals", "does_not_equal", "contains", "does_not_contain", "starts_with", "ends_with"],
)
def test_gates_agree_on_an_empty_string_text_operand(request, ref_compiler, column, build):
    """An empty-string literal under a text operator is unpushable (#382).

    Notion ignores the condition and returns every row. The random generator
    never puts "" under a text operator, so only this test covers the rule.
    """
    col = request.getfixturevalue(column)

    assert ref_compiler.is_pushable(build(col)) is False
    assert _is_pushable(build(col)) is False


@pytest.mark.parametrize("column", ["id_col", "name_col", "grade_col"], ids=["number", "title", "rich_text"])
@pytest.mark.parametrize(
    "build, pushable",
    [
        (lambda c: c == None, False),
        (lambda c: c != None, False),
        (lambda c: c.is_empty(), True),
        (lambda c: c.is_not_empty(), True),
    ],
    ids=["eq-none", "ne-none", "is_empty", "is_not_empty"],
)
def test_gates_agree_on_a_none_operand(request, ref_compiler, column, build, pushable):
    """A None literal under == or != is unpushable (Notion answers HTTP 400).

    is_empty() and is_not_empty() carry a None operand too, but it is a
    placeholder, so they stay pushable. The random generator never puts None
    under == or !=, so only this test covers the rule. The rule holds on every
    column type, text included: a text rule placed before it must not hide it.
    """
    col = request.getfixturevalue(column)

    assert ref_compiler.is_pushable(build(col)) is pushable
    assert _is_pushable(build(col)) is pushable


@pytest.mark.parametrize("column", ["id_col", "name_col", "grade_col"], ids=["number", "title", "rich_text"])
@pytest.mark.parametrize(
    "build",
    [
        lambda c: c == (lambda: None),
        lambda c: c != (lambda: None),
    ],
    ids=["eq-callable-none", "ne-callable-none"],
)
def test_gates_agree_on_a_callable_that_returns_none(request, ref_compiler, column, build):
    """A callable operand that returns None under == or != is unpushable.

    The callable is a deferred value. When it returns None, the comparison
    sends a null comparison value, and Notion answers HTTP 400, as for a None
    literal. The random generator never makes a callable operand, so only this
    test covers the rule.
    """
    col = request.getfixturevalue(column)

    assert ref_compiler.is_pushable(build(col)) is False
    assert _is_pushable(build(col)) is False

@pytest.mark.parametrize("column", ["name_col", "grade_col"], ids=["title", "rich_text"])
@pytest.mark.parametrize(
    "build",
    [
        lambda c: c == (lambda: ""),
        lambda c: c != (lambda: ""),
    ],
    ids=["eq-callable-empty-string", "ne-callable-empty-string"],
)
def test_gates_agree_on_a_callable_that_returns_an_empty_string(request, ref_compiler, column, build):
    col = request.getfixturevalue(column)

    assert ref_compiler.is_pushable(build(col)) is False
    assert _is_pushable(build(col)) is False


#-------------------------------------------
# Associativity invariants
#-------------------------------------------
def test_and_associativity(name_col, grade_col, id_col, ref_compiler, prod_compiler):
    """AND associativity"""
    e1 = (name_col == "A") & ((grade_col == "B") & (id_col > 10))
    e2 = ((name_col == "A") & (grade_col == "B")) & (id_col > 10)

    assert_compile_equal(e1, ref_compiler.process, prod_compiler.process)
    assert_compile_equal(e2, ref_compiler.process, prod_compiler.process)

    assert reference_compile(e1) == reference_compile(e2)

def test_or_associativity(name_col, grade_col, id_col, ref_compiler, prod_compiler):
    """OR associativity"""
    e1 = (name_col == "A") | ((grade_col == "B") | (id_col > 10))
    e2 = ((name_col == "A") | (grade_col == "B")) | (id_col > 10)

    assert_compile_equal(e1, ref_compiler.process, prod_compiler.process)
    assert_compile_equal(e2, ref_compiler.process, prod_compiler.process)

    assert reference_compile(e1) == reference_compile(e2)

def test_pruned_and_under_or_does_not_spend_a_nesting_level(
    name_col, grade_col, id_col, prod_compiler
):
    """A compound left holding ONE clause must be emitted as that clause (#383).

    Pruning an unpushable conjunct can leave an ``and`` with a single survivor.
    Emitting it as ``{"and": [X]}`` is semantically identical to ``X`` -- an ``and``
    of one thing IS that thing -- but it is not identical to the Notion API, which
    caps compound nesting at 2 levels. Here the redundant wrapper puts a filter that
    means ``A OR C`` at depth 2, spending half the budget on a wrapper around
    nothing.

    This is the whole reason the unwrap is load-bearing rather than cosmetic. The
    leaf repair lands next and manufactures a compound where a leaf used to be,
    which spends a level for a real reason; that level has to be available.

    Production compiler only, deliberately: ``ReferenceCompiler`` does not prune yet
    and reaches this expression through the ``not`` it cannot emit, so a differential
    assertion here would test the stale reference model rather than the invariant.
    ``visit_select``'s join path already unwraps its single survivor
    (``left_conjuncts[0] if len(...) == 1``); this pins the same rule on the scan
    path, where the two should not disagree.
    """
    expr = ((name_col == "A") & ~(grade_col == "B")) | (id_col > 10)

    compiled = prod_compiler.process(expr)

    assert _depth(compiled) == 1, (
        f"redundant single-child wrapper spends a nesting level: {compiled}"
    )

    assert compiled == {
        "or": [
            {"property": "name", "title": {"equals": ":param_0"}},
            {"property": "id", "number": {"greater_than": ":param_1"}},
        ]
    }

def test_repaired_ne_under_an_or_is_pruned_to_a_legal_depth(students):
    """A SELECT must prune its emitted filter to Notion's 2-level cap (#383).

    A REGRESSION, not a tidy-up. Measured on the same expression, fresh bind
    params per compile::

        WHERE name='name_0' AND (id != 1 OR grade='A')

          at 188d4ef:  depth 2   <- legal
          at e26d7af:  depth 3   <- HTTP 400

    The leaf repair (``e26d7af``) turns ``id != 1`` into a level-1 compound
    ``{"and": [does_not_equal, is_not_empty]}``. Under an ``and`` parent the
    fold's splice absorbs it; here the parent is an ``or``, where splicing
    would change which rows match, so the level stays -- and the ``or`` is
    itself nested under the root ``and``. A SELECT that ran against real Notion
    before the repair now draws a 400.

    **Compile-level, and it has to be.** Measured on this tree: the in-memory
    client accepts the depth-3 filter and returns exactly ``name_0``, the right
    answer. No execution test can see this bug -- the only observer of the
    nesting cap is the real API. The emitted dict is the evidence.

    **Why pruning is the sound answer here.** ADR-0022: the pushed filter is a
    hint, ``recheck_where`` re-applies the whole predicate over raw cells, so a
    filter matching a SUPERSET returns the right rows. The DELETE path gets the
    OPPOSITE verdict from the same shape -- see
    ``test_delete_pipeline.test_delete_refuses_a_where_it_cannot_push_within_the_nesting_cap``
    -- because nothing there re-narrows and a pruned filter is a destroyed row.

    **Both assertions are load-bearing, and the second one PINS THE FIX.** Depth
    is asserted FIRST on purpose: it fails as a one-line number rather than a
    wall-of-dict diff, and session 6's mutation run proved it kills mutants the
    dict alone does not. The exact dict comes second, by the user's call: a
    whitelist of retained properties admits several strategies at once and so
    says nothing about which one actually shipped. The bill is knowingly
    accepted -- improving the prune means editing this literal, in every red.

    **The prune un-repairs the ``!=`` leaf, and that is correct.** ``is_not_empty``
    is dropped as a conjunct and ``number.does_not_equal`` is pushed bare -- a
    SUPERSET of SQL's ``!=`` (#384, ADR-0022), so the filter weakens and never
    narrows. The division of labour is the point: the repair (``e26d7af``) exists
    for the DML path, which has no recheck and where over-matching destroys rows.
    On a SELECT the recheck re-narrows, so a nesting level spent on exactness
    buys nothing and costs the 400. Repair and prune compose rather than fight.
    The bind numbering is contiguous here only because ``is_not_empty`` is built
    literally and registers no param; the sibling reds DO have holes, correctly.

    What the pin refuses is the prune that goes green on depth alone: dropping a
    DISJUNCT. ``{"and": [name, grade='A']}`` measures 1 and is UNSOUND -- it
    NARROWS, losing rows ``id != 1`` selects, and no recheck can add back a row
    the push never fetched. That is the failure
    :meth:`NotionCompiler.visit_boolean_clause_list`'s docstring warns about,
    and a depth-only assertion would ship it. The empty push is refused for a
    weaker reason: it is sound but throws away a leaf that costs no depth at
    all, buying a full table scan for nothing.
    """
    attach_table_oid(students, str(uuid.uuid5(uuid.NAMESPACE_DNS, "students")))

    stmt = select(students.c.name).where(
        (students.c.name == "name_0")
        & ((students.c.id != 1) | (students.c.grade == "A"))
    )

    compiled_filter = stmt.compile(NotionCompiler()).as_dict()["payload"].get("filter", {})

    assert _depth(compiled_filter) <= 2, (
        f"emitted filter exceeds Notion's 2-level nesting cap: {compiled_filter}"
    )

    assert compiled_filter == {
        "and": [
            {"property": "name", "title": {"equals": ":param_0"}},
            {"or": [
                {"property": "id", "number": {"does_not_equal": ":param_1"}},
                {"property": "grade", "rich_text": {"equals": ":param_2"}},
            ]},
        ]
    }, (
        "an `and` may drop a conjunct (weakening, the recheck decides) but an `or` "
        "may never drop a disjunct (narrowing, unrecoverable), and pushing nothing "
        f"spends a full table scan on a leaf that costs no depth: {compiled_filter}"
    )

def test_user_written_and_or_and_is_pruned_to_a_legal_depth(students):
    """A SELECT must prune nesting the USER wrote, not just nesting we manufacture (#383).

    This is #383's headline case -- deep nesting causing HTTP 400 -- and it is
    the one the sibling test above does NOT cover. That one is about a
    REGRESSION: the leaf repair (``e26d7af``) manufactured a compound where the
    user wrote a leaf, and the fix could be as local as undoing it. This one has
    no ``!=`` anywhere::

        WHERE name='x' AND (grade='A' OR (id>5 AND is_active))

    Three plain user-written operators, and the emitted filter measures 3.
    Measured on this tree, and measured on ``main``: it has ALWAYS been broken.
    No repair to undo, so any fix that reaches only the manufactured compound
    leaves this query drawing a 400 with the regression test green.

    **The prune must recurse.** Depth is not capped at 3 -- operators that
    strictly alternate never collapse, because
    :class:`~normlite.sql.elements.BooleanClauseList` flattens only
    SAME-operator nesting. Measured on this fixture: ``name='x' AND (grade='A'
    OR (id>5 AND (grade='B' OR is_active)))`` emits depth 4. A prune that
    pattern-matches one shape is not a prune.

    **Which prunes are sound: monotonicity, not a strategy.** ``and`` and ``or``
    are each monotone in every child, so replacing any subterm by a WEAKER one
    weakens the whole filter -- a superset, which ADR-0022's ``recheck_where``
    re-narrows over raw cells. Dropping a child of an ``and`` weakens it; dropping
    a child of an ``or`` STRENGTHENS it. So all of these are legal here:

    ==============================================  ========================
    prune                                           surviving properties
    ==============================================  ========================
    drop the whole ``or`` conjunct                  ``{name}``
    ``{"and": [id, is_active]}`` -> ``id``          ``{name, grade, id}``
    ``{"and": [id, is_active]}`` -> ``is_active``   ``{name, grade, is_active}``
    ``{"and": [id, is_active]}`` -> ``{"or": ...}`` ``{name, grade, id, is_active}``
    ==============================================  ========================

    All four are sound; #383 pins the SECOND, and the literal below says so. The
    over-deep node sits under an ``or``, so it is replaced by its first child,
    recursed -- ``{"and": [id, is_active]}`` becomes ``id>5``.

    **The fourth row is the trap in this table, and it is not the unsound one.**
    It keeps every property and reads like the best outcome, which is exactly why
    the retired ``_properties`` whitelist ranked it first. The metric that matters
    is ROWS REACHING THE RECHECK, and on that metric it is the worst of the four:
    ``id>5 OR is_active`` is strictly weaker than ``id>5``, so it fetches a
    superset of what the pinned prune fetches, for the same depth. Properties
    retained is a proxy invented for an assertion, and it ranks these BACKWARDS.

    Note the bind hole. ``:param_3`` is registered for ``is_active`` during
    emission and then dropped by the prune, so the literal runs ``0, 1, 2`` and
    nothing binds ``3``. That is correct, not a leak: emission happens first and
    the prune only ever removes (decision L). A literal reading ``:param_0,
    :param_1, :param_2`` for grade and id would mean the prune had run BEFORE
    emission and the numbering had closed up behind it.

    **The trap.** Dropping the level-3 ``and`` outright leaves
    ``{"and": [name, grade='A']}``, which measures 2 and is UNSOUND: it is a
    DISJUNCT of the ``or``, and without it every row with ``id>5 AND is_active``
    but ``grade != 'A'`` is never fetched. No recheck can add back a row the push
    never returned. Note it is a strict SUBSET of a legal outcome
    (``{name, grade}`` vs ``{name, grade, id}``), so "kept properties are a
    subset of the predicate's" would ship it too -- the legal set is enumerated,
    not bounded.

    .. seealso::
        ``test_repaired_ne_under_an_or_is_pruned_to_a_legal_depth`` -- the
        regression half, same cap, manufactured nesting.

        ``test_delete_pipeline.test_delete_refuses_a_where_it_cannot_push_within_the_nesting_cap``
        -- the DELETE verdict on this same shape is a REFUSAL, because nothing
        re-narrows a DML push and a pruned filter is a destroyed row.
    """
    attach_table_oid(students, str(uuid.uuid5(uuid.NAMESPACE_DNS, "students")))

    stmt = select(students.c.name).where(
        (students.c.name == "x")
        & (
            (students.c.grade == "A")
            | ((students.c.id > 5) & (students.c.is_active == True))
        )
    )

    compiled_filter = stmt.compile(NotionCompiler()).as_dict()["payload"].get("filter", {})

    assert _depth(compiled_filter) <= 2, (
        f"emitted filter exceeds Notion's 2-level nesting cap: {compiled_filter}"
    )

    assert compiled_filter == {
        "and": [
            {"property": "name", "title": {"equals": ":param_0"}},
            {"or": [
                {"property": "grade", "rich_text": {"equals": ":param_1"}},
                {"property": "id", "number": {"greater_than": ":param_2"}},
            ]},
        ]
    }, (
        "an `and` may drop a conjunct (weakening, the recheck decides) but an `or` "
        "may never drop a disjunct (narrowing, unrecoverable): dropping the nested "
        "`and` loses every row matching `id>5 AND is_active` but not `grade='A'`. "
        f"Pushing nothing spends a full table scan for no depth saved: {compiled_filter}"
    )


def test_an_over_deep_or_under_an_and_is_dropped_and_the_lone_survivor_unwrapped(students):
    """A SELECT must DROP an over-deep ``or`` that sits under an ``and`` (#383).

    Reds 1 and 2 both put the violator under an ``or``, so both exercise the
    REPLACE row: the violator gives way to its first child, recursed. This shape
    turns the parent round::

        WHERE (name='x' AND (grade='A' OR id>5)) OR is_active

    The root ``or`` is level 1, the ``and`` level 2, and ``(grade='A' OR id>5)``
    level 3. Its parent is an ``and``, so dropping it WEAKENS the conjunction --
    a superset, which the recheck re-narrows. That leaves ``{"and": [name]}``, a
    one-child compound, and the fold unwraps it to the bare leaf. The pushed
    filter measures 1, not 2.

    **What each wrong prune ships.**

    * Replace instead of drop: ``{"or": [{"and": [name, grade]}, is_active]}``.
      It measures 2 and is UNSOUND. The violator is an ``or``, and an ``or``'s
      first child is STRONGER than the ``or``: every row with ``name='x' AND
      id>5``, ``grade != 'A'`` and an unticked ``is_active`` is never fetched.
      Replace is sound only for an ``and`` violator -- which is all reds 1 and 2
      ever built. This is the mutant this test exists to kill.
    * Drop the whole ``and`` disjunct: ``{"or": [is_active]}`` -> ``is_active``.
      That is UNSOUND. Every row with ``name='x' AND grade='A'`` and an unticked
      ``is_active`` is never fetched, and no recheck adds back a row the push
      never returned.
    * No unwrap: ``{"or": [{"and": [name]}, is_active]}`` measures 2 and is
      legal, but it is the fold's job to leave no one-child compound. A literal
      that admits it would hide a fold that stopped folding.

    The bind hole is ``:param_1`` and ``:param_2`` -- ``grade`` and ``id`` are
    emitted, registered, and then dropped. ``is_active`` keeps ``:param_3``.

    .. seealso::
        ``test_pruning_level_3_and_drops_it`` -- despite its name, its fixtures
        put the violator under an ``or``; this is the first test to reach drop.
    """
    attach_table_oid(students, str(uuid.uuid5(uuid.NAMESPACE_DNS, "students")))

    stmt = select(students.c.name).where(
        (
            (students.c.name == "x")
            & ((students.c.grade == "A") | (students.c.id > 5))
        )
        | (students.c.is_active == True)
    )

    compiled_filter = stmt.compile(NotionCompiler()).as_dict()["payload"].get("filter", {})

    assert _depth(compiled_filter) <= 2, (
        f"emitted filter exceeds Notion's 2-level nesting cap: {compiled_filter}"
    )

    assert compiled_filter == {
        "or": [
            {"property": "name",      "title":    {"equals": ":param_0"}},
            {"property": "is_active", "checkbox": {"equals": ":param_3"}},
        ]
    }, (
        "an over-deep `or` under an `and` must be DROPPED (the `and` weakens, the "
        "recheck decides), and the one-child `and` it leaves must fold to its leaf. "
        f"Dropping the `and` disjunct instead loses rows no recheck can restore: {compiled_filter}"
    )


def test_a_depth_5_filter_is_pruned_by_recursing_into_the_replacement(students):
    """A SELECT must prune a filter at ANY depth, not just one level over the cap (#383).

    Reds 1-3 all measure 3: one violator, one rule, done. This shape measures 5::

        name='x' AND (grade='A' OR ( ((id>5 AND is_active) OR grade='B') AND start_on=d ))

    Levels: ``and`` (1) -> ``or`` (2) -> ``and`` (3) -> ``or`` (4) -> ``and`` (5).
    The ``and`` at level 3 sits under an ``or``, so it is replaced by its first
    child -- the ``or`` at level 4. That child is itself over the cap's reach, so
    the replacement must be RECURSED, and at ``level - 1``: it splices into the
    parent and inherits the parent's level. At level 2 it is legal, its own ``and``
    child now sits at level 3 under an ``or`` and is replaced by ``id>5``. The
    surviving ``{"or": [id, grade='B']}`` splices into the level-2 ``or``.

    **What each wrong recursion ships.**

    * No recursion (return ``children[0]`` raw): the level-4 ``or`` keeps its
      ``and``, splices up, and the filter measures 3. The depth assertion fails.
    * Recursion at ``level`` instead of ``level - 1``: the level-4 ``or`` is
      judged at level 3 and replaced by its first child, the ``and``, which is
      replaced in turn by ``id>5``. ``grade='B'`` is lost -- and since the
      violator was an ``or``, that is a NARROWING: every row with ``grade='B'
      AND start_on=d`` is never fetched. Depth 2, unsound, and only the exact
      dict catches it.

    The old depth-4 shape is degenerate here: it emits the same dict as red 2
    and passes with no recursion at all. Depth 5 is the first that needs it.

    The bind holes are ``:param_3`` (``is_active``) and ``:param_5``
    (``start_on``); both leaves are emitted, registered, and then pruned.
    """
    attach_table_oid(students, str(uuid.uuid5(uuid.NAMESPACE_DNS, "students")))

    stmt = select(students.c.name).where(
        (students.c.name == "x")
        & (
            (students.c.grade == "A")
            | (
                (
                    ((students.c.id > 5) & (students.c.is_active == True))
                    | (students.c.grade == "B")
                )
                & (students.c.start_on == date(2024, 9, 1))
            )
        )
    )

    compiled_filter = stmt.compile(NotionCompiler()).as_dict()["payload"].get("filter", {})

    assert _depth(compiled_filter) <= 2, (
        f"emitted filter exceeds Notion's 2-level nesting cap: {compiled_filter}"
    )

    assert compiled_filter == {
        "and": [
            {"property": "name", "title": {"equals": ":param_0"}},
            {"or": [
                {"property": "grade", "rich_text": {"equals": ":param_1"}},
                {"property": "id",    "number":    {"greater_than": ":param_2"}},
                {"property": "grade", "rich_text": {"equals": ":param_4"}},
            ]},
        ]
    }, (
        "a replacement must be recursed at `level - 1` -- it splices into its parent "
        "and inherits that level. Recursing at `level` replaces the level-4 `or` "
        f"by its first child and loses `grade='B'`, rows no recheck can restore: {compiled_filter}"
    )


#-------------------------------------------
# Fold contract
#-------------------------------------------
def test_folding_zero_clauses_yields_no_filter():
    """No surviving clause must fold to a FALSY dict, never to ``{op: []}`` (#383).

    Called directly, deliberately. No caller can reach this input today: the scan
    path is gated upstream by :func:`_is_pushable`, whose lax **and** is ``any()``
    and whose **or** is ``all()``, so an admitted node always leaves at least one
    survivor for the comprehension; the join path pre-checks its own list. That
    coincidence is what the caller's docstring argues, and it is temporary --
    depth pruning (queue item 3) runs INSIDE ``visit_boolean_clause_list``,
    downstream of the gate, so a node the gate admitted can be pruned to nothing.
    The visitor is then the only frame that knows, and its only channel back is
    the return value. This pins that channel before anything depends on it.

    **Why a falsy dict and not a raise.** An empty clause list is not a
    compilation error and not a broken invariant: it is the well-defined outcome
    *push nothing*, which is sound under ADR-0022 because the pushed filter never
    decides -- the client-side recheck holds the full predicate and does. Measured
    on this tree: ``WHERE NOT id=1 AND NOT grade='B'`` already takes that path at
    ``compiler.py:803``, emits no ``filter`` key at all, and returns the right
    rows. Raising here would turn a working query into a crash.

    **Why ``{}`` is the whole contract.** ``payload['filter']`` is a key that must
    be ABSENT, not present-and-empty -- so both call sites will guard on
    truthiness and decline to assign. ``{"and": []}`` is a non-empty dict and
    therefore truthy, which is exactly what defeats that guard: it would be
    written into the payload, where an empty compound either draws an HTTP 400 or
    means match-everything. On a ``DELETE`` the second archives the table.

    Both operators, because the helper is operator-agnostic and the rule is a
    property of the empty list, not of the word wrapping it.

    .. seealso::

        Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.
    """
    assert _normalize_filter("and", []) == {}
    assert _normalize_filter("or", []) == {}

def test_nested_ands_are_spliced():
    """Same-operator nesting merges one level in; a different operator does NOT (#383).

    The splice is associativity, and associativity is per-operator.
    ``(P AND Q) AND (R OR S)`` is ``P AND Q AND (R OR S)`` -- the inner ``and``'s
    parens dissolve because ``and`` is associative with itself. The ``(R OR S)`` parens
    cannot dissolve: ``P AND Q AND R AND S`` is a strictly stronger predicate. So a
    non-matching operator is an opaque element, spliced past exactly like a leaf.

    The last assertion is the one that matters. A splice hardcoded to ``"and"`` instead
    of ``op`` passes the first three -- when ``op`` IS ``"and"`` the two spellings are
    indistinguishable -- and turns ``(P AND Q) OR R`` into ``P OR Q OR R``. That is
    reachable through an ordinary compile today, because
    :class:`BooleanClauseList`'s constructor flattens only SAME-operator children, so an
    ``or`` keeps its ``and`` child and that child emits a compound. On ``SELECT`` the
    weakened predicate survives as a superset that ``recheck_where`` re-narrows, but DML
    has no recheck and every leaf here is pushable, so the ``_all_terms_pushable`` gate
    admits it: measured on this tree,
    ``DELETE WHERE (name='name_0' AND grade='Z') OR id > 8`` destroyed a row it had to
    keep. The three ``and`` cases cannot see that; only varying ``op`` can.

    Positions are asserted on both sides because the merged children land WHERE THEIR
    WRAPPER WAS, not appended at the end. ``test_compiler_differential`` compares emitted
    JSON by exact equality, so a splice that collects non-matching elements first and
    appends matching ones later would be semantically identical and still fail there.

    Called directly, with opaque string placeholders: nothing reaches the splice through
    a compile yet, and strings prove the fold never inspects an element's internals --
    the property that lets it stay closed under composition. Keep placeholders short;
    a bare ``op in clause`` on a string is a SUBSTRING test, and ``"and" in "candidate"``
    is ``True``.

    .. seealso::

        Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.
    """
    assert _normalize_filter("and", [{"and": ["P", "Q"]}, {"and": ["R", "S"]}]) == {"and": ["P", "Q", "R", "S"]}
    assert _normalize_filter("and", [{"or": ["P", "Q"]}, {"and": ["R", "S"]}]) == {"and": [{"or": ["P", "Q"]}, "R", "S"]}
    assert _normalize_filter("and", [{"and": ["P", "Q"]}, {"or": ["R", "S"]}]) == {"and": ["P", "Q", {"or": ["R", "S"]}]}
    assert _normalize_filter("or", [{"and": ["P", "Q"]}, "R"]) == {"or": [{"and": ["P", "Q"]}, "R"]}    

def test_falsy_clauses_are_dropped_at_every_arity():
    """A falsy clause must never survive into the emitted compound (#383).

    The all-empty case is not the interesting one -- the MIXED list is. An empty
    clause sitting beside a real one produces ``{"and": [{}, X]}``, which is a
    NON-EMPTY dict and therefore TRUTHY, so it sails straight past the call sites'
    ``if filter_obj:`` guard and lands in ``payload['filter']``. There the in-memory
    client resolves it with ``payload.get('filter', False)`` and reads it as *no
    filter at all*: a ``DELETE`` so compiled matches every row, with the suite green.

    Dropping is required at EVERY arity rather than only at zero because the fold's
    output type is its input element type -- ``visit_boolean_clause_list`` returns
    this result and the parent takes it as a clause one level up -- so a legal ``{}``
    output is automatically a legal ``{}`` input. Without the drop the helper is not
    closed under composition and the sentinel leaks upward as a truthy wrapper.

    Called directly: no compile reaches a mixed list today, because pruning is gated
    upstream by :func:`_is_pushable` and always leaves a survivor. Depth pruning
    (queue item 3) runs INSIDE the visitor, downstream of that gate, and makes it
    reachable.

    .. seealso::

        Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.
    """
    # a lone empty clause is still no filter
    assert _normalize_filter("and", [{}]) == {}
    assert _normalize_filter("or", [{}, {}]) == {}

    # the mixed list: the empty clause is dropped, not carried
    assert _normalize_filter("and", [{}, {"p": 1}]) == {"p": 1}
    assert _normalize_filter("and", [{"p": 1}, {}, {"q": 2}]) == {"and": [{"p": 1}, {"q": 2}]}


# scratchpad: this is a preliminary test for the _prune() 

def test_pruning_level_1_is_idempotent():
    f = {"and": [{"X": 1}, {"Y": 1}]}
    assert _prune(f) == f

def test_pruning_level_2_is_idempotent():
    f1 = {"and": [{"or": [{"P": 1}, {"Q": 1}]}, {"X": 1}, {"Y": 1}]}
    f2 = {"and": [{"X": 1}, {"Y": 1}, {"or": [{"P": 1}, {"Q": 1}]},]}
    f3 = {"and": [{"or": [{"P": 1}, {"Q": 1}]}, {"or": [{"R": 1}, {"S": 1}]},]}

    assert _prune(f1) == f1    
    assert _prune(f2) == f2
    assert _prune(f3) == f3

def test_pruning_level_3_and_drops_it():
    # the fixtures need an additional term because 
    f1 = {"and": [{"or": [{"and": [{"P": 1}, {"Q": 1}]}, {"X": 1}, {"Y": 1}]}, {"Z": 1}]}
    f2 = {"and": [{"or": [{"P": 1}, {"Q": 1}, {"and": [{"X": 1}, {"Y": 1}]}]}, {"Z": 1}]}

    assert _prune(f1) == {"and": [{"or": [{"P": 1}, {"X": 1}, {"Y": 1}]}, {"Z": 1}]}
    assert _prune(f2) == {"and": [{"or": [{"P": 1}, {"Q": 1}, {"X": 1}]}, {"Z": 1}]}

def test_an_or_whose_survivor_goes_empty_pushes_nothing():
    """An ``or`` may never absorb a falsy survivor: the whole disjunction is ``{}`` (#383).

    ``{}`` carries two incompatible readings. In the weakening calculus the prune
    performs it means TRUE -- *this term no longer constrains*. :func:`_normalize_filter`
    reads it as *delete this clause*. The two agree under ``and`` (``TRUE and X`` is
    ``X``) and are exact opposites under ``or`` (``TRUE or X`` is TRUE, not ``X``).

    The fixture reaches that disagreement by the shortest route ordinary SQL allows:
    ``((P OR Q) AND (R OR S)) OR V``, depth 3. Both children of the level-2 ``and``
    are compound at level 3, so both hit the drop row and the ``and`` empties. Handed
    to the fold as a clause of the root ``or``, that ``{}`` is dropped and the filter
    collapses to ``V`` alone -- which matches FEWER rows than the WHERE, not more.
    ``recheck_where`` narrows and never widens (ADR-0022), so every row satisfying the
    left disjunct but not ``V`` is never fetched and is silently lost, with the suite
    green.

    The prune may only ever WEAKEN. Option (i): if any survivor of an ``or`` is falsy,
    the whole ``or`` is ``{}`` -- the same ``all()`` rule :func:`_is_pushable` already
    applies to a disjunction. The accepted cost is that this fixture now pushes nothing
    and full-scans; the recheck still answers it correctly.

    The second assertion is the control, and it is the half that keeps the rule honest:
    the rule must fire on a falsy SURVIVOR, not on the mere presence of a compound child
    under an ``or``. A guard that empties every compound it meets beneath an ``or``
    satisfies the first assertion and destroys the legal depth-2 filter below.

    .. seealso::

        Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.
    """
    P, Q, R, S, V = {"P": 1}, {"Q": 1}, {"R": 1}, {"S": 1}, {"V": 1}

    # depth 3: the level-2 "and" empties, so the root "or" must push nothing
    assert _prune({"or": [{"and": [{"or": [P, Q]}, {"or": [R, S]}]}, V]}) == {}

    # control: a legal depth-2 "or" carrying a compound child is left alone
    legal = {"or": [{"and": [P, Q]}, V]}
    assert _prune(legal) == legal
