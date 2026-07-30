import pdb
import pytest

from normlite.notion_sdk.client import _LogicalCondition, _Condition, _Filter

@pytest.fixture
def page() -> dict:
    return {
        'object': 'page',
        'id': '66666666-6666-6666-6666-666666666666',
        'parent': {
            'type': 'database_id',
            'database_id': '00000000-0000-0000-0000-000000000000'
        },
        'properties': {
            'student_id': {'type': 'number', 'number': 777},
            'name': {'type': 'title', 'title': [{'text': {'content': 'Isaac Newton'}}]},
            'grade': {'type': 'rich_text', 'rich_text': [{'text': {'content': 'B'}}]},
            'start_date': {'type': 'date', 'date': {'start': '2023-02-23'}}
        }
    }

        
def test_conditions(page: dict):
    num_cond = _Condition(
        page, 
        {'property': 'student_id', 'number': {'greater_than': 666}}
    )

    title_cond = _Condition(
        page,
        {'property': 'name', 'title': {'contains': 'Isaac'}}
    )

    assert num_cond.eval()
    assert title_cond.eval()

def test_composite_condition_true(page: dict):
    num_cond = _Condition(
        page, 
        {'property': 'student_id', 'number': {'greater_than': 666}}
    )

    title_cond = _Condition(
        page,
        {'property': 'name', 'title': {'contains': 'Isaac'}}
    )

    comp_cond = _LogicalCondition('and', [num_cond, title_cond])
    assert comp_cond.eval()

def test_composite_condition_false(page: dict):
    num_cond = _Condition(
        page, 
        {'property': 'student_id', 'number': {'less_than': 666}}
    )

    title_cond = _Condition(
        page,
        {'property': 'name', 'title': {'contains': 'Isaac'}}
    )
    
    comp_cond = _LogicalCondition('and', [num_cond, title_cond])
    assert not comp_cond.eval()


def test_filter_simple(page: dict):
    filter = _Filter(page, {
        'filter': {
            'property': 'grade',
            'rich_text': {
                'equals': 'A'
            }
        }
    })

    assert not filter.eval()

def test_filter_composite(page: dict):
    filter = _Filter(page, {
        'filter': {
            'and': [
                {
                    'property': 'name',
                    'title': {
                        'contains': 'Isaac'
                    }
                },
                {
                    'property': 'grade',
                    'rich_text': {
                        'equals': 'B'
                    }
                },
                {
                    'property': 'student_id',
                    'number': {
                        'greater_than': 666
                    }
                }
            ]
        }
    })
    
    assert filter.eval()

def test_filter_composite_w_date(page: dict):
    filter = _Filter(page, {
        'filter': {
            'and': [
                {
                    'property': 'name',
                    'title': {
                        'contains': 'Isaac'
                    }
                },
                {
                    'property': 'grade',
                    'rich_text': {
                        'equals': 'B'
                    }
                },
                {
                    'property': 'student_id',
                    'number': {
                        'greater_than': 666
                    }
                },
                {
                    'property': 'start_date',
                    'date': {
                        'after': '2021-05-10'
                    }
                }
            ]
        }
    })
    
    assert filter.eval()

# ------------------------------------------
# Filter on relations
# ------------------------------------------

def test_relation_contains_returns_true_only_when_id_is_in_the_relation_list():
    page_with_X = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-X'}],
            }
        }
    }
    page_with_Y = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-Y'}],
            }
        }
    }

    matching = _Condition(
        page_with_X,
        {'property': 'enrolled_in', 'relation': {'contains': 'course-X'}},
    )
    not_matching = _Condition(
        page_with_Y,
        {'property': 'enrolled_in', 'relation': {'contains': 'course-X'}},
    )

    assert matching.eval()
    assert not not_matching.eval()

def test_relation_filter_on_non_relation_property_raises_value_error():
    page = {
        'properties': {
            'name': {
                'type': 'title',
                'title': [{'text': {'content': 'Alice'}}],
            }
        }
    }

    with pytest.raises(ValueError):
        _Condition(
            page,
            {'property': 'name', 'relation': {'contains': 'some-course-id'}},
        )

def test_relation_filter_with_unsupported_operator_raises_value_error():
    page = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-X'}],
            }
        }
    }

    with pytest.raises(ValueError):
        _Condition(
            page,
            {'property': 'enrolled_in', 'relation': {'equals': 'course-X'}},
        )

def test_relation_does_not_contain_returns_true_only_when_id_is_absent():
    page_with_X = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-X'}],
            }
        }
    }
    page_with_Y = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-Y'}],
            }
        }
    }

    absent = _Condition(
        page_with_Y,
        {'property': 'enrolled_in', 'relation': {'does_not_contain': 'course-X'}},
    )
    present = _Condition(
        page_with_X,
        {'property': 'enrolled_in', 'relation': {'does_not_contain': 'course-X'}},
    )

    assert absent.eval()
    assert not present.eval()

def test_relation_contains_and_does_not_contain_on_empty_relation_list():
    unlinked = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [],
            }
        }
    }

    contains_check = _Condition(
        unlinked,
        {'property': 'enrolled_in', 'relation': {'contains': 'course-X'}},
    )
    does_not_contain_check = _Condition(
        unlinked,
        {'property': 'enrolled_in', 'relation': {'does_not_contain': 'course-X'}},
    )

    assert not contains_check.eval()
    assert does_not_contain_check.eval()

def test_relation_is_empty_returns_true_only_when_relation_list_is_empty():
    unlinked = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [],
            }
        }
    }
    linked = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-X'}],
            }
        }
    }

    empty_check = _Condition(
        unlinked,
        {'property': 'enrolled_in', 'relation': {'is_empty': None}},
    )
    non_empty_check = _Condition(
        linked,
        {'property': 'enrolled_in', 'relation': {'is_empty': None}},
    )

    assert empty_check.eval()
    assert not non_empty_check.eval()

def test_relation_is_empty_returns_true_only_when_relation_list_is_empty():
    unlinked = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [],
            }
        }
    }
    linked = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-X'}],
            }
        }
    }

    empty_check = _Condition(
        unlinked,
        {'property': 'enrolled_in', 'relation': {'is_empty': True}},
    )
    non_empty_check = _Condition(
        linked,
        {'property': 'enrolled_in', 'relation': {'is_empty': True}},
    )

    assert empty_check.eval()
    assert not non_empty_check.eval()

def test_relation_is_not_empty_returns_true_only_when_relation_list_has_items():
    unlinked = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [],
            }
        }
    }
    linked = {
        'properties': {
            'enrolled_in': {
                'type': 'relation',
                'relation': [{'id': 'course-X'}],
            }
        }
    }

    has_items = _Condition(
        linked,
        {'property': 'enrolled_in', 'relation': {'is_not_empty': True}},
    )
    no_items = _Condition(
        unlinked,
        {'property': 'enrolled_in', 'relation': {'is_not_empty': True}},
    )

    assert has_items.eval()
    assert not no_items.eval()

def test_filter_or_combines_relation_is_empty_and_contains():
    unlinked = {
        'properties': {
            'enrolled_in': {'type': 'relation', 'relation': []},
        }
    }
    linked_to_X = {
        'properties': {
            'enrolled_in': {'type': 'relation', 'relation': [{'id': 'course-X'}]},
        }
    }
    linked_to_Y = {
        'properties': {
            'enrolled_in': {'type': 'relation', 'relation': [{'id': 'course-Y'}]},
        }
    }

    filter_dict = {
        'filter': {
            'or': [
                {'property': 'enrolled_in', 'relation': {'is_empty': True}},
                {'property': 'enrolled_in', 'relation': {'contains': 'course-X'}},
            ]
        }
    }

    assert _Filter(unlinked, filter_dict).eval()         # matches via is_empty
    assert _Filter(linked_to_X, filter_dict).eval()      # matches via contains
    assert not _Filter(linked_to_Y, filter_dict).eval()  # matches neither

def test_filter_and_with_not_combines_title_scalar_and_relation_predicate():
    alice_in_X = {
        'properties': {
            'name': {'type': 'title', 'title': [{'text': {'content': 'Alice'}}]},
            'enrolled_in': {'type': 'relation', 'relation': [{'id': 'course-X'}]},
        }
    }
    alice_in_Y = {
        'properties': {
            'name': {'type': 'title', 'title': [{'text': {'content': 'Alice'}}]},
            'enrolled_in': {'type': 'relation', 'relation': [{'id': 'course-Y'}]},
        }
    }
    bob_in_X = {
        'properties': {
            'name': {'type': 'title', 'title': [{'text': {'content': 'Bob'}}]},
            'enrolled_in': {'type': 'relation', 'relation': [{'id': 'course-X'}]},
        }
    }

    # Filter: name == "Alice" AND NOT enrolled in course-Y
    filter_dict = {
        'filter': {
            'and': [
                {'property': 'name', 'title': {'equals': 'Alice'}},
                {'not': {'property': 'enrolled_in', 'relation': {'contains': 'course-Y'}}},
            ]
        }
    }

    assert _Filter(alice_in_X, filter_dict).eval()       # Alice, not in Y → both pass
    assert not _Filter(alice_in_Y, filter_dict).eval()   # Alice, IS in Y → NOT fails
    assert not _Filter(bob_in_X, filter_dict).eval()     # Bob, not in Y → AND fails on name

@pytest.mark.parametrize('prop_type', ['rich_text', 'title'])
def test_is_empty_on_a_blank_text_cell_is_true(prop_type):
    """A text cell holding one item of blank content is empty, and not non-empty.

    ``_Filter`` decodes a text cell to ``texts[0]["text"]["content"]`` and falls
    back to the ``EMPTY_TEXT`` sentinel only when the item list itself is empty,
    so ``[{"text": {"content": ""}}]`` arrives as ``""`` and both presence tests
    answer by identity against a sentinel that is not there. ``is_empty`` says
    False and ``is_not_empty`` says True -- of a cell with no content in it.

    That reads the *array length* where Notion reads the *content*. Measured
    against the real API (ADR-0019 Correction 2026-07-27): ``is_empty`` matches
    a blank cell. ``eval3`` was corrected to match in ``d9fc94e``; this is the
    same defect one evaluator over, and it is what the widened reference
    generator now walks into.

    Both operators are pinned in one test on purpose. Repairing ``is_empty``
    alone is what left ``eval3`` briefly answering True to *both* on a blank
    cell, so the complement is asserted alongside it rather than trusted to
    follow (``32e53a3``).
    """
    blank = {
        'properties': {
            'note': {'type': prop_type, prop_type: [{'text': {'content': ''}}]},
        }
    }

    empty_check = _Condition(
        blank,
        {'property': 'note', prop_type: {'is_empty': True}},
    )
    not_empty_check = _Condition(
        blank,
        {'property': 'note', prop_type: {'is_not_empty': True}},
    )

    assert empty_check.eval()
    assert not not_empty_check.eval()


@pytest.mark.parametrize(
    'cell, equals_matches, does_not_equal_matches',
    [
        (None, False, True),
        (0,    True,  False),
        (42,   False, True),
    ],
    ids=['valueless', 'zero', 'value'],
)
def test_number_does_not_equal_is_the_exact_complement_of_equals(
    cell, equals_matches, does_not_equal_matches
):
    """Notion's ``number.does_not_equal`` is the boolean complement of ``equals``.

    Measured against the real API (#384, 2026-07-30) over a data source holding
    a ``{"number": null}`` row, a ``{"number": 0}`` row and valued rows, with
    the literal ``0``: ``equals`` and ``does_not_equal`` **partition** every
    row -- disjoint and exhaustive, with no row falling outside both. That is
    the sharpest statement of the divergence #384 is about: Notion's negation
    leaves no room for a third truth value, where SQL's ``<>`` against NULL is
    UNKNOWN and puts the valueless row in *neither* set.

    The valueless case is the one that carries the claim, and the other two are
    here so it cannot be satisfied by an arm that simply always answers True.
    The pair is pinned in one test rather than two for the reason
    ``reference_eval`` spells its ``is_not_empty`` branch term-for-term against
    ``is_empty`` (``evaluator.py:141``): a complement asserted separately is a
    complement free to drift.

    ``does_not_equal`` is absent from ``_allowed_ops["number"]`` today (#381),
    so this first fails by ``ValueError`` rather than by a wrong answer. Once
    allowed, the valueless row is what forces the new rule to sit **above** the
    ``operand is None`` early return in ``_Condition.eval`` -- the guard added
    in ``0280fde`` is correct for ``equals``, ``greater_than`` and
    ``less_than``, all of which the API measured as *excluding* the valueless
    cell, and inheriting it here would answer False where Notion answers True.
    That comment at ``client.py:1769`` was written for this test.

    This is what makes #384's leaf divergence reachable by a local instrument
    at all: date can never be the vehicle, because the API rejects
    ``date.does_not_equal`` outright with a 400 (#383).
    """
    page = {
        'properties': {
            'effort': {'type': 'number', 'number': cell},
        }
    }

    equals_check = _Condition(
        page,
        {'property': 'effort', 'number': {'equals': 0}},
    )
    does_not_equal_check = _Condition(
        page,
        {'property': 'effort', 'number': {'does_not_equal': 0}},
    )

    assert equals_check.eval() is equals_matches
    assert does_not_equal_check.eval() is does_not_equal_matches


@pytest.mark.parametrize(
    'cell, is_empty_matches',
    [
        (None, True),
        (0,    False),
        (42,   False),
    ],
    ids=['valueless', 'zero', 'value'],
)
def test_number_is_empty_tests_presence_not_falsiness(cell, is_empty_matches):
    """``is_empty`` on a number asks whether the cell holds a value, not whether
    that value is falsy.

    ``{"number": 0}`` is the case that separates the two readings, and it is
    measured rather than argued: probing the live API against a data source
    holding one, ``is_empty`` does **not** match it and ``is_not_empty`` does
    (#384, and ``125aadd`` before it). Zero is a value. Spelling the check as
    falsiness -- ``not a`` -- would answer True there and quietly turn every
    zero in the table into a NULL.

    That mistake has been made in this codebase before, which is why the zero
    row is in the parametrization rather than left to the valueless row to
    imply. ``is_not_empty`` is asserted alongside as the exact complement, for
    the same reason ``reference_eval`` spells its own branch term-for-term
    (``evaluator.py:141``).

    Both operators are absent from ``_allowed_ops["number"]`` today (#381), so
    this first fails by ``ValueError``. When allowed, they need an arm **above**
    the ``operand is None`` early return in ``_Condition.eval``, exactly as
    ``date`` already has one: a presence test *does* have an answer for a
    valueless cell, so inheriting that return would make ``is_empty`` say False
    of the one cell that is empty -- inverting the operator ADR-0019 builds the
    pushdown-soundness invariant around, and the operator #381 was filed for.
    """
    page = {
        'properties': {
            'effort': {'type': 'number', 'number': cell},
        }
    }

    empty_check = _Condition(
        page,
        {'property': 'effort', 'number': {'is_empty': True}},
    )
    not_empty_check = _Condition(
        page,
        {'property': 'effort', 'number': {'is_not_empty': True}},
    )

    assert empty_check.eval() is is_empty_matches
    assert not_empty_check.eval() is not is_empty_matches
