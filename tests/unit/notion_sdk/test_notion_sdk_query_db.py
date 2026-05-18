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