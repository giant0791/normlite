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
