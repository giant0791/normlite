import pdb
import pytest

from normlite.notion_sdk.client import _CompositeCondition, _Condition, _Filter

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
            'grade': {'type': 'rich_text', 'rich_text': [{'text': {'content': 'B'}}]}
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

def test_composite_condition(page: dict):
    num_cond = _Condition(
        page, 
        {'property': 'student_id', 'number': {'greater_than': 666}}
    )

    title_cond = _Condition(
        page,
        {'property': 'name', 'title': {'contains': 'Isaac'}}
    )

    comp_cond = _CompositeCondition('and', [num_cond, title_cond])
    assert comp_cond.eval()

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
                    'property': 'student_id',
                    'number': {
                        'greater_than': 666
                    }
                }
            ]
        }
    })
    
    assert filter.eval()