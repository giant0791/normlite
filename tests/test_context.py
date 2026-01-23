from datetime import date
import pdb
import uuid
import pytest

from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _distill_params
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import insert
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Boolean, Date, Integer, String

@pytest.fixture
def metadata() -> MetaData:
    return MetaData()

@pytest.fixture
def students(metadata: MetaData) -> Table:
    return Table(
        'students',
        metadata,
        Column('name', String(is_title=True)),
        Column('id', Integer()),
        Column('is_active', Boolean()),
        Column('start_on', Date()),
        Column('grade',  String())
    )

@pytest.fixture
def insert_values() -> dict:
    return dict(
        name = 'Galileo Galilei',
        id=123456,
        is_active=False,
        start_on=date(1690,1,1),
        grade='A'
    )

#--------------------------------------------------
# Params distillation tests
#--------------------------------------------------
def test_none_distilled_as_seq_w_empty_dict():
    distilled_params = _distill_params()
    assert distilled_params == [{}]

def test_mapping_distilled_as_seq_w_one_dict():
    params = {'name': 'Galileo Galilei', 'id': 123456}
    distilled_params = _distill_params(params)
    assert len(distilled_params) == 1
    assert distilled_params[0] == params

def test_empty_seq_distilled_as_empty_seq():
    params = []
    distilled_params = _distill_params(params)
    assert distilled_params == []

def test_mappings_seq_distilled_as_mappings_seq():
    params = [
        {'name': 'Galileo Galilei', 'id': 123456},
        {'name': 'Isaac Newton', 'id':123457},
    ]
    distilled_params = _distill_params(params)
    assert distilled_params == params

def test_params_nor_mapping_nor_seq():
    params = 12345
    with pytest.raises(TypeError, match='must be a mapping or a sequence of mappings'):
        distilled_params = _distill_params(params)
    
def test_not_all_mappings():
    params = [{'name': 'Gianmarco'}, 12345, ]
    with pytest.raises(TypeError, match='multi-execute parameter sequence must be a mapping'):
        distilled_params = _distill_params(params)

#---------------------------------------------------
# Resolve parameters tests
#---------------------------------------------------
def test_resolve_params_for_insert(students: Table, insert_values: dict):
    mocked_db_id = str(uuid.uuid4())
    students.set_oid(mocked_db_id)

    insert_stmt = insert(students).values(
        **insert_values
    )

    compiled = insert_stmt.compile(NotionCompiler())
    ctx = ExecutionContext(None, compiled, distilled_params=[{"id": 6789012}])
    ctx.setup()

    assert ctx.execution_style == ExecutionStyle.EXECUTE
    payload = ctx.payload[0]
    assert payload['parent']['database_id'] == mocked_db_id
    assert payload['properties']['id']['number'] == 6789012



def _bind_params(template: dict, params: dict):
    """
    Recursively walk a dict/list/primitive template and replace any
    string of the form ':name' with params['name'].

    :param template: nested dict/list/str/etc (template)
    :param params: dict mapping param names -> values
    :return: template with values substituted
    """
    if isinstance(template, dict):
        return {k: _bind_params(v, params) for k, v in template.items()}

    elif isinstance(template, list):
        return [_bind_params(item, params) for item in template]

    elif isinstance(template, str):
        # parameter placeholder?
        if template.startswith(":"):
            name = template[1:]
            if name not in params:
                raise KeyError(f"Missing parameter: {name}")
            param = params[name]
            params.pop(name) 
            return param
        return template

    else:
        # int, float, None …
        return template


def test_bind_params_for_has_table():
    query_template = {
        'database_id': ':database_id',
        'filter': {
            'and': [
                {
                    'property': 'table_name',
                    'title' : {
                        'equals': ':table_name'
                    }
                },
                {
                    'property': 'table_catalog',
                    'rich_text': {
                        'equals': ':table_catalog'
                    }
                }
            ]
        }
    }        

    params = {
        'database_id': '66666666-6666-6666-6666-666666666666',
        'table_name': 'students',
        'table_catalog': 'university'
    }

    query = {
        'database_id': '66666666-6666-6666-6666-666666666666',
        'filter': {
            'and': [
                {
                    'property': 'table_name',
                    'title' : {
                        'equals': 'students'
                    }
                },
                {
                    'property': 'table_catalog',
                    'rich_text': {
                        'equals': 'university'
                    }
                }
            ]
        }
    }    

    bound_query = _bind_params(query_template, params)
    assert query == bound_query
    assert params == {}    

def test_bind_params_for_insert():
    query_template = {
        'properties': {
            'name': {
                'title': [{'text': {'content': ':name'}}],
                
            },
            'id': {
                'number': ':id'
            },
            'grade': {
                'rich_text': [{'text': {'content': ':grade'}}]
            },
            'since': {
                'date': {'start': ':since_start', 'end': ':since_end'}
            },
            'active': {
                'checkbox': ':active'
            }
        }
    }

    params = {
        'name': 'Galileo Galilei',
        'id': 123456,
        'grade': 'A',
        'since_start': '1670-01-01',
        'since_end': '1675-12-31',
        'active': False 
    }

    query = {
        'properties': {
            'name': {
                'title': [{'text': {'content': 'Galileo Galilei'}}],
                
            },
            'id': {
                'number': 123456
            },
            'grade': {
                'rich_text': [{'text': {'content': 'A'}}]
            },
            'since': {
                'date': {'start': '1670-01-01', 'end': '1675-12-31'}
            },
            'active': {
                'checkbox': False
            }
        }
    }

    bound_query = _bind_params(query_template, params)
    assert query == bound_query
    assert params == {}    
