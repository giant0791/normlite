import pytest

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
