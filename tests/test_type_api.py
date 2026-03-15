
from datetime import datetime
from decimal import Decimal
import pdb

import pytest

from normlite import (
    Boolean, 
    Date, 
    Integer, 
    Money, 
    Numeric, 
    String, 
    TypeEngine
)

from normlite.exceptions import InvalidRequestError
from normlite.sql.type_api import ObjectId, ArchivalFlag

@pytest.mark.parametrize('type_obj, no_obj, py_obj, no_type', [
    (
        Integer(), 
        {"number": 25}, 
        25, 
        "number"
    ),
    (
        Numeric(), 
        {"number": 2.5}, 
        Decimal(2.5), 
        "number"
    ), 
    (
        Money('euro'), 
        {"number": 1.8}, 
        Decimal(1.8), 
        "number"
    ),
    (
        Boolean(), 
        {"checkbox" : True}, 
        True, 
        "checkbox"
    ),
    (
        String(), 
        {'rich_text':[{"text": {"content": "A nice, woderful day with you"}}]},
        "A nice, woderful day with you", 
        "rich_text"
    ),
    (
        String(is_title=True), 
        {'title':[{"text": {"content": "A nice, woderful day with you"}}]}, 
        "A nice, woderful day with you", 
        "title"
    ),
])
def test_type_engine_scalar_datatypes(type_obj: TypeEngine, no_obj: dict, py_obj: object, no_type: dict):
    bind = type_obj.bind_processor()
    result = type_obj.result_processor()

    bound = bind(py_obj)
    restored = result(no_obj)

    assert result(bind(py_obj)) == py_obj   
    assert bound == no_obj
    assert restored == py_obj
    assert type_obj.get_col_spec() == no_type

@pytest.mark.parametrize('type_obj, no_obj, py_obj, no_type', [
    (
        Date(), 
        {"date": {"start": "2023-02-23T00:00:00", "end": None}}, 
        (datetime(2023, 2, 23), None), 
        "date"
    ),
    (
        Date(), 
        {"date": {"start": "2023-02-23T00:00:00", "end": "2023-04-23T00:00:00"}}, 
        (datetime(2023,2,23), datetime(2023,4,23)), 
        "date"
    ),
])
def test_type_engine_date_datatypes(type_obj: TypeEngine, no_obj: dict, py_obj: object, no_type: dict):
    bind = type_obj.bind_processor()
    result = type_obj.result_processor()

    bound = bind(py_obj[0])
    restored = result(no_obj)
    #pdb.set_trace()

    assert result(bind(py_obj[0])) == py_obj   
    assert bound == no_obj
    assert restored == py_obj
    assert type_obj.get_col_spec() == no_type

def test_objectid_raises_if_bind_is_attempted():
    type_obj = ObjectId()
    with pytest.raises(InvalidRequestError):
        bind = type_obj.bind_processor()

def test_archivalflag_raises_if_bind_is_attempted():
    type_obj = ArchivalFlag()
    with pytest.raises(InvalidRequestError):
        bind = type_obj.bind_processor()

