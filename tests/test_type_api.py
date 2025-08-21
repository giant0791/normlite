
from datetime import datetime
from decimal import Decimal
import pdb
from typing import Optional, Union

import pytest

from normlite import (
    ArchivalFlag, 
    Boolean, 
    Date, 
    Integer, 
    Money, 
    Numeric, 
    ObjectId, 
    String, 
    TypeEngine
)


@pytest.mark.parametrize('type_obj, no_obj, py_obj, no_type', [
    (
        Integer(), 
        {"number": 25}, 
        25, 
        {"type": "number", "number": {"format": "number"}}
    ),
    (
        Numeric(), 
        {"number": 2.5}, 
        Decimal(2.5), 
        {"type": "number", "number": {"format": "number_with_commas"}}
    ), 
    (
        Money('euro'), 
        {"number": 1.8}, 
        Decimal(1.8), 
        {"type": "number", "number": {"format": "euro"}}
    ),
    (
        Boolean(), 
        {"checkbox" : True}, 
        True, 
        {"type": "checkbox"}
    ),
    (
        Date(), 
        {"start": "2023-02-23T00:00:00", "end": None}, 
        datetime(2023, 2, 23), 
        {"type": "date"}
    ),
    (
        Date(), 
        {"start": "2023-02-23T00:00:00", "end": "2023-04-23T00:00:00"}, 
        (datetime(2023,2,23), datetime(2023,4,23)), 
        {"type": "date"}
    ),
    (
        String(), 
        [{"plain_text": "A nice, woderful day with you"}], 
        "A nice, woderful day with you", 
        {"type": "rich_text"}
    ),
    (
        String(is_title=True), 
        [{"plain_text": "A nice, woderful day with you"}], 
        "A nice, woderful day with you", 
        {"type": "title"}
    ),
    (
        ObjectId(),
        "59833787-2cf9-4fdf-8782-e53db20768a5",
        "59833787-2cf9-4fdf-8782-e53db20768a5",
        "id"
    ),
    (
        ArchivalFlag(),
        True,
        True,
        "archived"
    )
])
def test_typeengine_datatypes(type_obj: TypeEngine, no_obj: dict, py_obj: object, no_type: dict):
    bind = type_obj.bind_processor(dialect=None)
    result = type_obj.result_processor(dialect=None, coltype=None)

    bound = bind(py_obj)
    restored = result(no_obj)

    assert result(bind(py_obj)) == py_obj
    assert bound == no_obj
    assert restored == py_obj
    assert type_obj.get_col_spec(dialect=None) == no_type

