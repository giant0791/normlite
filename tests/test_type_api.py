
from datetime import datetime
from decimal import Decimal
import pdb
from zoneinfo import ZoneInfo

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

from normlite.exceptions import ArgumentError, InvalidRequestError
from normlite.sql.type_api import DateTimeRange, ObjectId, ArchivalFlag

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

def test_type_engine_datetimerange_from_json_start_only():
    dt_obj = {
        "date": {
            "start": "2026-03-26"
        }
    } 

    dtr = DateTimeRange.from_json(dt_obj)

    assert dtr.start == datetime(2026,3,26)
    assert dtr.end is None

def test_type_engine_datetimerange_from_json_start_only_w_tz():
    dt_obj = {
        "date": {
            "start": "2026-03-26T23:59:59",
            "time_zone": "America/Los_Angeles"
        }
    } 

    dtr = DateTimeRange.from_json(dt_obj)
    tzinfo = ZoneInfo("America/Los_Angeles")

    assert dtr.start == datetime(2026,3,26, 23, 59, 59, tzinfo=tzinfo)
    assert dtr.end is None
    assert dtr.timezone == tzinfo

def test_type_engine_datetimerange_from_json_start_end():
    dt_obj = {
        "date": {
            "start": "2026-03-26",
            "end": "2026-04-26"
        }
    } 

    dtr = DateTimeRange.from_json(dt_obj)

    assert dtr.start == datetime(2026,3,26)
    assert dtr.end == datetime(2026,4,26)

def test_type_engine_datetimerange_from_json_no_start_raises():
    dt_obj = {
        "date": {
            "end": "2026-04-26"
        }
    } 

    with pytest.raises(ValueError, match="must contain 'start' field"):
        dtr = DateTimeRange.from_json(dt_obj)

def test_type_engine_datetimerange_from_json_end_before_start_raises():
    dt_obj = {
        "date": {
            "start":"2026-05-26",
            "end": "2026-04-26"
        }
    } 

    with pytest.raises(ValueError, match=">= start datetime"):
        dtr = DateTimeRange.from_json(dt_obj)

def test_type_engine_datetimerange_invalid_tz_raises_from_json():
    dt_obj = {
        "date": {
            "start": "2026-03-26",
            "end": "2026-04-26",
            "time_zone": "Invalid/Zone"
        }
    } 

    with pytest.raises(ArgumentError) as exc:
        dtr = DateTimeRange.from_json(dt_obj)

    assert "IANA timezone: 'Invalid/Zone'" in str(exc.value)

def test_type_engine_datetimerange_dt_only_w_tz_raises():

    dt_obj = {
        "date": {
            "start": "2026-03-26",
            "time_zone": "Europe/Berlin"
        }
    } 

    with pytest.raises(ValueError) as exc:
        dtr = DateTimeRange.from_json(dt_obj)

    assert "Date-only values cannot have a timezone" in str(exc.value)

@pytest.mark.parametrize('type_obj, no_obj, py_obj, no_type', [
    (
        Date(), 
        {"date": {"start": "2023-02-23", "end": None, "time_zone": None}}, 
        DateTimeRange("2023-02-23"), 
        "date"
    ),
    (
        Date(), 
        {"date": {"start": "2023-02-23", "end": "2023-04-23", "time_zone": None}}, 
        DateTimeRange(datetime(2023,2,23), datetime(2023,4,23)), 
        "date"
    ),
    (
        Date(), 
        {"date": {"start": "2023-02-23T00:00:00", "end": "2023-04-23T00:00:00", "time_zone": "Europe/Rome"}}, 
        DateTimeRange(datetime(2023,2,23), datetime(2023,4,23), timezone="Europe/Rome"), 
        "date"
    ),
])
def test_type_engine_date_datatypes(type_obj: TypeEngine, no_obj: dict, py_obj: object, no_type: dict):
    bind = type_obj.bind_processor()
    result = type_obj.result_processor()

    bound = bind(py_obj)
    restored = result(no_obj)

    assert result(bind(py_obj)) == py_obj   
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

