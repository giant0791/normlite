from __future__ import annotations
from datetime import datetime
from decimal import Decimal
import pdb
from typing import Any, Callable, List, Literal, Optional, Protocol, TypeAlias, Union

from typing import Callable, Optional

import pytest

class TypeEngine(Protocol):
    """Base class for all Notion/SQL datatypes."""

    def bind_processor(self, dialect) -> Optional[Callable[[Any], Any]]:
        """Python → SQL/Notion (prepare before sending)."""
        return None

    def result_processor(self, dialect, coltype) -> Optional[Callable[[Any], Any]]:
        """SQL/Notion → Python (process values after fetching)."""
        return None

    def get_col_spec(self, dialect) -> str:
        """Return a string for the SQL-like type name."""
        raise NotImplementedError
    
_NumericType: TypeAlias = Union[int, Decimal]
Currency = Literal['dollar', 'euro', 'franc', 'krona', 'pound', 'yuan']
    
class Number(TypeEngine):
    """Notion-specific number type. Can represent integer, decimal, percent, or currency."""

    def __init__(self, format: str):
        """
        format options (per Notion API):
        - "number" → integer
        - "number_with_commas" → decimal
        - "percent"
        - "currency"
        """
        self.format = format

    def get_col_spec(self, dialect):
        return {"type": "number", "number": {"format": self.format}}

    def bind_processor(self, dialect):
        def process(value: Optional[_NumericType]) -> Optional[dict]:
            if value is None:
                return None
            return {"number": value}
        return process

    def result_processor(self, dialect, coltype=None):
        def process(value: Optional[dict]) -> Optional[_NumericType]:
            if value is None:
                return None
            number = value['number']
            return Decimal(number) if self.format != "number" else int(number)
        return process

class Integer(Number):
    """Covenient type engine for Notion "number" objetcs with format = "number"."""

    def __init__(self):
        super().__init__('number')

class Numeric(Number):
    """Convenient type engine for Notion "number" objects with format ="number_with_commas"."""

    def __init__(self):
        super().__init__('number_with_commas')

class Money(Number):
    """Convenitent type engine for Notion "number" objects handling currencies."""
    def __init__(self, currency: Currency):
        super().__init__(currency)

class String(TypeEngine):
    def __init__(self, is_title: bool = False):
        self._is_title = is_title
        """``True`` if it is a "title", ``False`` if it is a "richt_text"."""

    def bind_processor(self, dialect):
        def process(value: Optional[str]) -> Optional[List[dict]]:
            if value is None:
                return None
            block = {}
            block['plain_text'] = str(value)
            return [block]
        return process

    def result_processor(self, dialect, coltype=None):
        def process(value: Optional[dict]) -> Optional[str]:
            if value is None:
                return None
             # Notion rich_text is a list of text objects → extract plain_text
            return "".join([block.get("plain_text", "") for block in value])
        return process

    def get_col_spec(self, dialect):
        return {"type": "title"} if self._is_title else {"type": "richt_text"}
    
class Boolean(TypeEngine):
    def get_col_spec(self, dialect):
        return {"type": "checkbox"}

    def bind_processor(self, dialect):
        def process(value: Optional[bool]) -> Optional[dict]:
            if value is None:
                return None
            return {"checkbox": bool(value)}
        return process

    def result_processor(self, dialect, coltype=None):
        def process(value: Optional[dict]) -> Optional[bool]:
            if value is None:
                return None
            return bool(value['checkbox'])
        return process
    
class Date(TypeEngine):
    def bind_processor(self, dialect):
        def process(value: Optional[Union[tuple, datetime]]) -> Optional[dict]:
            if value is None:
                return None
            
            if isinstance(value, tuple):
                start, end = value
                if not start:
                    raise ValueError('Date must have a start (end is optional)')
                
                if not isinstance(start, datetime):
                    raise ValueError(f'Start date must be a valid datetime, received: {start}')
                
                if not end and not isinstance(end, datetime):
                    raise ValueError(f'End date must be a valid datetime, received: {end}')

                return {
                    "start": start.isoformat(),
                    "end": end.isoformat() if end else None,
                }
            
            if isinstance(value, datetime):
                return {"start": value.isoformat(), "end": None}
            
            raise TypeError("Date must be datetime or (start, end) tuple")
        return process

    def result_processor(self, dialect, coltype=None):
        def process(value: Optional[dict]) -> Optional[Union[tuple, datetime]]:
            if value is None:
                return None
            
            if isinstance(value, dict):
                start = datetime.fromisoformat(value["start"]) if value.get("start") else None
                end = datetime.fromisoformat(value["end"]) if value.get("end") else None
                restored = (start, end)
            
                if restored == (None, None):
                    return None
                
                if restored[1] is None:
                    return restored[0]
                
                return restored
            
        return process

    def get_col_spec(self, dialect):
        return {"type": "date"}

@pytest.mark.parametrize('type_obj, no_obj,py_obj', [
    (Integer(), {"number": 25}, 25),
    (Numeric(), {"number": 2.5}, Decimal(2.5)),
    (Money('euro'), {"number": 1.8}, Decimal(1.8)),
    (Boolean(), {"checkbox" : True}, True),
    (Date(), {"start": "2023-02-23T00:00:00", "end": None}, datetime(2023, 2, 23)),
    (Date(), {"start": "2023-02-23T00:00:00", "end": "2023-04-23T00:00:00"}, (datetime(2023,2,23), datetime(2023,4,23))),
    (String(), [{"plain_text": "A nice, woderful day with you"}], "A nice, woderful day with you")
])
def test_typeengine_bind_pydata(type_obj: TypeEngine, no_obj, py_obj):
    bind = type_obj.bind_processor(dialect=None)
    result = type_obj.result_processor(dialect=None, coltype=None)

    bound = bind(py_obj)
    restored = result(no_obj)

    assert result(bind(py_obj)) == py_obj
    assert bound == no_obj
    assert restored == py_obj
    

