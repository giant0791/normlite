# normlite/sql/type_api.py
# Copyright (C) 2025 Gianmarco Antonini
#
# This module is part of normlite and is released under the GNU Affero General Public License.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Provide the type system for SQL constructs and to handle data conversion Python <--> Notion.
The central class is :class:`TypeEngine`, which lays the foundation for SQL datatypes in ``normlite``.
It defines a _contract_ (interface + partial implementation) for how Python values are:
1. Bound into SQL literals (Python → SQL).
2. Converted from SQL result rows (SQL → Python).

Concrete subclasses of :class:`TypeEngine` represent actual SQL types:
* :class:`Integer` for integer values
* :class:`String` for string values
* :class:`Boolean` for boolean values
* :class:`Numeric` for decimal values
* :class:`Date` for datetime and datetime range values
* .class:`Money` for currency values

All concrete :class:`TypeEngine` subclasses are designed in a backend-agnostic way. 
This allows to define datatypes without the Notion specific details.
The Notion type system is thus treated as an SQL dialect.
Subclasses of :class:`TypeEngine` are used to define the datatype of table columns.

Usage::

    # define an integer SQL datatype
    int_dt = Integer()
    
    # get bind and result processors
    bind = int_dt.bind_processor(dialect=None)
    result = int_dt.result_processor(dialect=None, coltype=None)

    # covert Python datatype <--> Notion datatype
    bind(25)                    # --> {"number": 25}
    result({"number": 25})      # --> 25

    # get columns specification (Notion type representation)
    int_dt.get_col_spec(dialect=None)   # -> {"type": "number"}

    # define a string SQL datatype
    str_dt = String(is_title=True)

    # get bind and result processors    
    bind = int_dt.bind_processor(dialect=None)
    result = int_dt.result_processor(dialect=None, coltype=None)

    # covert Python datatype <--> Notion datatype
    bind("A nice, woderful day with you")                       # --> [{"plain_text": "A nice, woderful day with you"}]
    result([{"plain_text": "A nice, woderful day with you"}])   # --> "A nice, woderful day with you"

    # get columns specification (Notion type representation)
    str_dt.get_col_spec(dialect=None)   # -> {"type": "title"}

.. versionadded:: 0.7.0

"""

from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable, List, Literal, Optional, Protocol, TypeAlias, Union
import uuid

class TypeEngine(Protocol):
    """Base class for all Notion/SQL datatypes.
    
    .. versionadded:: 0.7.0
    """

    def bind_processor(self, dialect) -> Optional[Callable[[Any], Any]]:
        """Python → SQL/Notion (prepare before sending)."""
        return None

    def result_processor(self, dialect, coltype) -> Optional[Callable[[Any], Any]]:
        """SQL/Notion → Python (process values after fetching)."""
        return None

    def get_col_spec(self, dialect) -> str:
        """Return a string for the SQL-like type name."""
        raise NotImplementedError
    
    def __repr__(self):
        return self.__class__.__name__
    
_NumericType: TypeAlias = Union[int, Decimal]
"""Type alias for numeric datatypes. It is not part of the public API."""

_DateTimeRangeType: TypeAlias = Union[tuple[datetime, datetime], datetime]
"""Type alias for datetime datatypes. It is not part of the publich API."""

Currency = Literal['dollar', 'euro', 'franc', 'krona', 'pound', 'yuan']
"""Literal alias for the currently supported currencies. 
These are the same literal strings as defined by Notion.
"""
    
class Number(TypeEngine):
    """Notion-specific number type. Can represent integer, decimal, percent, or currency.
    
    .. versionadded:: 0.7.0
    """

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
    """Covenient type engine for Notion "number" objetcs with format = "number".
    
    .. versionadded:: 0.7.0
    """

    def __init__(self):
        super().__init__('number')

class Numeric(Number):
    """Convenient type engine for Notion "number" objects with format ="number_with_commas".
    
    .. versionadded:: 0.7.0
    """

    def __init__(self):
        super().__init__('number_with_commas')

class Money(Number):
    """Convenient type engine for Notion "number" objects handling currencies.
    
    .. versionadded:: 0.7.0
    """
    def __init__(self, currency: Currency):
        super().__init__(currency)

class String(TypeEngine):
    """Textual type for Notion title and rich text properties.
    
    Usage:
        >>> # create a title property
        >>> title_txt = String(is_title=True)
        >>> title_text.get_col_spec(None)
        {"type": "title"}

        >>> # create a rich text property
        >>> rich_text = String()
        >>> rich_text.get_col_spec(None)
        {"type": "rich_text"}

    .. versionadded:: 0.7.0
    """
    def __init__(self, is_title: bool = False):
        self.is_title = is_title
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
        return {"type": "title"} if self.is_title else {"type": "rich_text"}
    
    def __repr__(self) -> str:
        kwarg = []
        if self.is_title:
            kwarg.append('is_title')
        
        return "String(%s)" % ", ".join(
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg]
        )
    
class Boolean(TypeEngine):
    """Covenient type engine class for "checkbox" objects.
    
    .. versionadded:: 0.7.0
    """
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
    """Convenient type engine class for "date" objects.
    
    .. versionadded:: 0.7.0
    """
    def bind_processor(self, dialect):
        def process(value: Optional[_DateTimeRangeType]) -> Optional[dict]:
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

class UUID(TypeEngine):
    """Base type engine class for UUID ids.
    
    .. versionadded:: 0.7.0
    """
    def bind_processor(self, dialect):
        def process(value: Optional[Union[str, uuid.UUID]]) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, uuid.UUID):
                return str(value)   # JSON-safe
            return str(uuid.UUID(value))      # parse from string
        return process

    def result_processor(self, dialect, coltype=None):
        def process(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            return str(uuid.UUID(value))      # parse from string
        return process

    def get_col_spec(self, dialect):
        return "UUID"

class ObjectId(UUID):
    """Special UUID type representing Notion's "id" property.
    
    .. versionadded:: 0.7.0
    """
    def get_col_spec(self, dialect):
        return "id"

class ArchivalFlag(Boolean):
    """Special Boolean type representing Notion's "archived" property.
    
    .. versionadded:: 0.7.0
    """

    def get_col_spec(self, dialect):
        # In Notion JSON, this is always stored under property 'archived'
        return "archived"

    def bind_processor(self, dialect):
        def process(value: Optional[bool]) -> Optional[bool]:
            if value is None:
                return None
            return bool(value)
        return process

    def result_processor(self, dialect, coltype=None):
        def process(value: Optional[bool]) -> Optional[bool]:
            if value is None:
                return None
            return bool(value)
        return process

