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
    bind = int_dt.bind_processor()
    result = int_dt.result_processor()

    # covert Python datatype <--> Notion datatype
    bind(25)                    # --> {"number": 25}
    result({"number": 25})      # --> 25

    # get columns specification (Notion type representation)
    int_dt.get_col_spec()       # -> {"type": "number"}

    # define a string SQL datatype
    str_dt = String(is_title=True)

    # get bind and result processors    
    bind = int_dt.bind_processor()
    result = int_dt.result_processor()

    # covert Python datatype <--> Notion datatype
    bind("A nice, woderful day with you")                       # --> [{"plain_text": "A nice, woderful day with you"}]
    result([{"plain_text": "A nice, woderful day with you"}])   # --> "A nice, woderful day with you"

    # get columns specification (Notion type representation)
    str_dt.get_col_spec()   # -> {"type": "title"}

.. versionadded:: 0.7.0

"""

from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
import pdb
from types import MappingProxyType
from typing import Any, Callable, List, Literal, NoReturn, Optional, Protocol, TypeAlias, Union, TYPE_CHECKING
import uuid

from normlite.exceptions import InvalidRequestError
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql.elements import Operator, BooleanComparator, Comparator, NumberComparator, DateComparator, ObjectIdComparator, StringComparator, TimeStampStringISO8601Comparator


class TypeEngine(Protocol):
    """Base class for all Notion/SQL datatypes.

    .. versionchanged:: 0.8.0
        :class:`TypeEngine` now defines a new processor API for processing filter values.
        Notion requires this additional processor API as filter values used in queries
        are different from page values.

    .. versionadded:: 0.7.0
    """

    comparator_factory: Comparator
    supported_ops: dict[Operator, str]

    def bind_processor(self) -> Optional[Callable[[Any], Any]]:
        """Python → SQL/Notion (prepare before sending)."""
        return None

    def result_processor(self) -> Optional[Callable[[Any], Any]]:
        """SQL/Notion → Python (process values after fetching)."""
        return None
    
    def filter_value_processor(self) -> Optional[Callable[[Any], Any]]:
        return None 

    def get_col_spec(self) -> str:
        """Return a string for the SQL-like type name."""
        raise NotImplementedError
    
    def get_notion_spec(self) -> dict:
        """Return a dictionary used for creating database properties in the JSON payload.
        
        The default implementation returns a dictionary as follows:

        .. code:: python

            {
                self.get_col_spec(): {}
            }

        Subclasses can add additional element.

        .. seealso::

            :meth:`Number.get_notion_spec`

        .. versionadded:: 0.8.

        """

        return {
            self.get_col_spec(): {}
        }
    
    @property
    def python_type(self) -> Any:
        """Return the Python type object expected to be returned
        by instances of this type.

        Basically, for those types which enforce a return type,
        or are known across the board to do such for all common
        DBAPIs (like ``int`` for example), will return that type.

        By default the generic ``object`` type is returned.

        Returns:
            Any: The Python type corresponding to this type engine instance.

        .. versionadded:: 0.9.0
        """
        return object
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        """ Return the DBAPI type code corresponding to this type engine instance."""
        raise NotImplemented

    def __repr__(self):
        return self.__class__.__name__
    
    def _raise_if_val_not_dict(self, value: dict) -> NoReturn:
        if not isinstance(value, dict):
                raise ValueError(
                    f'{self.get_col_spec()} value must be a dict. '
                    f'Value type is: {value.__class__.__name__}'
                )
    
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

    comparator_factory = NumberComparator
    supported_ops = MappingProxyType({
        Operator.EQ: "equals",
        Operator.NE: "does_not_equal",
        Operator.LT: "less_than",
        Operator.GT: "greater_than",
        Operator.LE: "less_than_or_equal_to",
        Operator.GE: "greater_than_or_equal_to",
        Operator.IS_EMPTY: "is_empty",
        Operator.IS_NOT_EMPTY: "is_not_empty"
    })

    def __init__(self, format: str):
        """
        format options (per Notion API):
        - "number" → integer
        - "number_with_commas" → decimal
        - "percent"
        - "currency"
        """
        self.format = format

    def get_col_spec(self):
        return 'number'
    
    def get_notion_spec(self):
        return {
            self.get_col_spec(): {
                'format': self.format
            }
        }

    def bind_processor(self):
        def process(value: Optional[Union[_NumericType, str]]) -> Optional[dict]:
            if value is None:
                return None
            return {self.get_col_spec(): value}
        return process

    def result_processor(self):
        def process(value: Optional[dict]) -> Optional[_NumericType]:
            if value is None:
                return None
            
            self._raise_if_val_not_dict(value)
            num_value = value.get('number')
            if isinstance(num_value, Decimal):
                num_value = float(num_value)            
            if not isinstance(num_value, (float, int,)):
                raise ValueError(
                    'Number value can be either float or int. '
                    f'Value type is: {num_value.__class__.__name__}'
                )
            return Decimal(num_value) if isinstance(num_value, float) else int(num_value)
        return process

    def __repr__(self) -> str:
        kwarg = []
        if self.format:
            kwarg.append('format')
        
        return "Number(%s)" % ", ".join(
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg]
        )

class Integer(Number):
    """Covenient type engine for Notion "number" objetcs with format = "number".
    
    .. versionadded:: 0.7.0
    """

    comparator_factory = NumberComparator

    def __init__(self):
        super().__init__('number')

    @property
    def python_type(self) -> Any:
        return int
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.NUMBER

class Numeric(Number):
    """Convenient type engine for Notion "number" objects with format ="number_with_commas".
    
    .. versionadded:: 0.7.0
    """

    def __init__(self):
        super().__init__('number_with_commas')

    @property
    def python_type(self) -> Any:
        return Decimal
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.NUMBER_WITH_COMMAS

class Money(Number):
    """Convenient type engine for Notion "number" objects handling currencies.
    
    .. versionadded:: 0.7.0
    """
    def __init__(self, currency: Currency):
        super().__init__(currency)

    @property
    def python_type(self) -> Any:
        return Decimal
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        if self.format == "dollar":
            return DBAPITypeCode.NUMBER_DOLLAR
        
        raise NotImplementedError(f"Number format '{self.format}' not supported in this version.")

class String(TypeEngine):
    """Textual type for Notion title and rich text properties.
    
    Usage:
        >>> # create a title property
        >>> title_txt = String(is_title=True)
        >>> title_text.get_col_spec()
        {"title": {}}

        >>> # create a rich text property
        >>> rich_text = String()
        >>> rich_text.get_col_spec()
        {"rich_text": {}}

    .. versionadded:: 0.7.0
    """

    comparator_factory = StringComparator
    supported_ops = MappingProxyType({
        Operator.EQ: "equals",
        Operator.NE: "does_not_equal",
        Operator.IN: "contains", 
        Operator.NOT_IN: "does_not_contain",
        Operator.ENDSWITH: "ends_with",
        Operator.STARTSWITH: "starts_with",
        Operator.IS_EMPTY: "is_empty",
        Operator.IS_NOT_EMPTY: "is_not_empty"
    })
 
    def __init__(self, is_title: bool = False):
        self.is_title = is_title
        """``True`` if it is a "title", ``False`` if it is a "richt_text"."""

    def bind_processor(self):
        def process(value: Optional[str]) -> Optional[List[dict]]:
            if value is None:
                return None
            return {self.get_col_spec(): [{'text': {'content': str(value)}}]}
        return process
    
    def _normalize_value_for_result_processing(self, value: Union[dict, list]) -> list:
            if isinstance(value, list):
                return value

            if isinstance(value, dict):
                text_value = value.get(self.get_col_spec())
            
                if not isinstance(text_value, list):
                    raise ValueError(
                        f'{self.get_col_spec()} value must be a list. '
                        f'Value type is: {value.__class__.__name__}'
                    )

                return text_value
    
            # **new** string result processor must handle both dict **and** list.
            raise ValueError(
                f'{self.get_col_spec()} value must be either a dict or a list. '
                f'Value type is: {value.__class__.__name__}'
            )

    def result_processor(self):
        def process(value: Optional[Union[dict, list]]) -> Optional[str]:
            if value is None:
                return None
        
            # **new** in 0.9.0: both dict and list values supported
            text_value = self._normalize_value_for_result_processing(value)
            
            # Notion rich_text is a list of text objects → extract 'text'
            return "".join([block.get("text").get("content") for block in text_value])
        return process

    def get_col_spec(self):
        return "title" if self.is_title else "rich_text"
    
    def __repr__(self) -> str:
        kwarg = []
        if self.is_title:
            kwarg.append('is_title')
        
        return "String(%s)" % ", ".join(
            ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg]
        )
    
    @property
    def python_type(self) -> Any:
        return str
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.TITLE if self.is_title else DBAPITypeCode.RICH_TEXT

class Boolean(TypeEngine):
    """Covenient type engine class for "checkbox" objects.
    
    .. versionadded:: 0.7.0
    """
    comparator_factory = BooleanComparator
    supported_ops = MappingProxyType({
        Operator.EQ: "equals",
        Operator.NE: "does_not_equal",
    })

    def get_col_spec(self):
        return "checkbox"

    def bind_processor(self):
        def process(value: Optional[bool]) -> Optional[dict]:
            if value is None:
                return None
            if isinstance(value, str):
                # bind parameter: :is_active or :param_01
                return {self.get_col_spec(): value}
            return {self.get_col_spec(): bool(value)}
        return process

    def result_processor(self):
        def process(value: Optional[Union[bool, dict]]) -> Optional[bool]:
            if value is None:
                return None
            
            if isinstance(value, dict):
                bool_value = value.get(self.get_col_spec())
                if bool_value is None:
                    raise TypeError('Boolean value must have "checkbox" object')
                return bool_value
            
            if isinstance(value, bool):
                return value

            raise ValueError(
                f"""
                    Boolean value must be either a bool or a dictionary "checkbox" object.
                    Type of value argument: {value.__class__.__name__}
                """
            )
        return process
    
    @property
    def python_type(self) -> Any:
        return bool
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.CHECKBOX

class Date(TypeEngine):
    """Convenient type engine class for "date" objects.
    
    .. versionadded:: 0.8.0
        Operators supported by this type engine.

    .. versionadded:: 0.7.0
    """
    comparator_factory = DateComparator
    supported_ops = MappingProxyType({
        Operator.EQ: "equals",
        Operator.NE: "does_not_equal",
        Operator.AFTER: "after",
        Operator.BEFORE: "before",
        Operator.IS_EMPTY: "is_empty",
        Operator.IS_NOT_EMPTY: "is_not_empty"
    })

    def bind_processor(self):
        def process(value: Optional[_DateTimeRangeType]) -> Optional[dict]:
            if value is None:
                return None
            
            if isinstance(value, str) and value.startswith(':'):
                return {self.get_col_spec(): value}
            
            if isinstance(value, tuple):
                start, end = value
                if not start:
                    raise ValueError('Date must have a start (end is optional)')
                
                if not isinstance(start, datetime):
                    raise ValueError(f'Start date must be a valid datetime, received: {start}')
                
                if not end and not isinstance(end, datetime):
                    raise ValueError(f'End date must be a valid datetime, received: {end}')

                return {
                    self.get_col_spec(): {
                        "start": start.isoformat(),
                        "end": end.isoformat() if end else None
                    }
                }
            
            # IMPORTANT: Both data types must be checked as both are valid
            if isinstance(value, (date, datetime,)):
                return { 
                    self.get_col_spec(): {
                        "start": value.isoformat(), 
                        "end": None
                    }
                }
            
        return process

    def result_processor(self):
        def process(value: Optional[dict]) -> Optional[tuple[Optional[datetime], Optional[datetime]]]:
            if value is None:
                return None

            self._raise_if_val_not_dict(value)
            date_value = value.get("date")

            if not date_value:
                return None

            start_raw = date_value.get("start")
            end_raw = date_value.get("end")

            start = datetime.fromisoformat(start_raw) if start_raw else None
            end = datetime.fromisoformat(end_raw) if end_raw else None

            if start is None and end is None:
                return None

            return (start, end)
        return process
    
    def filter_value_processor(self):
        def process(value: Union[datetime, date, str]) -> Optional[str]:
            if value is None:
                return None
            
            if not isinstance(value, (datetime, date, str,)):
                raise ValueError(
                    f'{self.get_col_spec()} value must be either a date or a str. '
                    f'Value type is: {value.__class__.__name__}.'
                )

            if isinstance(value, str):
                value = datetime.fromisoformat(value)

            return value.isoformat()
        return process

    def get_col_spec(self):
        return "date"

    @property
    def python_type(self) -> Any:
        return tuple
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.DATE

class UUID(TypeEngine):
    """Base type engine class for UUID ids.

    .. versionchanged:: 0.9.0
        :meth:`bind_processor` raises :exc:`normlite.exceptions.InvalidRequestError`:
        Object ids are in Notion **read-only** properties.
    
    .. versionadded:: 0.7.0
    """
    def bind_processor(self):
        raise InvalidRequestError(
            "Cannot bind values to system-managed object/property id columns."
        )

    def result_processor(self):
        def process(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            return str(uuid.UUID(value))      # parse from string
        return process

    def get_col_spec(self):
        return "UUID"

    @property
    def python_type(self) -> Any:
        return str
    
class PropertyId(TypeEngine):
    """Type engine class for property identifiers.
    
    .. versionadded:: 0.8.0
        This solves the issue of generating the description for pages that were created or updated.
        See issue `#136 <https://github.com/giant0791/normlite/issues/136>`.

    """
    def bind_processor(self):
        raise InvalidRequestError(
            "Cannot bind values to system-managed preperty 'id' columns."
        )

    def result_processor(self):
        def process(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None
            return value      
        return process

    def get_col_spec(self):
        raise NotImplementedError('Column spec is not supported for this type engine subclass.')
  
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.PROPERTY_ID

class ObjectId(UUID):
    """Special UUID type representing Notion's "id" property.
    
    .. versionadded:: 0.7.0
    """
    comparator_factory = ObjectIdComparator

    def get_col_spec(self):
        raise NotImplementedError('Column spec is not supported for this type engine subclass.')
  
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.ID

class ArchivalFlag(Boolean):
    """Special Boolean type representing Notion "in_trash" and "archived" keys.

    .. versionchanged:: 0.9.0
        This version support provision of the DBAPI type code for full integration into result-set
        schema information.
    
    .. versionadded:: 0.7.0
    """

    def get_col_spec(self):
        raise NotImplementedError('Column spec is not supported for this type engine subclass.')
    
    def bind_processor(self):
        def process(value: Optional[bool]) -> Optional[bool]:
            if value is None:
                return None
            return bool(value)
        return process

    def result_processor(self):
        def process(value: Optional[bool]) -> Optional[bool]:
            if value is None:
                return None
            return bool(value)
        return process
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.ARCHIVAL_FLAG

class TimeStampStringISO8601(TypeEngine):
    """Special type representing Notion "created_time" and "last_edited_time" keys.
    
    .. versionadded:: 0.9.0
    """

    comparator_factory = TimeStampStringISO8601Comparator

    def get_col_spec(self):
        raise NotImplementedError('Column spec is not supported for this type engine subclass.')
    
    def _validate_iso8601(self, value: str) -> None:
        """Validate ISO 8601 compatibility."""
        try:
            normalized = value.replace("Z", "+00:00")
            datetime.fromisoformat(normalized)
        except Exception as exc:
            raise ValueError(
                f"Value '{value}' is not a valid ISO 8601 timestamp."
            ) from exc

    def bind_processor(self):
        raise InvalidRequestError(
            "Cannot bind values to system-managed timestamp columns."
        )

    def result_processor(self):
        def process(value: Optional[str]) -> Optional[str]:
            if value is None:
                return None

            if not isinstance(value, str):
                raise ValueError(
                    f"Expected ISO 8601 string from Notion, got {type(value).__name__}"
                )

            self._validate_iso8601(value)
            return value
        
        return process

    @property
    def python_type(self) -> Any:
        return str
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.TIMESTAMP

type_mapper: dict[str, TypeEngine] = {
    DBAPITypeCode.ID: ObjectId(),
    DBAPITypeCode.PROPERTY_ID: PropertyId(),
    DBAPITypeCode.TITLE: String(is_title=True),
    DBAPITypeCode.RICH_TEXT: String(),
    DBAPITypeCode.CHECKBOX: Boolean(),
    DBAPITypeCode.NUMBER: Integer(),
    DBAPITypeCode.NUMBER_WITH_COMMAS: Numeric(),
    DBAPITypeCode.NUMBER_DOLLAR: Money('dollar'),
    DBAPITypeCode.DATE: Date(),
    DBAPITypeCode.ARCHIVAL_FLAG: ArchivalFlag(),
    DBAPITypeCode.TIMESTAMP: TimeStampStringISO8601(),
}
