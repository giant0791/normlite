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
from typing import Optional, Union, Any, Dict
from datetime import datetime, date, time, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from decimal import Decimal
import pdb
from types import MappingProxyType
from typing import Any, Callable, List, Literal, NoReturn, Optional, Protocol, TypeAlias, Union, TYPE_CHECKING
import uuid

from normlite.exceptions import ArgumentError, InvalidRequestError
from normlite.notion_sdk.getters import rich_text_to_plain_text
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
    
    .. versionchanged:: 0.9.0
        Add value normalization to dict for more robust result processing.
    
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
    
    def _normalize_value_for_result_processing(self, value: Union[int, float, dict]) -> dict[Union[int, float]]:
        if isinstance(value, (int, float)):
            return {
                self.get_col_spec(): value
            }
        
        if isinstance(value, dict):
            return value
        
        raise ValueError(
            'Number value can be either float, int, or dict. '
            f'Value type is: {value.__class__.__name__}'
        )

    def result_processor(self):
        def process(value: Optional[dict]) -> Optional[_NumericType]:
            if value is None:
                return None
            
            value = self._normalize_value_for_result_processing(value)
            self._raise_if_val_not_dict(value)
            num_value = value.get(self.get_col_spec())
            if isinstance(num_value, Decimal):
                num_value = float(num_value)            

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

    .. versionchanged:: 0.9.0
        Add value normalization to dict for more robust result processing.
    
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
    
    def _normalize_value_for_result_processing(self, value: Union[dict, list]) -> dict:
            if isinstance(value, list):
                return {
                    self.get_col_spec(): value
                }

            if isinstance(value, dict):
                return value
            
            raise ValueError(
                'String value must be either a dict or list. '
                f'Value type is: {value.__class__.__name__}'
            )
    
    def result_processor(self):
        def process(value: Optional[Union[dict, list]]) -> Optional[str]:
            if value is None:
                return None
        
            # **new** in 0.9.0: both dict and list values supported
            text_value = self._normalize_value_for_result_processing(value)
            
            # Notion rich_text is a list of text objects → extract 'text'
            return rich_text_to_plain_text(text_value.get(self.get_col_spec(), []))
        
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

    .. versionchanged:: 0.9.0
        Add value normalization to dict for more robust result processing.
    
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
    
    def _normalize_value_for_result_processing(self, value: Union[bool, dict]) -> dict:
        if isinstance(value, dict):
            return value
        
        if isinstance(value, bool):
            return {
                self.get_col_spec(): value
            }
        
        raise ValueError(
            f"""
                Boolean value must be either a bool or a dict.
                Type of value argument: {value.__class__.__name__}
            """
        )

    def result_processor(self):
        def process(value: Optional[Union[bool, dict]]) -> Optional[bool]:
            if value is None:
                return None
            
            value = self._normalize_value_for_result_processing(value)
            bool_value = value.get(self.get_col_spec())
            if bool_value is None:
                raise TypeError('Boolean value must have "checkbox" object')
            
            return bool_value
            
        return process
    
    @property
    def python_type(self) -> Any:
        return bool
    
    def get_dbapi_type(self) -> DBAPITypeCode:
        return DBAPITypeCode.CHECKBOX

def _normalize_datetime_value(
    value: Union[datetime, str, None],
    timezone: Optional[tzinfo],
) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value

    elif isinstance(value, date):
        dt = datetime.combine(value, time.min)

    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            try:
                d = date.fromisoformat(value)
                dt = datetime.combine(d, time.min)
            except ValueError:
                raise ValueError(f"Invalid ISO date/datetime string: {value}")

    else:
        raise ValueError(f"Unsupported datetime value: {value} ({type(value)})")

    # Apply timezone if provided and datetime is naive
    if timezone:
        if dt.tzinfo:
            dt = dt.astimezone(timezone)
        else:
            dt = dt.replace(tzinfo=timezone)

    return dt

def _parse_timezone(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError) as e:
        raise ArgumentError(f"Invalid IANA timezone: '{tz_name}' for DateTimeRange") from e
    
def _detect_date_only(
        start: Union[date, datetime, str], 
        end: Union[date, datetime, str]
) -> bool:
    def is_date_like(v: Union[date, datetime, str]):
        return isinstance(v, date) and not isinstance(v, datetime)

    def is_str_date_only(v: Union[date, datetime, str]):
        return isinstance(v, str) and "T" not in v

    return (
        (is_date_like(start) or is_str_date_only(start)) and
        (end is None or is_date_like(end) or is_str_date_only(end))
    )

class DateTimeRange:
    """Python domain object for Notion date types.

    This class provides a convenient Python type to natively represent Notion date types.
    It fully supports dates (start only), intervals (start and end), and time zones. 
    
    .. versionadded:: 0.9.0
    """
    def __init__(
        self,
        start_datetime: Union[datetime, str, None] = None,
        end_datetime: Union[datetime, str, None] = None,
        start_time_format: Optional[str] = None,
        end_time_format: Optional[str] = None,
        timezone: Optional[tzinfo | str] = None,
    ) -> None:

        # -----------------------------------------
        # Detect intent BEFORE normalization: 
        # is it a date or a datetime
        # -----------------------------------------
        self._is_date_only = _detect_date_only(start_datetime, end_datetime)

        # -----------------------------------------
        # Normalize timezone
        # -----------------------------------------
        if isinstance(timezone, str):
            timezone = _parse_timezone(tz_name=timezone)

        self.timezone: Optional[tzinfo] = timezone

        # -----------------------------------------
        # Normalize values
        # -----------------------------------------
        self.start = _normalize_datetime_value(start_datetime, timezone)
        self.end = _normalize_datetime_value(end_datetime, timezone)

        self._validate_invariants()

        self.start_time_format = start_time_format
        self.end_time_format = end_time_format

    def _validate_invariants(self) -> None:
        if self.start is None:
            raise ValueError("DateTimeRange requires a start datetime")

        if self.end is not None and self.end < self.start:
            raise ValueError("End datetime must be >= start datetime")
        
        if self._is_date_only and self.timezone is not None:
            raise ValueError(
                "Date-only values cannot have a timezone"
            )        

    # -----------------------------------------
    # JSON constructor
    # -----------------------------------------
    @classmethod
    def _from_parsed(
        cls,
        start: datetime,
        end: Optional[datetime],
        timezone: Optional[tzinfo],
        is_date_only: bool
    ) -> DateTimeRange:
        """Trusted internal constructor.
        
        This internal constructor is exclusively used by :meth:`DateTimeRange.from_json`.
        It trusts Notion to ensure wellformedness of date objects.
        Therefore, **no validation is done**.
        """
        obj = cls.__new__(cls)

        obj.start = start
        obj.end = end
        obj.timezone = timezone
        obj.start_time_format = None
        obj.end_time_format = None
        obj._is_date_only = is_date_only
        obj._validate_invariants()

        return obj
    
    @classmethod
    def from_json(cls, value: Dict[str, Any]) -> DateTimeRange:
        """
        Expected format:
        {
            "date": {
                "start": "...",
                "end": "..." | None
                "time_zone": "..." | None
            }
        }
        """
        date_obj = value["date"]

        start_raw: str = date_obj.get("start")
        end_raw: Optional[str] = date_obj.get("end")
        tz_name: Optional[str] = date_obj.get("time_zone")

        if start_raw is None:
            raise ValueError("JSON date must contain 'start' field")

        is_date_only = (
            "T" not in start_raw and
            (end_raw is None or "T" not in end_raw)
        )

        def parse(dt_str: Optional[str]) -> Optional[datetime]:
            return datetime.fromisoformat(dt_str) if dt_str else None

        start_dt = parse(start_raw)
        end_dt = parse(end_raw)

        timezone = None

        if tz_name:
            timezone = _parse_timezone(tz_name=tz_name)

            # Notion guarantees correctness → just attach
            start_dt = start_dt.replace(tzinfo=timezone)
            if end_dt:
                end_dt = end_dt.replace(tzinfo=timezone)

        return cls._from_parsed(start_dt, end_dt, timezone, is_date_only)    

    def to_json(self) -> dict:
        def serialize(dt: Optional[datetime]) -> Optional[str]:
            if dt is None:
                return None

            # -----------------------------------------
            # Date-only
            # -----------------------------------------
            if self._is_date_only:
                return dt.date().isoformat()

            # -----------------------------------------
            # Case A: timezone present → strip offset
            # -----------------------------------------
            if self.timezone is not None:
                # convert to target timezone first (safety)
                dt = dt.astimezone(self.timezone)

                # remove tzinfo → Notion expects naive string
                dt = dt.replace(tzinfo=None)

                return dt.isoformat(timespec="seconds")

            # -----------------------------------------
            # Case B: no timezone → keep offset
            # -----------------------------------------
            if dt.tzinfo is not None:
                return dt.isoformat(timespec="seconds")

            # -----------------------------------------
            # Case C: naive datetime → date-only (guarded)
            # -----------------------------------------
            if dt.time() != time.min:
                raise ValueError(
                    "Invalid DateTimeRange: naive datetime with time component "
                    "requires a timezone or tzinfo for serialization. "
                    f"Received: {dt.isoformat()}"
                )

            return dt.date().isoformat()

        return {
            "date": {
                "start": serialize(self.start),
                "end": serialize(self.end),
                "time_zone": str(self.timezone) if self.timezone else None,
            }
        }

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DateTimeRange):
            return NotImplemented

        return (
            self.start == other.start and
            self.end == other.end and
            self.timezone == other.timezone
        )

    def __repr__(self) -> str:
        def fmt(dt: datetime) -> str:
            if dt is None:
                return "None"
            
            return dt.strftime("%Y-%m-%dT%H:%M:%S%z")

        if self.end:
            return f"{fmt(self.start)} - {fmt(self.end)}"

        return fmt(self.start)

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
        def process(value: Union[str, date, datetime, DateTimeRange,    None]):
            if value is None:
                return None

            if isinstance(value, str) and value.startswith(':'):
                return {self.get_col_spec(): value}
            
            try:
                dtr = value if isinstance(value, DateTimeRange) else DateTimeRange(value)
            except Exception as e:
                raise ValueError(
                    f"{self.get_col_spec()} value must be a valid date or DateTimeRange. "
                    f"Received: {value} ({type(value).__name__})"
                ) from e

            return dtr.to_json()
        
        return process

    def result_processor(self):
        def process(value: Optional[dict]) -> Optional[DateTimeRange]:
            if value is None:
                return None

            self._raise_if_val_not_dict(value)
            return DateTimeRange.from_json(value)

        return process


    def filter_value_processor(self):
        def process(
            value: Union[DateTimeRange, datetime, date, str, None]
        ) -> Optional[str]:
            if value is None:
                return None

            # -----------------------------------------
            # Normalize to DateTimeRange
            # -----------------------------------------
            try:
                dtr = value if isinstance(value, DateTimeRange) else DateTimeRange(value)
            except Exception as e:
                raise ValueError(
                    f"{self.get_col_spec()} filter value must be a valid date/datetime/ISO string. "
                    f"Received: {value} ({type(value).__name__})"
                ) from e

            # -----------------------------------------
            # Extract start date (Notion filter semantics)
            # -----------------------------------------
            start = dtr.start

            # Notion expects date-only string for filters like on_or_after
            return start.date().isoformat()

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
            "Cannot bind values to system-managed 'object_id' columns."
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
        raise InvalidRequestError(
            "Cannot bind values to system-managed preperties 'is_archived' or 'is_deleted' columns."
        )

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
            "Cannot bind values to system-managed 'created_at' columns."
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
