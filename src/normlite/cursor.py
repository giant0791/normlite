# normlite/cursor.py
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

from __future__ import annotations
from typing import Any, ClassVar, Iterator, Mapping, NamedTuple, NoReturn, Optional, Sequence, Union

from normlite.exceptions import MultipleResultsFound, NormliteError, NoResultFound, ResourceClosedError
from normlite.notiondbapi.dbapi2 import CompositeCursor, Cursor, InterfaceError

class _CursorColMapRecType(NamedTuple):
    """Helper record data structure to store column metadata.
    
    This class provides a description record to enable value and type conversions between DBAPI 2.0 rows and 
    higher level class:`Row` objects.


    .. versionchanged:: 0.7.0 Added support for column identifiers.
    .. versionadded:: 0.5.0

    """
    column_name: str
    """The name of the column."""
    
    index: int
    """The column position in the description (first column --> index = 0)."""

    column_type: str
    """Currently, a string denoting the column type for conversion in the Python type system."""

    column_id: str
    """The Notion identifier for the property corresponding to this column.
    
    .. versionadded:: 0.7.0
    """

_ColMapType = dict

class _NoCursorResultMetadata:
    returns_row: ClassVar[bool] = False

    def _raise_error(self) -> NoReturn:
        raise ResourceClosedError(
            'This result object does not return rows. '
            'It has been automatically closed.'
        )
    
    @property
    def keys(self) -> Sequence[str]:
        self._raise_error()

    @property
    def key_to_index(self) -> Mapping[str, int]:
        self._raise_error()

    @property
    def index_for_key(self) -> Mapping[int, str]:
        self._raise_error()

# sentinel value for closed cursor results
_NO_CURSOR_RESULT_METADATA = _NoCursorResultMetadata()

class CursorResultMetaData(_NoCursorResultMetadata):
    """Provide helper metadata structures to access row data from low level 
    :class:`normlite.notionbdapi.dbapi2.Cursor` DBAPI 2.0.

    .. versionchanged:: 0.7.0 Added support for column identifiers.
    .. versionadded:: 0.5.0

    """
    
    returns_row = True
    """The associated cursor returns rows (e.g. ``SELECT`` statement)."""

    def __init__(self, desc: Sequence[tuple], result_columns: Optional[Sequence[str]] = None):
        self._colmap: _ColMapType = dict()
        """Mapping between column name and its description record :class:`_CursorColMapRecType`."""
        
        self._colmap = {}

        if result_columns:
            self._colmap = {
                col_name: _CursorColMapRecType(col_name, index, column_type, column_id)
                for index, (col_name, column_type, column_id, _, _, _, _) in enumerate(desc)
                if col_name in result_columns
            }            
        else:
            self._colmap = {
                col_name: _CursorColMapRecType(col_name, index, column_type, column_id)
                for index, (col_name, column_type, column_id, _, _, _, _) in enumerate(desc)
            }

        self._key_to_index: Mapping[str, int] = dict()
        """Mapping between column name and its positional index."""
       
        self._key_to_index = {
            key: rec.index
            for key, rec in self._colmap.items()
        } 
        
        self._index_for_key: Mapping[int, str] = dict()
        """Mapping between column positional index and its name."""
        
        self._index_for_key = {
            rec.index: key
            for key, rec in self._colmap.items()
        }
 
        self._keys: Sequence[str] = [key for key in self._colmap.keys()]
        """A sequence containing all the column names."""

    @property
    def keys(self) -> Sequence[str]:
        """Provide all the column names for the described row."""
        return self._keys

    @property
    def key_to_index(self) -> Mapping[str, int]:
        """Provide the mapping between column name and its positional index."""
        return self._key_to_index
    
    @property
    def index_for_key(self) -> Mapping[int, str]:
        """Provid the mapping beween the positional index of a column and its name."""
        return self._index_for_key
    
class CursorResult:
    """Provide pythonic high level interface to result sets from SQL statements.

    This class is an adapter to the DBAPI cursor (see :class:`normlite.notiondbapi.dbapi2.Cursor`) 
    representing state from the DBAPI cursor. It provides a high level API to
    access returned database rows as :class:`Row` objects. 

    Note:
        If a closed DBAPI cursor is passed to the init method, this cursor result automatically
        transitions to the closed state.

    .. versionchanged:: 0.7.0   This class now fully supports Notion property identifier as column ids. 
        Additionally, the :meth:`close()` is now available to close the underlying DBAPI cursor. 
        Therefore, all methods returning rows now check whether the cursor
        is closed and raise the :exc:`ResourceClosedError`. 
        The attribute :attr:`CursorResult.return_rows` of closed cursor result always returns ``False``.

    .. versionchanged:: 0.5.0
        The fetcher methods now check that the cursor metadata returns row prior to execution.
        This ensures that no calls to ``None`` objects are issued.
    """
    def __init__(self, cursor: Cursor, result_columns: Optional[Sequence[str]] = None):
        self._cursor = cursor
        """The underlying DBAPI cursor."""

        self._closed = False
        """``True`` if this cursor result is closed."""

        if self._cursor.description:
            self._metadata = CursorResultMetaData(self._cursor.description, result_columns)
            """The metadata object describing the DBAPI cursor."""    

        else:
            # the cursor passed has not executed any operation yet
            # or it does not returns row
            self._metadata = _NO_CURSOR_RESULT_METADATA
            self._closed = self._cursor._closed

    @property
    def returns_rows(self) -> bool:
        """``True`` if this :class:`CursorResult` returns zero or more rows.

        This attribute signals whether it is legal to call the methods: :meth:`CursorResult.fetchone()`,
        :meth:`fetchall()`, and :meth:`fetchmany()`.
        
        The truthness of this attribute is strictly in sync with whether the underlying DBAPI cursor
        had a :attr:`normlite.notiondbapi.dbapi2.Cursor.description`, which always indicates the presence
        of result columns.

        Note:
            A cursor that returns zero rows (e.g. an empty sequence from :meth:`CursorResult.all()`) 
            has still a :attr:`normlite.notiondbapi.dbapi2.Cursor.description`, if a row-returning 
            statement was executed.

        .. versionadded:: 0.5.0

        Returns:
            bool: ``True`` if this cursor result returns zero or more rows.
        """
        return self._metadata.returns_row

    def __iter__(self) -> Iterator[Row]:
        """Provide an iterator for this cursor result.

        .. versionchanged:: 0.7.0   Raise :exc:`ResourceClosedError` if it was previously closed.
        .. versionadded:: 0.5.0

        Raises:
            ResourceClosedError: If it was previously closed.

        Yields:
            Iterator[Row]: The row iterator.
        """
        self._check_if_closed()
        for raw_row in self._cursor:
            yield Row(self._metadata, raw_row)

        # close the result set
        self._metadata = _NO_CURSOR_RESULT_METADATA
        self._cursor.close()        
    
    def one(self) -> Row:
        """Return exactly one row or raise an exception.

        .. versionchanged:: 0.7.0   Raise :exc:`ResourceCloseError` if it was previously closed.
        .. versionadded:: 0.5.0

        Raises:
            ResourceClosedError: If it was previously closed.
            NoResultFound: If no row was found when one was required.
            MultipleResultsFound: If multiple rows were found when exactly one was required.
        
        Returns:
            Row: The one row required.
        """
        self._check_if_closed()

        if not self._metadata.returns_row:
            raise NoResultFound('No row was found when one was required.')
        
        if self._cursor.rowcount > 1:
            raise MultipleResultsFound('Multiple rows were found when exactly one was required.')
        
        return self.all()[0]
    
    def all(self) -> Sequence[Row]:
        """Return all rows in a sequence.

        This method closes the result set after invocation. Subsequent calls will return an empty sequence.

        .. versionchanged:: 0.7.0   Raise :exc:`ResourceCloseError` if it was previously closed.
        .. versionadded:: 0.5.0
        
        Raises:
            ResourceClosedError: If it was previously closed.
        
        Returns:
            Sequence[Row]: All rows in a sequence.
        """
        self._check_if_closed()

        if not self._metadata.returns_row:
            return list()
        
        raw_rows = self._cursor.fetchall()  # [(id_val, archived_val, in_trash_val, col1_val, ...), ...]
        row_sequence = [Row(self._metadata, row_data) for row_data in raw_rows]
        self.close()
        return row_sequence
    
    def first(self) -> Optional[Row]:
        """Return the first row or ``None`` if no row is present.

        Note:
            This method closes the result set and discards remaining rows.

        .. versionchanged:: 0.7.0  Raise :exc:`ResourceClosedError` if it was previously closed.
        .. versionadded:: 0.5.0


        Raises:
            ResourceClosedError: If it was previously closed.
        
        Returns:
            Optional[Row]: The first row in the result set or ``None`` if no row is present.
        """
        self._check_if_closed()

        if not self._metadata.returns_row:
            return None
        
        rows = list(self.all())
        return rows[0]
    
    def fetchone(self) -> Optional[Row]:
        """Fetch the next row.

        When all rows are exhausted, returns ``None``.

        .. versionadded:: 0.5.0

        Returns:
            Optional[Row]: The row object in the result.
        """
        if not self._metadata.returns_row:
            # underlying DBAPI cursor is closed or no rows returned from a query
            return None

        try:
            next_row = self._cursor.fetchone()
        except InterfaceError:
            # Either call to cursor execute() did not produce results or execute() was not called yet
            self._metadata = _NO_CURSOR_RESULT_METADATA
            self._cursor.close()        
            return None
        
        if next_row:
            # the next row is not empty ()
            return Row(self._metadata, next_row)
        
        # next_row is an empty tuple
        return None

    def fetchall(self) -> Sequence[Row]:
        """Synonim for :class:`CursorResult.all()` method.

        .. versionchanged:: 0.5.0
            This method has been refactored as a wrapper around :meth:`CursorResult.all()`.
            This ensures consistent behavior across synomin methods.

        Returns:
            Sequence[Row]: The sequence of row objects. Empty sequence if the cursor result is closed.
        """
        return self.all()
    
    def fetchmany(self) -> Sequence[Row]:
        """Fetch many rows.

        When all rows are exhausted, returns an empty sequence.

        .. versionadded:: 0.5.0

        Raises:
            NotImplementedError: Method not implemented yet.

        Returns:
            Sequence[Row]: All rows or an empty sequence when exhausted.
        """
        raise NotImplementedError
    
    def close(self) -> None:
        """Close the cursor result.
        
        After a cursor result is closed, the :attr:`returns_row` returns ``False``.

        .. versionadded:: 0.7.0
            This method closes the underlying DBAPI cursor and manages the internal state.
    
        """
        self._metadata = _NO_CURSOR_RESULT_METADATA
        self._cursor.close()
        self._closed = self._cursor._closed

    def _check_if_closed(self) -> None:
        """Raise ResourceClosedError if this cursor result is closed."""
        
        if self._closed:
            raise ResourceClosedError(
                'An operation was requested from a connection, cursor,'
                ' or other object that is in a closed state.'
            )

    
class CompositeCursorResult(CursorResult):
    """Prototype for new type of cursor result for handling multiple result sets.

    The :class:`CompositeCursorResult` is intended for use with results produced by a
    multi-statement transaction. In this case, multiple result sets are produced.
    This class introduces the :meth:`next_result()` method to advance to the next result set.
    
    .. versionadded:: 0.7.0

    Warning:
        Experimental, DON'T USE YET.

    """

    # TODO: 
    # DECIDE: Composition over inheritance?
    # THINK: CursorResultBase or better just Result in case of composition?
    # --------------------------------------------------------------------------------------
    # 1. Put current CursorResult implementation into CursorResultBase
    # 2. Refactor CursorResult as subclass of CursorResultBase and use this implementation
    # 3. Add CursorResultBase.close() and CursorResult.close() methods 
    # --------------------------------------------------------------------------------------
    
    def __init__(self, dbapi_cursor: CompositeCursor):
        self._dbapi_cursor = dbapi_cursor
        self._current_result = CursorResult(self._dbapi_cursor._current_cursor)

    def next_result(self) -> bool:
        """Advance to the next cursor, if available."""
        if self._dbapi_cursor.nextset():
            # next result set is available
            # first close the current cursor result
            # TODO: Replace with self._current_cursor.close()
            self._current_result._metadata = _NO_CURSOR_RESULT_METADATA

            # update the current cursor result
            self._current_result = CursorResult(self._dbapi_cursor._current_cursor)
            return True
        
        # all result sets depleted
        # TODO: add self.close()
        return False
    
    def one(self) -> Row:
        return self._current_result.one()
    
    def all(self) -> Sequence[Row]:
        return self._current_result.all()

class Row:
    """Provide pythonic high level interface to a single SQL database row.

    .. versionchanged:: 0.7.0
        :meth:`__getattr__()` has been added to provide access to row values with Python dot notation.
    
    .. versionchanged:: 0.5.0
        :class:`Row` has been significantly extended to provide iteratable capabilities
        and a mapping-sytle object to access the values of the columns returned in the row.
        
    """
    
    def __init__(self, metadata: CursorResultMetaData, row_data: tuple):
        # Use object.__setattr__ to bypass __setattr__ during initialization
        self._metadata = metadata
        """The metadata object to process raw rows."""
        
        self._values = list(row_data)
        """Thew column values."""

    def __getattr__(self, key: str) -> Any:
        """Provide access with dot notation to row values."""
        try:
            col_idx = self._metadata.key_to_index[key]
            return self._values[col_idx]
        except KeyError as err:
            raise AttributeError(key) from err

    def __getitem__(self, key_or_index: Union[str, int]) -> Any:
        """Provide keyed and indexed access to the row values. 

        Providing this method enables row object to be iterated::
        
            >>> for value in row:
            ...     print(f"{value = }")
            value = '680dee41-b447-451d-9d36-c6eaff13fb45'
            value = False
            value = False
            value = 12345
            value = 'B'
            value = 'Isaac Newton'

        .. versionchanged:: 0.5.0
            Now, it supports both keyes and indexed access. Error handling is more
            robust and consistent.
            
        Args:
            key_or_index (Union[str, int]): The value's key (column name) or index (column positional). 

        Raises:
            IndexError: If index is out or range.
            KeyError: If row has no column named ``key_or_index``.
            TypeError: If the provided index is neither ``str`` (column name) or ``int`` (column index).

        Returns:
            Any: The value for the column the key or index has been provided.
        """

        if isinstance(key_or_index, int):
            try:
                return self._values[key_or_index]
            except IndexError:
                raise IndexError(f"{type(self).__name__} index out of range: {key_or_index}")
            
        elif isinstance(key_or_index, str):
            try:
                col_name = self._metadata.key_to_index[key_or_index]
                return self._values[col_name]
            except KeyError:
                raise KeyError(f"{type(self).__name__} has no column named {key_or_index!r}")
            
        else:
            raise TypeError(
                f"{type(self).__name__} indices must be str (column name) or int (column index), "
                f"not {type(key_or_index).__name__}"
            )
        
    def mapping(self) -> dict:
        """Provide the mapping object for this row.
        
        .. versionadded:: 0.5.0
        
        """
        return RowMapping(self)

    def __repr__(self):
        return f"Row({{ {', '.join(f'{k!r}: {self[k]!r}' for k in self._metadata.key_to_index)} }})"

class RowMapping(Mapping[str, Any]):
    """Helper to construct mapping objects for rows.
    
    :class:`RowMapping` provides a dedicated mapping implementation for column name, column value pairs.
        
    .. versionadded:: 0.5.0
    
    """
    def __init__(self, row: Row):
        self._row = row
        """The underlying row object."""

        self._mapping = {
            key: value 
            for key, value in zip(self._row._metadata.keys, self._row)
        }
        """The mapping object created from the row object."""

    def __getitem__(self, key):
        return self._row[key]

    def __iter__(self):
        return iter(self._row)

    def __len__(self):
        return len(self._row._metadata.keys)
    
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Mapping):
            return dict(self.items()) == dict(other.items())
        return NotImplemented

    def keys(self):
        return self._mapping.keys()

    def values(self):
        return self._mapping.values()

    def items(self):
        return self._mapping.items()
