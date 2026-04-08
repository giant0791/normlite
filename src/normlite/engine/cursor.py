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
import pdb
from typing import Iterator, Optional, Sequence

from normlite.engine.context import ExecutionContext
from normlite.exceptions import MultipleResultsFound, NoResultFound, ResourceClosedError
from normlite.engine.resultmetadata import _NO_CURSOR_RESULT_METADATA, CursorResultMetaData
from normlite.engine.row import Row
from normlite.notiondbapi.dbapi2 import Cursor, InterfaceError
from normlite.sql.base import Compiled
 
class CursorResult:
    """Provide pythonic high level interface to result sets from SQL statements.

    This class is an adapter to the DBAPI cursor (see :class:`normlite.notiondbapi.dbapi2.Cursor`) 
    representing state from the DBAPI cursor. It provides a high level API to
    access returned database rows as :class:`Row` objects. 
    CursorResult is a **consuming, read-only façade** over a DBAPI cursor.
    It does not mutate result data, but **does manage cursor exhaustion and closure**.

    Note:
        If a closed DBAPI cursor is passed to the init method, this cursor result automatically
        transitions to the closed state.

    .. versionchanged:: 0.8.0
        This class now provides results for row-returning (DML) statements only.
        The :meth:`CursorResult.__init__` method receives the whole execution context. This enables the cursor
        result to process the result set data in a context-oriented way. This means, the cursor result object
        can now determine whether the rows returned are data or reflected metadata.

    .. versionchanged:: 0.7.0   
        This class now fully supports Notion property identifier as column ids. 
        Additionally, the :meth:`close()` is now available to close the underlying DBAPI cursor. 
        Therefore, all methods returning rows now check whether the cursor
        is closed and raise the :exc:`ResourceClosedError`. 
        The attribute :attr:`CursorResult.return_rows` of closed cursor result always returns ``False``.

    .. versionchanged:: 0.5.0
        The fetcher methods now check that the cursor metadata returns row prior to execution.
        This ensures that no calls to ``None`` objects are issued.
    """

    context: ExecutionContext
    """The execution context this cursor result belongs to.
    
    .. versionadded:: 0.8.0
    """

    def __init__(
            self, 
            context: ExecutionContext,
    ) -> None:
        self.context = context
        
        self._cursor = context.cursor
        """The underlying DBAPI cursor."""

        self._compiled = context.compiled
        """The execution context that produced this cursor result.
        
        .. versionadded: 0.8.0
        """

        self._closed = False
        """``True`` if this cursor result is closed."""

        if self._cursor.description:
            self._metadata = CursorResultMetaData(
                self._cursor.description, 
                self._compiled.is_ddl,
            )

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

    @property
    def rowcount(self) -> int:
        """ Return the 'rowcount' for this result.

        The primary purpose of 'rowcount' is to report the number of rows
        matched by the WHERE criterion of an UPDATE or DELETE statement
        executed once (i.e. for a single parameter set), which may then be
        compared to the number of rows expected to be updated or deleted as a
        means of asserting data integrity.

        This attribute is transferred from the ``cursor.rowcount`` attribute
        of the DBAPI before the cursor is closed. The Notion DBAPI supports row count.
        In order to retrieve ``cursor.rowcount`` for these statements, set the
        :attr:`normlite.engine.interfaces.ExecutionOptions.preserve_rowcount` execution option to ``True``, 
        which will cause the :attr:`normlite.notiondbapi.dbapi2.Cursor.rowcount` value to be unconditionally memoized before 
        any results are returned or the cursor is closed, regardless of statement type.

        .. versionadded:: 0.9.0

        """
        rowcount = self.context.get_rowcount()
        return -1 if rowcount is None else rowcount
    
    @property
    def returned_primary_keys_rows(self) -> Optional[list[tuple]]:
        """Return the value of :attr:`normlite.engine.context.ExecutionContext._returned_primary_keys_rows
        as a list of tuples representing rows.
        
        .. versionadded:: 0.9.0
        """
        return self.context._returned_primary_keys_rows
    
    
    def __iter__(self) -> Iterator[Row]:
        """Provide an iterator for this cursor result.

        .. versionchanged:: 0.9.0
            This version adds support for multiple result sets returned by the underlying DBAPI cursor.

        .. versionchanged:: 0.7.0   
            Raise :exc:`ResourceClosedError` if it was previously closed.
        
        .. versionadded:: 0.5.0

        Raises:
            ResourceClosedError: If it was previously closed.

        Yields:
            Iterator[Row]: The row iterator.
        """
        self._check_if_closed()

        for raw_row in self._cursor._iter_all():
            yield Row(self._metadata, raw_row)

        self.close()    

    def _soft_close(self) -> None:
        """Soft close the cursor result.
        
        Soft closing means the underlying result set is closed (:attr:`returns_row` is `` False``), 
        but the cursor result itself it is not, that is no :exc:`normlite.exceptions.ResourceCloseError` 
        is raised.
        This method is private and it is used by user classes that are perform result interpretation.

        .. versionadded:: 0.9.0
        """
        self._metadata = _NO_CURSOR_RESULT_METADATA

    def one(self) -> Row:
        """Return exactly one row or raise an exception.

        Note:
            This method soft-closes the result set.

        .. versionchanged:: 0.9.0
            Soft-closing logic added.
            
        .. versionchanged:: 0.7.0   
            Raise :exc:`normlite.exceptions.ResourceCloseError` if it was previously closed.
        
        .. versionadded:: 0.5.0

        Raises:
            ResourceClosedError: If it was previously closed.
            NoResultFound: If no row was found when one was required.
            MultipleResultsFound: If multiple rows were found when exactly one was required.
        
        Returns:
            Row: The one row required.
        """
        self._check_if_closed()

        row = self.fetchone()
        if row is None:
            raise NoResultFound("No row was found when one was required.")

        second = self.fetchone()
        if second is not None:
            raise MultipleResultsFound(
                "Multiple rows were found when exactly one was required."
            )

        self._soft_close()
        return row
    
    def all(self) -> Sequence[Row]:
        """Return all rows in a sequence.

        This method closes the result set after invocation. Subsequent calls will return an empty sequence.

        Note:
            This method soft-closes the result set.
        
        .. versionchanged:: 0.9.0
            This version adds support for multiple result sets returned by the underlying DBAPI cursor,
            and soft-closing logic.
            
        .. versionchanged:: 0.7.0   
            Raise :exc:`ResourceCloseError` if it was previously closed.

        .. versionadded:: 0.5.0
        
        Raises:
            ResourceClosedError: If it was previously closed.
        
        Returns:
            Sequence[Row]: All rows in a sequence.
        """
        self._check_if_closed()

        if not self._metadata.returns_row:
            return []

        rows = [Row(self._metadata, raw) for raw in self._cursor._iter_all()]

        self._soft_close()
        return rows    
    
    def first(self) -> Optional[Row]:
        """Return the first row or ``None`` if no row is present.

        Note:
            This method soft-closes the result set and discards remaining rows.

        .. versionchanged:: 0.9.0
            After this method is 

        .. versionchanged:: 0.7.0  
            Raise :exc:`normlite.exceptions.ResourceClosedError` if it was previously closed.

        .. versionadded:: 0.5.0


        Raises:
            ResourceClosedError: If it was previously closed.
        
        Returns:
            Optional[Row]: The first row in the result set or ``None`` if no row is present.
        """
        self._check_if_closed()

        if not self._metadata.returns_row:
            return None

        row = self.fetchone()
        self._soft_close()
        return row
    
    def fetchone(self) -> Optional[Row]:
        """Fetch the next row.

        When all rows are exhausted, returns ``None``.

        .. versionchanged:: 0.9.0
            This version adds support for multiple result sets returned by the underlying DBAPI cursor.

        .. versionadded:: 0.5.0

        Returns:
            Optional[Row]: The row object in the result.
        """
        self._check_if_closed()

        if not self._metadata.returns_row:
            return None

        while True:
            raw = self._cursor.fetchone()
            if raw is not None:
                return Row(self._metadata, raw)

            # move to next result set
            if not self._cursor.nextset():
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
    
    def fetchmany(self, size: Optional[int] = None) -> Sequence[Row]:
        """Fetch many rows.

        When all rows are exhausted, returns an empty sequence.

        .. versionchanged:: 0.9.0
            This version adds support for multiple result sets returned by the underlying DBAPI cursor.

        .. versionadded:: 0.5.0

        Returns:
            Sequence[Row]: All rows or an empty sequence when exhausted.
        """
        self._check_if_closed()

        if not self._metadata.returns_row:
            return []

        n = self._cursor.arraysize if size is None else size
        result = []

        while len(result) < n:
            chunk = self._cursor.fetchmany(n - len(result))
            result.extend(Row(self._metadata, r) for r in chunk)

            if len(result) >= n:
                break

            if not self._cursor.nextset():
                break

        return result    

    def close(self) -> None:
        """Close the cursor result.
        
        After a cursor result is closed, the :attr:`returns_row` returns ``False``, and
        all public API methods raise :exc:`normlite.exceptions.ResourceClosedError`.

        .. versionchanged:: 0.9.0
            This version uses :meth:`_soft_close` to soft-close the underlying DBAPI cursor.

        .. versionadded:: 0.7.0
            This method closes the underlying DBAPI cursor and manages the internal state.
    
        """
        self._soft_close()
        self._cursor.close()
        self._closed = self._cursor._closed

    def _check_if_closed(self) -> None:
        """Raise ResourceClosedError if this cursor result is closed."""
        
        if self._closed:
            raise ResourceClosedError(
                'An operation was requested from a connection, cursor,'
                ' or other object that is in a closed state.'
            )

