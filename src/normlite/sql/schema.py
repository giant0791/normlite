# normlite/sql/schema.py
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

"""Provide the key abstractions for database metadata.
Each class in this module describe a database entity: tables, columns, constraints, etc.

Example of a table creation:
    >>> students = Table(
    >>>     'students',
    >>>     Column('id', Integer(), primary_key=True),
    >>>     Column('name', String(is_title=True)),
    >>>     Column('grade', String()),
    >>>     Column('since' Date())
    >>> )

``normlite`` automatically adds the Notion object id as additional primary key:
    >>> print(repr(students))
    Table('students', 
        Column('student_id', Integer, table=<students>, primary_key=True), 
        Column('name', String(is_title=True), table=<students>), 
        Column('grade', String(), table=<students>), 
        Column('since', Date, table=<students>), 
        Column('_no_id', ObjectId, table=<students>, primary_key=True), 
        Column('_no_archived', ArchivalFlag, table=<students>))

Note: 
    The Notion archived property is automatically added too. By default, the ``_no_id`` column
    is a primary key. Notion always generates unique object ids, so it is guarateed that no 2 pages with 
    the same object identifiers exist. The primary key constraint automatically contains all columns with
    primary key set to ``True``. That is, if you define your own primary key column (in the above example, the 
    column ``student_id``), the primary key constraint will add it to its collection of columns as well along
    with the ``_no_id``.

You can inspect the table's primary key constraints by calling the :attr:`primary_key` as follows:
    >>> print(repr(stundents.primary_key))
    PrimaryKeyConstraint(name=None, 
        (Column('student_id', Integer, table=<students>, primary_key=True), 
        (Column('_no_id', ObjectId, table=None, primary_key=True)))

.. versionadded:: 0.7.0

"""
from __future__ import annotations
from typing import Any, Dict, Iterable, Iterator, List, NoReturn, Optional, Set, Tuple, Union, overload, TYPE_CHECKING

from normlite.exceptions import ArgumentError, DuplicateColumnError, InvalidRequestError
from normlite.sql.type_api import ArchivalFlag, ObjectId, TypeEngine
from normlite.sql.dml import Insert

if TYPE_CHECKING:
    from normlite.engine import Engine

class Column:
    """A single table column specifying the type and its constraints.

    The :class:`Column` objects model Notion properties. You define a column by assigning it a name
    and a type (for available types, refer to :mod:`normlite.sql.type_api.py`). 
    If a column is intended to act as a primary key in your database, then set the
    argument ``primary_key`` to true (refer to :class:`PrimaryKeyConstraint` for more details).

    Example:
        >>> col = Column('name', String(is_title=True))
        >>> print(repr(col))
        ... Column('name', String(is_title=True), table=None)

    Note:
        Currently, only the primary key constraint is supported.
        In the future, the ``nullable`` constraint might be supported too.

    .. versionadded:: 0.7.0

    """
    def __init__(self, name: str, type_: TypeEngine, primary_key: bool = False):
        self.name = name
        """The column name. This must be unique within the same table."""

        self.type_ = type_
        """The column type as a concrete subclass of :class:`normlite.sql.type_api.TypeEnging`."""

        self.primary_key = primary_key
        """Whether this column is a primary key or not."""

        self.parent: Table = None
        """The table this column belongs to. 
        Initially ``None``, it is set when this column is appended to its table.
        See :meth:`Table.append_column()` for more details.
        """

    def _set_parent(self, parent: Table) -> None:
        """Set the table this columns belongs to.
        This method is not intended to be called by users of the :class:`Column`,
        because it is automatically set by the :class:`Table`.
        """
        self.parent = parent

    def __repr__(self):
        kwarg = []
        if self.primary_key:
            kwarg.append('primary_key')
        
        return "Column(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(self.type_)]
            + [
                (
                    self.parent is not None
                    and "table=<%s>" % self.parent.name
                    or "table=None"
                )
            ]
            + ["%s=%s" % (k, repr(getattr(self, k))) for k in kwarg]
        )

class Table:
    """A database table.
    
    The :class:`Table` models a Notion database object. It provides the central abstraction in 
    ``normlite`` core to manage database objects.
    A table contains a collection of columns (see :class:`ColumnCollection` which provides dictionary-like
    and indexed access to a table's columns).
    
    Tables are defined as follows:
        >>> students = Table(
        >>>     'students',
        >>>     Column('student_id', Integer(), primary_key=True),
        >>>     Column('name', String(is_title=True)),
        >>>     Column('grade', String()),
        >>>     Column('since' Date())
        >>> )

    Tables have to attributes :attr:`Table.columns` and its convenient short version :attr:`Table.c` which
    provide dictionary-like and indexed accessors to its columns:

        >>> # access column "name"
        >>> students.columns.name
        ...
        >>> # or just
        >>> students.c.name
        ...
        >>> # via string
        >>> students.c['name'] 
        ...
        >>> # indexed access
        >>> students.c[1]       # --> 'name'
        ...
        >>> # via a slice
        >>> students.c[0:2]     # --> a readonly collection of the columns: 'student_id' and 'name'
        ...
        >>> # access a column's table
        >>> students.c.since.parent is students
        ...
        >>> # iterate through the columns
        >>> for col in students.columns:
        >>>     print(repr(col))
        ...
        >>> # check whether a given column belongs to the table's columns
        >>> 'name' in students.c            # --> True
        >>> '_no_archived' in students.c    # --> True
        >>> 'non_existing' in students.c    # --> False
        ...
        >>> # get the number of colums
        >>> len(students.columns)           # --> 4 user defined + 2 auto-added = 6

    Tables can also be reflected using the keyword argument ``autoload_with`` as follows:

        >>> students = Table('students', autoload_with=engine)
        >>> [c.name for c in students.columns]
        ['student_id', 'name', 'grade', 'is_active', '_no_id', '_no_archived']

    If you use ``autoload_with``, you cannot specify the columns. 
    Doing so results in a :exc:`normlite.exceptions.ArgumentError` being raised.

    Note:
        Since Notion automatically generates several special properties to its pages, 
        the :class:`Table` automatically adds the following two columns:

            >>> print(repr(students))
            >>> Table('students', 
            ...     Column('student_id', Integer, table=<students>, primary_key=True), 
            ...     Column('name', String(is_title=True), table=<students>), 
            ...     Column('grade', String(), table=<students>), 
            ...     Column('since', Date, table=<students>), 
            ...     Column('_no_id', ObjectId, table=None, primary_key=True), 
            ...     Column('_no_archived', ArchivalFlag, table=<students>))

        
    .. versionadded: 0.7.0

    """
    def __init__(self, name: str, *columns: Column, dialect=None, **kwargs: Any):
        self.name = name
        """Table name."""

        self._columns: ColumnCollection = ColumnCollection()
        """The underlying column collections for this table's column.
        
        .. seealso::

            :class:`ColumnCollection`

        """

        self._constraints: Set[Constraint] = set()
        """The set of constraints associated to this table's columns."""

        self._primary_key: PrimaryKeyConstraint = None
        """The primary key constraint
        
        .. seealso::

            :class:`PrimaryKeyConstraint`

            :attr:`Table.primary_key`

        """

        self._database_id = None
        """The Notion id corresponding to this table."""

        if kwargs:
            if columns:
                raise ArgumentError('Columns cannot be specified when using autoload_with keyword argument')
            
            if 'autoload_with' in kwargs:
                self._autoload(kwargs['autoload_with'])
        
        if columns:
            # add user-declared columns
            for col in columns:
                self.append_column(col)

            # Always add implicit Notion columns
            self._ensure_implicit_columns()

            # Generate primary key constraint object
            self._create_pk_constraint()
            self.add_constraint(self._primary_key)

    @property
    def columns(self) -> ReadOnlyColumnCollection:
        """Accessor for this table's columns.
        
        It returns a ready-only copy column collection.
        """
        return self._columns.as_readonly()
    
    @property
    def c(self) -> ReadOnlyColumnCollection:
        """Short form synonim for :attr:`columns`."""
        return self._columns.as_readonly()
    
    @property
    def primary_key(self) -> PrimaryKeyConstraint:
        """Return the primary key constraint object associated to this table.

        Example:
            >>> print(repr(stundents.primary_key))
            >>> PrimaryKeyConstraint(name=None, 
            ...     (Column('student_id', Integer, table=<students>, primary_key=True), 
            ...     (Column('_no_id', ObjectId, table=None, primary_key=True)))

        """
        return self._primary_key
    
    def add_constraint(self, constraint: Constraint) -> None:
        """Add a constraint to this table."""
        constraint._set_parent(self)
        self._constraints.add(constraint)

    def append_column(self, column: Column):
        """Append a column to this table."""
        column._set_parent(self)
        self._columns.add(column)

    def insert(self) -> Insert:
        """Generate a new SQL insert statement for this table."""

        insert_stmt = Insert()
        insert_stmt._set_table(self)
        return insert_stmt

    def _ensure_implicit_columns(self):
        # Notion object ID: always primary key
        if "_no_id" not in self._columns:
            _no_id_col = Column("_no_id", ObjectId(), primary_key=True)
            _no_id_col._set_parent(self)
            self._columns.add(_no_id_col)

        # Archival flag: always present
        if "_no_archived" not in self._columns:
            _no_archived_col = Column("_no_archived", ArchivalFlag())
            _no_archived_col._set_parent(self)
            self._columns.add(_no_archived_col)

    def _create_pk_constraint(self) -> None:
        table_pks = [c for c in self._columns if c.primary_key]
        # IMPORTANT: Here you have to unpack the table_pks list
        self._primary_key = PrimaryKeyConstraint(*table_pks)

    def _autoload(self, engine: Engine) -> None:
        from normlite.engine import Inspector

        inspector: Inspector = engine.inspect()
        inspector.reflect_table(self)

    def __repr__(self) -> str:
        return "Table(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(x) for x in self.columns]
        )
    
class ColumnCollection:
    """Provide a container to efficiently store and conveniently access a table's columns.

    This class provides a table's column accessor with dictionary-like and indexed interface.
    It allows unique column names only. 
    Column collection objects provide an iterator interface (incl. :meth:`__len__()`) as well as
    a method to test column existence in the collection (see :meth:`__contains__()`).

    .. versionadded:: 0.7.0

    """
    __slots__ = ('_collection', '_index', '_colset')

    _collection: List[Tuple[str, Column]]
    _index: Dict[Union[None, str, int], Tuple[str, Column]]
    _colset: Set[Column]

    def __init__(self, columns: Optional[Iterable[Tuple[str, Column]]] = None):
        object.__setattr__(self, '_colset', set())
        object.__setattr__(self, '_index', {})
        object.__setattr__(self, '_collection', [])
        if columns:
            self._populate_separate_keys(columns)

    def add(self, column: Column) -> None:
        """Add a new column to the collection.

        Args:
            column (Column): The column to be added.

        Raises:
            DuplicateColumnError: If a column with the same name as the column to be added 
                                  already exists in the collection.
        """
        colkey = column.name
        if colkey in self._index:
            raise DuplicateColumnError(f'ColumnCollection does not allow duplicate columns: {colkey}')

        l = len(self._collection)
        self._collection.append((colkey, column))
        self._colset.add(column)
        self._index[l] = (colkey, column)
        self._index[colkey] = (colkey, column)

    def _populate_separate_keys(self, iter_: Iterable[Tuple[str, Column]]) -> None:
        cols = list(iter_)

        for colname, col in cols:
            if colname != col.name:
                raise ArgumentError(
                    'ColumnCollection requires columns be under '
                    'the same name as their .name'
                )
            if colname in self._index:
                raise DuplicateColumnError(
                    f'ColumnCollection does not allow duplicate columns: {colname}'
                )
            
            self._index[colname] = (colname, col)
            self._collection.append((colname, col))

        self._colset.update(c for (_, c) in self._collection)
        self._index.update(
            (idx, (k, c)) for idx, (k, c) in enumerate(self._collection)
        )

    def __contains__(self, key: str) -> bool:
        if key not in self._index:
            if not isinstance(key, str):
                raise ArgumentError(
                    "__contains__ requires a string argument"
                )
            return False
        else:
            return True
        
    def __len__(self) -> int:
        return len(self._collection)
    
    def len(self, usr_def_only: Optional[bool] = True) -> int:
        if usr_def_only:
            non_no_cols = [
                colname 
                for colname, _ in self._collection 
                if not colname.startswith('_no_')
            ]
            return len(non_no_cols)
        return self.__len__()

    def __iter__(self) -> Iterator[Column]:
        # turn to a list first to maintain over a course of changes
        return iter([col for (_, col) in self._collection])

    def __getattr__(self, key: str) -> Column:
        try:
            return self._index[key][1]
        except KeyError as err:
            raise AttributeError(key) from err
        
    def __str__(self) -> str:
        return "%s(%s)" % (
            self.__class__.__name__,
            ", ".join(str(c) for c in self),
        )
    
    @overload
    def __getitem__(self, key: Union[str, int]) -> Column:
        ...

    @overload
    def __getitem__(self, key: slice) -> ReadOnlyColumnCollection:
        ...

    def __getitem__(self, key: Union[str, int, slice]) -> Union[Column, ReadOnlyColumnCollection]:
        """Indexed accessor to the columns in the collection.

        Args:
            key (Union[str, int, slice]): The index key.

        Raises:
            IndexError: If the index key provided is incorrect.

        Returns:
            Union[Column, ReadOnlyColumnCollection]: A single column if an int index or a read-only collection for a slice index. 
        """
        try:
            if isinstance(key, slice):
                cols = ((sub_key, col) for (sub_key, col) in self._collection[key])
                return ColumnCollection(cols).as_readonly()
            
            return self._index[key][1]
        except KeyError as ke:
            if isinstance(ke.args[0], int):
                raise IndexError(ke.args[0]) from ke
            else:
                raise

    def __setitem__(self, key: str, value: Any) -> NoReturn:
        raise NotImplementedError()

    def __delitem__(self, key: str) -> NoReturn:
        raise NotImplementedError()

    def __setattr__(self, key: str, obj: Any) -> NoReturn:
        raise NotImplementedError()

    def clear(self) -> NoReturn:
        raise NotImplementedError()

    def remove(self, column: Any) -> NoReturn:
        raise NotImplementedError()

    def update(self, iter_: Any) -> NoReturn:
        raise NotImplementedError()

    def as_readonly(self) -> ReadOnlyColumnCollection:
        """Return a read-only collection."""
        return ReadOnlyColumnCollection(self)
    
class ReadOnlyCollectionMixin:
    """Mixin for read-only collections."""
    __slots__ = ()

    def _readonly(self) -> NoReturn:
        cls_name = self.__class__.__name__
        raise TypeError(f'{cls_name} object is immutable and/or readonly.')
    
    def __delitem__(self, key: Any) -> NoReturn:
        self._readonly()

    def __setitem__(self, key: Any, value: Any) -> NoReturn:
        self._readonly()

    def __setattr__(self, key: Any, value: Any) -> NoReturn:
        self._readonly()

class ReadOnlyColumnCollection(ReadOnlyCollectionMixin, ColumnCollection):
    """Read-only version of a column collection."""
    def __init__(self, collection: ColumnCollection):
        object.__setattr__(self, '_parent', collection)
        object.__setattr__(self, '_colset', collection._colset)
        object.__setattr__(self, '_index', collection._index)
        object.__setattr__(self, '_collection', collection._collection)

class Constraint:
    """Ancestor class for all column constraints."""
    def __init__(self, name: Optional[str] = None):
        self._name = name
        self._parent: Table = None

    @property    
    def table(self) -> Table:
        if self._parent is None:
            raise InvalidRequestError('This constraint is not bound to a table.')

        return self._parent
    
    def _set_parent(self, parent: Table) -> None:
        self._parent = parent

class PrimaryKeyConstraint(Constraint):
    """Provide primary key constraint for columns."""
    def __init__(self, *columns: Column, name: Optional[str] = None):
        super().__init__(name)
        self._columns = ColumnCollection()
        for col in columns:
            self._columns.add(col)

    @property
    def columns(self) -> ReadOnlyColumnCollection:
        return self._columns.as_readonly()
    
    @property
    def c(self) -> ReadOnlyColumnCollection:
        return self._columns.as_readonly()
    
    def __repr__(self): 
        return f"PrimaryKeyConstraint(name={self._name}, ({', '.join([repr(c) for c in self.columns])}))"
