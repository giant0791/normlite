normlite.sql.schema
===================

.. py:module:: normlite.sql.schema

.. autoapi-nested-parse::

   Provide the key abstractions for database metadata.
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

   .. note::

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





Module Contents
---------------

.. py:class:: Column(name: str, type_: normlite.sql.type_api.TypeEngine, primary_key: bool = False)

   A single table column specifying the type and its constraints.

   The :class:`Column` objects model Notion properties. You define a column by assigning it a name
   and a type (for available types, refer to :mod:`normlite.sql.type_api.py`).
   If a column is intended to act as a primary key in your database, then set the
   argument ``primary_key`` to true (refer to :class:`PrimaryKeyConstraint` for more details).

   .. rubric:: Example

   >>> col = Column('name', String(is_title=True))
   >>> print(repr(col))
   ... Column('name', String(is_title=True), table=None)

   .. note::

      Currently, only the primary key constraint is supported.
      In the future, the ``nullable`` constraint might be supported too.

   .. versionadded:: 0.7.0



   .. py:attribute:: name

      The column name. This must be unique within the same table.


   .. py:attribute:: type_

      The column type as a concrete subclass of :class:`normlite.sql.type_api.TypeEnging`.


   .. py:attribute:: primary_key
      :value: False


      Whether this column is a primary key or not.


   .. py:attribute:: parent
      :type:  Table
      :value: None


      The table this column belongs to.
      Initially ``None``, it is set when this column is appended to its table.
      See :meth:`Table.append_column()` for more details.


   .. py:method:: _set_parent(parent: Table) -> None

      Set the table this columns belongs to.
      This method is not intended to be called by users of the :class:`Column`,
      because it is automatically set by the :class:`Table`.



   .. py:method:: __repr__()


.. py:class:: Table(name: str, *columns: Column, dialect=None)

   A database table.

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

   .. note::

      Since Notion automatically generates several special properties to its pages,
      the :class:`Table` automatically adds the following two columns:
      
          >>> print(repr(students))
          >>> Table('students',
          ...     Column('student_id', Integer, table=<students>, primary_key=True),
          ...     Column('name', String(is_title=True), table=<students>),
          ...     Column('grade', String(), table=<students>),
          ...     Column('since', Date, table=<students>),
          ...     Column('_no_id', ObjectId, table=None, primary_key=True),
          ...     Column('_no_archived', ArchivalFlag, table=None))

   .. versionadded: 0.7.0



   .. py:attribute:: name

      Table name.


   .. py:attribute:: _columns
      :type:  ColumnCollection

      The underlying column collections for this table's column.

      .. seealso::

          :class:`ColumnCollection`


   .. py:attribute:: _constraints
      :type:  Set[Constraint]

      The set of constraints associated to this table's columns.


   .. py:attribute:: _primary_key
      :type:  PrimaryKeyConstraint
      :value: None


      The primary key constraint

      .. seealso::

          :class:`PrimaryKeyConstraint`

          :attr:`Table.primary_key`


   .. py:property:: columns
      :type: ReadOnlyColumnCollection


      Accessor for this table's columns.

      It returns a ready-only copy column collection.


   .. py:property:: c
      :type: ReadOnlyColumnCollection


      Short form synonim for :attr:`columns`.


   .. py:property:: primary_key
      :type: PrimaryKeyConstraint


      Return the primary key constraint object associated to this table.

      .. rubric:: Example

      >>> print(repr(stundents.primary_key))
      >>> PrimaryKeyConstraint(name=None,
      ...     (Column('student_id', Integer, table=<students>, primary_key=True),
      ...     (Column('_no_id', ObjectId, table=None, primary_key=True)))


   .. py:method:: add_constraint(constraint: Constraint) -> None

      Add a constraint to this table.



   .. py:method:: append_column(column: Column)

      Append a column to this table.



   .. py:method:: insert() -> normlite.sql.dml.Insert

      Generate a new SQL insert statement for this table.



   .. py:method:: _ensure_implicit_columns()


   .. py:method:: _create_pk_constraint() -> None


   .. py:method:: __repr__() -> str


.. py:class:: ColumnCollection(columns: Optional[Iterable[Tuple[str, Column]]] = None)

   Provide a container to efficiently store and conveniently access a table's columns.

   This class provides a table's column accessor with dictionary-like and indexed interface.
   It allows unique column names only.
   Column collection objects provide an iterator interface (incl. :meth:`__len__()`) as well as
   a method to test column existence in the collection (see :meth:`__contains__()`).

   .. versionadded:: 0.7.0



   .. py:attribute:: __slots__
      :value: ('_collection', '_index', '_colset')



   .. py:attribute:: _collection
      :type:  List[Tuple[str, Column]]


   .. py:attribute:: _index
      :type:  Dict[Union[None, str, int], Tuple[str, Column]]


   .. py:attribute:: _colset
      :type:  Set[Column]


   .. py:method:: add(column: Column) -> None

      Add a new column to the collection.

      :param column: The column to be added.
      :type column: Column

      :raises DuplicateColumnError: If a column with the same name as the column to be added
          already exists in the collection.



   .. py:method:: _populate_separate_keys(iter_: Iterable[Tuple[str, Column]]) -> None


   .. py:method:: __contains__(key: str) -> bool


   .. py:method:: __len__() -> int


   .. py:method:: len(usr_def_only: Optional[bool] = True) -> int


   .. py:method:: __iter__() -> Iterator[Column]


   .. py:method:: __getattr__(key: str) -> Column


   .. py:method:: __str__() -> str


   .. py:method:: __getitem__(key: Union[str, int]) -> Column
                  __getitem__(key: slice) -> ReadOnlyColumnCollection

      Indexed accessor to the columns in the collection.

      :param key: The index key.
      :type key: Union[str, int, slice]

      :raises IndexError: If the index key provided is incorrect.

      :returns: A single column if an int index or a read-only collection for a slice index.
      :rtype: Union[Column, ReadOnlyColumnCollection]



   .. py:method:: __setitem__(key: str, value: Any) -> NoReturn
      :abstractmethod:



   .. py:method:: __delitem__(key: str) -> NoReturn
      :abstractmethod:



   .. py:method:: __setattr__(key: str, obj: Any) -> NoReturn
      :abstractmethod:



   .. py:method:: clear() -> NoReturn
      :abstractmethod:



   .. py:method:: remove(column: Any) -> NoReturn
      :abstractmethod:



   .. py:method:: update(iter_: Any) -> NoReturn
      :abstractmethod:



   .. py:method:: as_readonly() -> ReadOnlyColumnCollection

      Return a read-only collection.



.. py:class:: ReadOnlyCollectionMixin

   Mixin for read-only collections.


   .. py:attribute:: __slots__
      :value: ()



   .. py:method:: _readonly() -> NoReturn


   .. py:method:: __delitem__(key: Any) -> NoReturn


   .. py:method:: __setitem__(key: Any, value: Any) -> NoReturn


   .. py:method:: __setattr__(key: Any, value: Any) -> NoReturn


.. py:class:: ReadOnlyColumnCollection(collection: ColumnCollection)

   Bases: :py:obj:`ReadOnlyCollectionMixin`, :py:obj:`ColumnCollection`


   Read-only version of a column collection.


.. py:class:: Constraint(name: Optional[str] = None)

   Ancestor class for all column constraints.


   .. py:attribute:: _name
      :value: None



   .. py:attribute:: _parent
      :type:  Table
      :value: None



   .. py:property:: table
      :type: Table



   .. py:method:: _set_parent(parent: Table) -> None


.. py:class:: PrimaryKeyConstraint(*columns: Column, name: Optional[str] = None)

   Bases: :py:obj:`Constraint`


   Provide primary key constraint for columns.


   .. py:attribute:: _columns


   .. py:property:: columns
      :type: ReadOnlyColumnCollection



   .. py:property:: c
      :type: ReadOnlyColumnCollection



   .. py:method:: __repr__()


