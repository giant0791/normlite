"""Provide the Notion API Object model.

This module implements a simple Notion API object model for easy cross-compilation
of Notion JSON objects into tuples of elements.
Notion JSON obejcts like pages and databases are cross-compiled into a Python ``tuple`` object,
which represents a row as returned by the DBAPI methods :meth:`notiondbapi.dbapi2.Cursor.fetchone()` or
:meth:`notiondbapi.dbapi2.Cursor.fetchall()`.

.. list-table:: Mapping between Notion and SQL Objects
   :header-rows: 1
   :widths: 15 10 75
   :class: longtable

   * - Notion
     - SQL
     - Description
   * - Page
     - Row
     - A Notion page belonging to a database maps to an SQL row in a table. The Python implementation
       of the row is a ``tuple`` containing the key elements of the Notion page (see the class
       :class:`notiondbapi._model.NotionPage` for more details).
   * - Database
     - Table
     - A Notion database maps to an SQL table. The Python implementation
       of the table is a ``tuple`` containing the key elements of the Notion database 
       (see the class :class:`notiondbapi._model.NotionDatabase` for more details).

Important:
    This module is **private** to the package :mod:`notiondbapi`. It is intended to be used *internally*
    by the class :class:`notiondbapi.dbapi2.Cursor` to cross-compile Notion JSON objects. 
    **Do not import directly.**

"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, List, Optional


class AbstractNotionObject(ABC):
    """Abstract class for Notion API objects.

    This class starts the class hierarchy and it is used to represent the starting node
    of the **Abstract Syntax Tree** built by the parser.
    It implements the design pattern **Visitor**.
    """
    @abstractmethod
    def accept(self, visitor: NotionObjectVisitor) -> tuple:
        """Provide entry point for the visitor to compile.

        Args:
            visitor (NotionObjectVisitor): The compiler.

        Returns:
            tuple: A tuple containing a subset of values for the represented Notion object. 
        """
        pass


class NotionObjectVisitor(ABC):
    """Abstract class for a Notion API objects compiler.

    Subclasses of :class:`NotionObjectVisitor` implement the cross-compilation for each Notion object. 
    """
    @abstractmethod
    def visit_page(self, page: NotionPage) -> tuple:
        """Compile a Notion page object and extracts the relevant values.

        The extracted values in the tuple are:
        ``(<object>, <id>, <archived>, <in_trash>, <properties>)``

        - ``<object>``: ``str``, always ``"page"``
        - ``<id>``: ``str``, the unique identifier of the page
        - ``<archived>``: ``bool``, the archived status of the page 
        - ``<in_trash>``: ``bool``, whether the page is in Trash (can be ``None`` 
          if not returned by Notion) 
        
        The page properties ``<properties>`` are rendered as a flattened sequence of values:
        ``<key>, <pid>, <type>, <value>`` (see also :class:`NotionProperty`)

        - ``<key>``: ``str``, the property key
        - ``<pid>``: ``str``, the underlying identifier for the property
        - ``<type>``: ``str``, the type of the property. Currently supported types are: 
          ``"number"``, ``"rich_text"``, ``"title"``.

        Example:
            >>> properties = [
            >>>     NotionProperty('Price', 'BJXS', 'number', 2.5)
            >>>     NotionProperty('Description', '_Tc_', 'rich_text', 'A dark green leafy vegetable'),
            >>>     NotionProperty('Name', 'title', 'title', 'Tuscan kale')
            >>> ]
            >>> page = NotionPage(
            >>>     '59833787-2cf9-4fdf-8782-e53db20768a5',
            >>>     properties
            >>> )
            >>> visitor = ToRowVisitor()
            >>> row = page.accept(visitor)
            >>> row
            ('page', '59833787-2cf9-4fdf-8782-e53db20768a5', False, None,
            'Price', 'BJXS', 'number', 2.5,
            'Description', '_Tc_', 'rich_text', 'A dark green leafy vegetable',
            'Name', 'title', 'title', 'Tuscan kale',)


        Args:
            page (NotionPage): The Notion page object to be cross-compiled.

        Returns:
            tuple: A tuple containing the relevant values for this page.
        """
        raise NotImplementedError

    @abstractmethod
    def visit_database(self, db: NotionDatabase) -> tuple:
        """Compile a Notion database object and extracts the relevant values.

        The extracted values in the tuple are:
        ``(<object>, <id>, <title>, <archived>, <in_trash>, <properties>)``

        - ``<object>``: ``str``, always ``"database"``.
        - ``<id>``: ``str``, the unique identifier of the database.
        - ``<title>``: ``str``, th name of the database as it appears in Notion.
        - ``<archived>``: ``bool``, the archived status of the page. 
        - ``<in_trash>``: ``bool``, whether the page is in Trash (can be ``None``
          if not returned by Notion).
        - ``<properties>``: The schema of properties for the database as they appear in Notion. 
        
        The database properties ``<properties>`` are rendered as a flattened sequence of values:
        ``<key>, <pid>, <type>, <value>`` (see also :class:`NotionProperty`)

        - ``<key>``: ``str``, the property key.
        - ``<pid>``: ``str``, the underlying identifier for the property.
        - ``<type>``: ``str``, the type that controls the behavior of the property. 
          Currently supported types are: 
          ``"number"``, ``"rich_text"``, ``"title"``.

        Example:
            >>> properties = [
            >>>     NotionProperty('id', 'evWq', 'number', None)
            >>>     NotionProperty('name', 'title', 'title', None)
            >>>     NotionProperty('grade', 'V}lX', 'rich_text', None)
            >>> ]
            >>> page = NotionDatabase(
            >>>     '5bc1211ca-e3f1-4939-ae34-5260b16f627c', 
            >>>     'students',
            >>>     properties
            >>> )
            >>> visitor = ToRowVisitor()
            >>> row = page.accept(visitor)
            >>> row
            ('database', '59833787-2cf9-4fdf-8782-e53db20768a5', False, None,
            'id', 'evWq', 'number', None,
            'name', 'title', 'title', None,
            'grade', 'V}lX', 'rich_text', None,)


        Args:
            page (NotionDatabase): The Notion database object to be cross-compiled.

        Returns:
            tuple: A tuple containing the relevant values for this adatabase.
        """
        raise NotImplementedError

    @abstractmethod
    def visit_property(self, prop: NotionProperty) -> tuple:
        pass

class NotionProperty(AbstractNotionObject):
    """Provide Python object implementation for Notion property objects.

    This class implements both Notion page and database property objects.
    Its :meth:`NotionProperty.accept()` compiles the property elements into a tuple as follows::

        >>> property = NotionProperty('name', 'title', 'title')  # no value provided
        >>> visitor = ToRowVisitor()
        >>> row = property.accept(visitor)
        >>> row
        ('name', 'title', 'title', None)

    Args:
        name (str): The property name.
        id (str): The property id.
        type (str): The property type.
        value (Any, optional): The property value. Defaults to ``None`` (for example, database properties).
    
    """
    def __init__(
            self, 
            name: str, 
            id: str, 
            type: str, 
            value: Any = None):
        """

        """
        self.name = name
        """name (str): The property name."""
        self.id = id
        """id (str): The property id."""
        self.type = type
        """type (str): The property type."""
        self.value = value
        """value (Any): The property value. Defaults to ``None``
        when a property belongs to a database object or when the parent object is a 
        newly created object."""

    def accept(self, visitor: NotionObjectVisitor) -> tuple:
        """Compile the property object to a tuple.

        Example:
            >>> # property is a previously created NotionProperty object
            >>> visitor = ToRowVisitor()
            >>> row = visitor.accept(property)
            >>> row
            ('id', 'evWq', 'number', None,)

        Args:
            visitor (NotionObjectVisitor): The compiler.

        Returns:
            tuple: The tuple describing the property object.
        """
        return visitor.visit_property(self)


# === Notion Objects ===

class NotionPage(AbstractNotionObject):
    """Provide Python object implementation for Notion page objects.

    This class implements Notion page objects.
    Its :meth:`NotionPage.accept()` compiles the page elements into a tuple as follows::

        >>> properties = [
        >>>     NotionProperty('Price', 'BJXS', 'number', 2.5)
        >>>     NotionProperty('Name', 'title', 'title', 'Tuscan kale')
        >>> ]
        >>> page = NotionPage(
        >>>     '59833787-2cf9-4fdf-8782-e53db20768a5',     # page id as assigned by Notion
        >>>     False,                                      # archived flag is False for this page
        >>>     None,                                       # in_trash flag is not provided
        >>>     properties
        >>> )
        >>> visitor = ToRowVisitor()
        >>> row = page.accept(visitor)
        >>> row
        ('page','59833787-2cf9-4fdf-8782-e53db20768a5', False, None,
        'Price', 'BJXS', 'number', 2.5,
        'Name', 'title', 'title', 'Tuscan kale')

    Args:
        id (str): The page id.
        properties (List[NotionProperty]): The page properties
        archived (Optional[bool], optional): The ``"archived"`` flag for this page. Defaults to ``None``.
        in_trash (Optional[bool], optional): The ``"in_trash"`` flag for this page. Defaults to ``None``.
     """
    def __init__(
        self, id: str, 
        properties: List[NotionProperty],
        archived: Optional[bool] = None, 
        in_trash: Optional[bool] = None,
    ):
        
        self.id = id
        """The page id as assigned by Notion."""
        self.archived = archived
        """The ``"archived"`` flag for this page. Defaults to ``None` when 
        the page is a newly created object."""
        self.in_trash = in_trash
        """The ``"in_trash"`` flag for this page. Defaults to ``None` when
        the page is a newly created object."""
        self.properties = properties
        """The page ``"properties"`` object."""

    def accept(self, visitor: NotionObjectVisitor) -> tuple:
        """Compile a Notion page into a row.

        This method cross-compiles the page object into a row, which can be further processed by
        the class :class:`normlite.notiondbapi.Cursor`.
        The internal Python representation of a JSON Notion page object is a ``tuple``.
        
        Example:
            >>> # page is a previously created NotionPage object
            >>> visitor = ToRowVisitor()
            >>> row = visitor.accept(page)
            >>> row
            ('page','59833787-2cf9-4fdf-8782-e53db20768a5', False, None,
            'Price', 'BJXS', 'number', 2.5,
            'Name', 'title', 'title', 'Tuscan kale')

        Args:
            visitor (NotionObjectVisitor): The compiler.

        Returns:
            tuple: The cross-compiled row.
        """
        return visitor.visit_page(self)


class NotionDatabase(AbstractNotionObject):
    """Provide Python object implementation for Notion database objects.

    This class implements both Notion database objects.
    Its :meth:`NotionDatabase.accept()` compiles the page elements into a tuple as follows::

        >>> properties = [
        >>>     NotionProperty('id', 'evWq', 'number', None)
        >>>     NotionProperty('name', 'title', 'title', None)
        >>>     NotionProperty('grade', 'V}lX', 'rich_text', None)
        >>> ]
        >>> database = NotionDatabase(
        >>>     '59833787-2cf9-4fdf-8782-e53db20768a5',     # page id as assigned by Notion
        >>>     'students'                                  # the database name
        >>>     False,                                      # archived flag is False for this page
        >>>     None,                                       # in_trash flag is not provided
        >>>     properties
        >>> )
        >>> visitor = ToRowVisitor()
        >>> row = database.accept(visitor)
        >>> row
        ('page','59833787-2cf9-4fdf-8782-e53db20768a5', 'students', False, None,
        'id', 'evWq', 'number', None,
        'name', 'title', 'title', None,
        'grade', 'V}lX', 'rich_text', None,)

    Args:
        AbstractNotionObject (_type_): _description_
    """
    def __init__(
        self, 
        id: str, 
        title: str, 
        properties: List[NotionProperty],
        archived: Optional[bool] = None, 
        in_trash: Optional[bool] = None,
    ):

        self.id = id
        self.title = title
        self.archived = archived
        self.in_trash = in_trash
        self.properties = properties

    def accept(self, visitor: NotionObjectVisitor) -> tuple:
        return visitor.visit_database(self)


