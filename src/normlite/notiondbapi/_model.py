# notiondbapi/_model.py
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
from abc import ABC
from typing import Any, Optional, Protocol, Sequence, Union

from normlite.exceptions import UnsupportedCompilationError

class NotionObject(ABC):
    """Base for all Notion model objects.

    This base class implements the compiler dispatch logic for visit methods.
    
    .. versionadded:: 0.8.0
        It replaces old :class:`AbstractNotionObject` class.
    
    """
    __visit_name__: str
    """Name of the compiler'S visit method to be dispatched when starting compilation."""

    def _compiler_dispatch(self, compiler: NotionObjectCompiler) -> Union[Sequence[tuple], tuple]:
        """Dispatch call to Notion object compiler's visit method.

        Args:
            compiler (NotionObjectCompiler): The compiler to dispatch the call to the visit method.

        Raises:
            UnsupportedCompilationError: If the :attr:`__visit_name__` is not found

        Returns:
            Union[Sequence[tuple], tuple]: A sequence of tuples for pages and databases, a simple tuple for properties.
        """

        visit_name = getattr(self, '__visit_name__', None)

        if not visit_name:
            raise UnsupportedCompilationError(
                f"{self.__class__.__name__} is missing '__visit_name__' attribute."
            )

        visit_fn = getattr(compiler, f"visit_{visit_name}", None)
        if not callable(visit_fn):
            raise UnsupportedCompilationError(
                f"{compiler.__class__.__name__} has no method visit_{visit_name}() "
                f"for {self.__class__.__name__}"
            )

        return visit_fn(self)
    
    def compile(self, compiler: NotionObjectCompiler) -> Union[Sequence[tuple], tuple]:
        """Notion object's entry-point to start the compilation.

        Args:
            compiler (NotionObjectCompiler): The compiler to be used in the compilation.

        Returns:
            Union[Sequence[tuple], tuple]: A sequence of tuples for pages and databases, a simple tuple for properties.
        """
        return compiler.process(self)
    

class NotionProperty(NotionObject):
    """Provide Python object implementation for Notion property objects.

    This class implements both Notion page and database property objects.
    The subclasses of :class:`NotionObjectCompiler` can use the :attr:`is_page_created_or_updated` to do conditional
    compilation based on whether a page has been created or updated.

    .. versionchanged:: 0.8.0
        Complete refactoring to implement the new :meth:`NotionObject.compile` API for compilation.
        This class provides a new helper property :attr:`is_page_created_or_updated` to produce different compilation results.
    """
    __visit_name__ = 'property'

    def __init__(
            self,
            is_page_property: bool, 
            name: str, 
            id: str, 
            type: str, 
            arg: dict,
            value: Any = None
    ):


        self._is_page_property = is_page_property
        """``True`` if this property belong to a page, ``False`` if it belongs to a database.
        
        .. versionadded:: 0.8.0
        """
        
        self.name = name
        """The property name."""

        self.id = id
        """The property id."""
        
        self.type = type
        """The property type."""

        self.arg = arg
        """The arguments to be provided to construct the type as :class:`normlite.sql.type_api.TypeEngine` subclasses.
        
        .. versionadded: 0.8.0
            The :attr:`arg` is needed for table reflection as the rows returned by 
            the :class:`normlite.cursor.CursorResult` methods must provide the reflected columns.
        """
        
        self.value = value
        """The property value. Defaults to ``None``
        when a property belongs to a database object or when the parent object is a 
        newly created object.
        """

    @property
    def is_page_created_or_updated(self) -> bool:
        """``True`` if this property object belongs to a page returned by a pages create or update endpoints."""
        return self._is_page_property and (self.type is None and self.arg is None and self.value is None)

    def __repr__(self) -> str:
        return f'Property(name="{self.name}", is_page_property={self._is_page_property}, id="{self.id}", type="{self.type}", value={self.value})'

class NotionPage(NotionObject):
    __visit_name__ = 'page'

    def __init__(
        self,
        id: str, 
        properties: Sequence[NotionProperty],
        archived: Optional[bool] = None, 
        in_trash: Optional[bool] = None,
    ):
        self.id = id
        """The page id as assigned by Notion."""

        self.archived = archived
        """The ``"archived"`` flag for this page. Defaults to ``None`` when 
        the page is a newly created object."""
        
        self.in_trash = in_trash
        """The ``"in_trash"`` flag for this page. Defaults to ``None`` when
        the page is a newly created object."""
        
        self.properties = properties
        """The page ``"properties"`` object."""

    @property
    def is_page_created_or_updated(self) -> bool:
        """``True`` if this page object was returned by a pages create or update endpoint.
        
        The page objects returned by the Notion API have different contents depending of the endpoint.

            * ``POST /pages/`` and ``PATCH /pages/<page_id>`` return properties with the "id" key only.

            * ``GET /pages/<page_id>`` returns properties with the "id" and "type" keys.

        .. versionadded:: 0.8.0 
        """
        return all([p.is_page_created_or_updated for p in self.properties])

class NotionDatabase(NotionObject):
    """Provide Python object implementation for Notion database objects.

    This class implements Notion database objects.

    .. versionchanged:: 0.8.0
        Complete refactoring to implement the new :meth:`NotionObject.compile` API for compilation.
        The :class:`NotionDatabase` class now stores the parent idententifier as an attribute.            
    """
    __visit_name__ = 'database'
    def __init__(
        self, 
        id: str, 
        parent_id: str,
        title: str, 
        properties: Sequence[NotionProperty],
        archived: Optional[bool] = None, 
        in_trash: Optional[bool] = None,
    ):

        self.id = id
        self.parent_id = parent_id
        self.title = title
        self.archived = archived
        self.in_trash = in_trash
        self.properties = properties

    def __repr__(self):
        prop_str = ", ".join([prop for prop in self.properties])
        db_str = f'Database(id="{self.id}", title="{self.title}", archived={self.archived}, in_trash={self.in_trash}, properties={prop_str})'
        return db_str 


class NotionObjectCompiler(Protocol):
    """Base compiler for Notion objects

    This implements the new :meth:`NotionObject,_compiler_dispatch` API to visit the Notion objects.
    
    .. versionadded:: 0.8.0
        Complete refactoring, implements new dispatch API.
    """
    def visit_page(self, page: NotionPage) -> Sequence[tuple]:
        ...

    def visit_database(self, database: NotionDatabase) -> Sequence[tuple]:
        ...

    def visit_property(self, prop: NotionProperty) -> tuple:
        ...

    def process(self, object_: NotionObject) -> Union[Sequence[tuple], tuple]:
        return object_._compiler_dispatch(self)
