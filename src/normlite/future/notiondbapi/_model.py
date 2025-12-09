from __future__ import annotations
from abc import ABC
from typing import Any, Optional, Protocol, Sequence, Union

from normlite.exceptions import UnsupportedCompilationError

class NotionObject(ABC):
    __visit_name__: str

    def _compiler_dispatch(self, compiler: NotionObjectCompiler) -> Union[Sequence[tuple], tuple]:
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
        return compiler.process(self)
    

class NotionProperty(NotionObject):
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


        #
        # TODO: Consider to add a reference to the parent object
        # This can help to determine whether the property belongs to a page or a database
        self._is_page_property = is_page_property
        
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
        return all([p.is_page_created_or_updated for p in self.properties])

class NotionDatabase(NotionObject):
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

class NotionObjectCompiler(Protocol):
    def visit_page(self, page: NotionPage) -> Sequence[tuple]:
        ...

    def visit_database(self, database: NotionDatabase) -> Sequence[tuple]:
        ...

    def visit_property(self, prop: NotionProperty) -> tuple:
        ...

    def process(self, object_: NotionObject) -> Union[Sequence[tuple], tuple]:
        return object_._compiler_dispatch(self)






