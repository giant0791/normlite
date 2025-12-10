# notiondbapi/compiler.py
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

"""Provide the compiler implementation for cross-compiling Notion JSON objects to tuples of elements.

This module provides two different compilers to accomplish the following cross-compilation tasks from
:class:`normlite.notiondbapi._model.NotionPage` or :class:`normlite.notiondbapi._model.NotionDatabase` objects, which have been constructed by parsing the 
JSON object returned by the Notion API:
    
    1. Create a DBAPI row: :class:`normlite.notiondbapi.compiler.RowCompiler`.

    2. Create a DBAPI description: :class:`normlite.notiondbapi.compiler.DescriptionCompiler`.

:class:`RowCompiler` constructs the row objects returned by :meth:`normlite.notiondbapi.dbapi2.Cursor.fetchone` and 
:meth:`normlite.notiondbapi.dbapi2.Cursor.fetchall`. A row object is a Python sequence of tuples.

    * the tuples contain 2 values: column name and id for pages that have been created or updated; 
    column name and value for pages that have been retrieved. Values are returned as JSON objects (i.e., Python dictionary).
    
    * The tuples contain 3 values: column name, type and id for databases that have been created, updated or retrieved. 

:class:`DescriptionCompiler` constructs the cursor description object returned by the read-only attribute
:attr:`normlite.notiondbapi.dbapi2.Cursor.description`. A description object is a sequence of Python 7-valued tuples containing metadata
used by the :class:`normlite.cursor.CursorResult` to construct :class:`normlite.cursor.Row` objects for a more pythonic hanlding
of the query results. 

.. admonition:: Examples
    :collapsible: open

    .. rubric:: Example 1 - Compiling a Notion page retrieved by the pages retrieve endpoint

    .. code-block:: python

        # simplified page object for brevity reasons
        def page_retrieved() -> dict:
            return {
                "object": "page",
                "id": "59833787-2cf9-4fdf-8782-e53db20768a5",
                "created_time": "2022-03-01T19:05:00.000Z",
                "last_edited_time": "2022-07-06T20:25:00.000Z",
                "parent": {
                    "type": "database_id",
                    "database_id": "d9824bdc-8445-4327-be8b-5b47500af6ce"
                },
                "archived": False,
                "properties": {
                    "Price": {
                        "id": "BJXS",
                        "type": "number",
                        "number": 2.5
                    },
                    "Description": {
                        "id": "_Tc_",
                        "type": "rich_text",
                        "rich_text": [
                            {
                                "type": "text",
                                "text": {
                                    "content": "A dark leafy green vegetable",
                                    "link": None
                                },
                            }
                        ]
                    },
                    "Name": {
                        "id": "title",
                        "type": "title",
                        "title": [
                            {
                                "type": "text",
                                "text": {
                                    "content": "Tuscan kale",
                                    "link": None
                                },
                            }
                        ]
                    },
                },
                "url": r"https://www.notion.so/Tuscan-kale-598337872cf94fdf8782e53db20768a5",
                "public_url": None
        }

        page_object: NotionPage = parse_page(page_retrieved)
        row = page_object.compile(RowCompiler())
        for c in row:
            print(c)

        # result:
        # (<SpecialColumns.NO_ID: '_no_id'>, '59833787-2cf9-4fdf-8782-e53db20768a5')
        # (<SpecialColumns.NO_ARCHIVED: '_no_archived'>, False)
        # (<SpecialColumns.NO_IN_TRASH: '_no_in_trash'>, None)
        # ('Price', {'number': 2.5})
        # ('Description', {'rich_text': [{'text': {'content': 'A dark green leafy vegetable', 'link': None}}]}) 
        # ('Name', {'title': [{'text': {'content': 'Tuscan kale', 'link': None}}]})


"""

import pdb
from typing import Optional, Sequence
from normlite._constants import SpecialColumns
from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty, NotionObjectCompiler
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.exceptions import CompileError

_typecode_mapper: dict[str, DBAPITypeCode] = {

    'title': DBAPITypeCode.TITLE,
    'rich_text': DBAPITypeCode.RICH_TEXT,
    'number': DBAPITypeCode.NUMBER,
    'number_with_commas': DBAPITypeCode.NUMBER_WITH_COMMAS,
    'dollar': DBAPITypeCode.NUMBER_DOLLAR,
    'checkbox': DBAPITypeCode.CHECKBOX,
    'date': DBAPITypeCode.DATE,
    'property_id': DBAPITypeCode.PROPERTY_ID
}
"""Mapping to translate Notion types into DBAPI type codes.

    .. versionadded: 0.8.0
        For module internal use only.
"""

class RowCompiler(NotionObjectCompiler):
    """Cross-compiles Notion objects into a DBAPI row.
    
    .. versionchanged:: 0.8.0
        Renamed to :class:`RowCompiler` to reflect the compiler nature, it replaces the old row visitor class. 
        Completely redesigned, it now compiles pages into a sequence of 2-value tuples and databases into a sequence of 3-value tuples.

    .. versionchanged:: 0.4.0
        The overall cross-compilation implemented in :class:`ToRowVisitor` has been refactored to be fully DBAPI 2.0 compliant.
    
    """
    def visit_page(self, page: NotionPage) -> Sequence[tuple]:
        """Compiles a Notion page object into a sequence of 2-value tuples.
        
        .. versionadded:: 0.8.0
            New redesigned visit method.
        """
        visited_page = list()
        # (column_name, column_arg, column_value)
        special_columns = [
            (SpecialColumns.NO_ID, page.id,),
            (SpecialColumns.NO_ARCHIVED, page.archived,),
            (SpecialColumns.NO_IN_TRASH, page.in_trash,),
        ]

        visited_page.extend(special_columns)
        properties = [p.compile(self) for p in page.properties]
        visited_page.extend(properties)

        return visited_page

    def visit_database(self, database: NotionDatabase) -> Sequence[tuple]:
        visited_database = list()
        special_columns = [
            (SpecialColumns.NO_ID, None, database.id,),
            (SpecialColumns.NO_ARCHIVED, None, database.archived,),
            (SpecialColumns.NO_TITLE, None, database.title)
        ]
        visited_database.extend(special_columns)
        properties = [p.compile(self) for p in database.properties]
        visited_database.extend(properties)

        return visited_database

    def visit_property(self, prop: NotionProperty) -> tuple:
        if prop._is_page_property:
            if prop.is_page_created_or_updated:
                return (prop.name, prop.id,)
            else:
                return (prop.name, prop.value,)
        else:
            return (prop.name, prop.id, prop.value,)
    
class DescriptionCompiler(NotionObjectCompiler):
    """Cross-compile Notion objects into a DBAPI compliant description.

    The description for created or updated pages returns the special columns only. 
    This is because the properties of such pages contain "id" keys only and thus the type
    cannot be determined. The type of the special columns is known.
    
    .. versionadded: 0.8.0
        This class is a more powerful version of the old visitor implementation.
    """
    def _add_not_used_seq(self, col_desc: tuple, count: int = 5) -> tuple:
        """Helper to fill in the missing elements with ``None`` values."""
        for _ in range(count):
            col_desc += (None,)

        return col_desc

    def visit_page(self, page: NotionPage) -> Sequence[tuple]:
        return [
            self._add_not_used_seq((SpecialColumns.NO_ID, DBAPITypeCode.ID,)), 
            self._add_not_used_seq((SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX,)),
            self._add_not_used_seq((SpecialColumns.NO_IN_TRASH, DBAPITypeCode.CHECKBOX,)),
            *[prop.compile(self) for prop in page.properties]             
        ]

    def visit_database(self, database: NotionDatabase) -> Sequence[tuple]:
        return [
            self._add_not_used_seq((SpecialColumns.NO_ID, DBAPITypeCode.ID,)),
            self._add_not_used_seq((SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX,)),
            self._add_not_used_seq((SpecialColumns.NO_TITLE, DBAPITypeCode.TITLE,)),
            *[prop.compile(self) for prop in database.properties]
        ]

    def visit_property(self, prop: NotionProperty) -> Optional[tuple]:
        try:
            return self._add_not_used_seq((prop.name, _typecode_mapper[prop.type],))
        except KeyError:
            raise TypeError(f'Unexpected or unsupported property type: "{prop.type}"')


