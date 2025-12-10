# notiondbapi/_parser.py
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

"""Provide the parser for Notion API objects.

This module implements a simple Notion API objects parser for constructing an AST, which
is used by the compilers for cross-compilation of Notion JSON objects into tuples of elements.

.. seealso::

    ``normlite``'s :py:mod:`normlite.notiondbapi.compiler` module
        Documentation of compilers for Notion objects.

    `Notion API Reference Page object <https://developers.notion.com/reference/page>`
        Documentation of Notion page objects.

.. versionchanged:: 0.8.0
    Completely redesigned and refactor, this module now provides new parsing capabilities more in line
    with the DBAPI 2.0 specification.

Important:
    This module is **private** to the package :mod:`notiondbapi` and it does **not** expose
    its features outside.
"""

from normlite.exceptions import NormliteError
from normlite.notiondbapi._model import NotionDatabase, NotionProperty, NotionPage
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode

def parse_page_property(name: str, payload: dict) -> NotionProperty:
    """Generate a Notion property object from the corresponding JSON object
  
    Args:
        name (str): The property name (key).
        payload (dict): The JSON object representing the property.

    Returns:
        NotionProperty: The parsed Notion property object.
    """
    pid = payload.get("id")
    ptype = payload.get("type")             # ptype is None for new and updated pages
    property_type = ptype
    value = None

    if not ptype:
        # the page to be parsed is the return object of either create pages (POST) or update pages (PATCH)
        return NotionProperty(
            is_page_property=True,
            name=name, 
            id=pid,
            type=DBAPITypeCode.PROPERTY_ID,         # inferred because the property belongs to a created or updated page
            arg=None,
            value=None
        )

    value = payload.get(ptype)
    if ptype == 'number':
        if isinstance(value, int):
            property_type = 'number'
        elif isinstance(value, float):
            property_type = 'number_with_commas'

    elif ptype == 'title' or ptype == 'rich_text':
        value = [{'text': tv.get('text')} for tv in value]

    parg = value if isinstance(value, dict) else None     
    return NotionProperty(
        is_page_property=True,
        name=name, 
        id=pid, 
        type=property_type, 
        arg=parg, 
        value={ptype: value}
    )

def parse_database_property(name: str, payload: dict) -> NotionProperty:
    pid = payload.get("id")
    ptype = payload.get("type")             # ptype is None for new and updated pages

    if not ptype:
        raise NormliteError(f'Internal error: expected key "type" in property: "{name}".')

    value = payload.get(ptype)
    property_type = ptype                   # property_type is computed for type "number"

    if ptype == 'number':
        has_format = value.get('format', None)
        property_type = has_format if has_format else ptype

    parg = value if isinstance(value, dict) else None     

    return NotionProperty(
        is_page_property=False,
        name=name,  
        id=pid, 
        type=property_type, 
        arg=parg, 
        value=None
    )


def parse_page(payload: dict) -> NotionPage:
    id = payload["id"]
    archived = payload.get('archived')
    in_trash = payload.get('in_trash')
    properties = [
        parse_page_property(name, pdata) for name, pdata in payload.get("properties", {}).items()
    ]
    return NotionPage(id=id, properties=properties, archived=archived, in_trash=in_trash)
    
def parse_database(payload: dict) -> NotionDatabase:
    id = payload.get('id')
    title = payload.get('title')
    archived = payload.get('archived')
    in_trash = payload.get('in_trash')
    properties = [
        parse_database_property(name, pdata) for name, pdata in payload.get("properties", {}).items()
    ]

    return NotionDatabase(id, '', {'title': title}, properties, archived, in_trash)