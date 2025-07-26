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

"""Provide the parser for Notion API objects.

This module implements a simple Notion API objects parser for constructing an AST, which
is used by the visitor for cross-compilation of Notion JSON objects into tuples of elements.

Important:
    This module is **private** to the package :mod:`notiondbapi` and it does **not** expose
    its features outside.
"""

import pdb
from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty


def parse_text_content(values: list) -> str:
    if values:
        contents = [value.get("text", {}).get("content", "") for value in values]
        text_value = "".join(contents)
        return text_value
    return None

def parse_number(value: dict | int | float) -> str | int | float | None:
    if isinstance(value, dict):
        # number object value is a definition (dictionary), 
        # return format or None if format is not specified
        return value.get('format')
    
    if isinstance(value, (int, float,)):
        # number object value is a numeric value, return it
        return value

def parse_property(name: str, payload: dict) -> NotionProperty:
    """Parse a JSON property object 

    This method parses a JSON property object and creates the corresponding Python node object.

    Args:
        name (str): The property name.
        payload (dict): A dictionary containing the JSON property object.

    Raises:
        TypeError: If an unexpected or unsupported property type is in the :obj:`payload`.

    Returns:
        NotionProperty: The Python node object corresponding to the Notion page or database property.
    """
    pid = payload.get("id")
    ptype = payload.get("type")             # pytype is None for updated pages
    value = None

    if ptype and not ptype in ['number', 'title', 'rich_text']:
        raise TypeError(f'Unexpected or unsupported property type: {ptype}')

    if ptype == "number":
        number = payload.get("number", {})

        # number is {} for database objects or pages returned from pages.create
        value = parse_number(number) if number else None

    elif ptype == "title":
        title = payload.get("title", [])

        # title is [] for database objects or pages returned from pages.create
        value = parse_text_content(title) if title else None

    elif ptype == "rich_text":
        rich_text = payload.get("rich_text", [])

        # rich_text is [] for database objects or pages returned from pages.create
        value = parse_text_content(rich_text) if rich_text else None

    return NotionProperty(name=name, id=pid, type=ptype, value=value)

def parse_page(payload: dict) -> NotionPage:
    id = payload["id"]
    archived = payload.get('archived')
    in_trash = payload.get('in_trash')
    properties = [
        parse_property(name, pdata) for name, pdata in payload.get("properties", {}).items()
    ]
    return NotionPage(id=id, properties=properties, archived=archived, in_trash=in_trash)

def parse_database(payload: dict) -> NotionDatabase:
    id = payload["id"]
    title = parse_text_content(payload.get("title", []))
    archived = payload.get('archived')
    in_trash = payload.get('in_trash')
    properties = [
        parse_property(name, pdata) for name, pdata in payload.get("properties", {}).items()
    ]
    return NotionDatabase(id=id, title=title, properties=properties, archived=archived, in_trash=in_trash)
