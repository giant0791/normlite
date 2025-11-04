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
is used by the visitor for cross-compilation of Notion JSON objects into tuples of elements.

Important:
    This module is **private** to the package :mod:`notiondbapi` and it does **not** expose
    its features outside.
"""

import pdb
from typing import Tuple, Union
from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty


def parse_text_content(value_or_list: Union[list[dict], dict]) -> str:
    text_value = None
    if value_or_list and isinstance(value_or_list, list):
        contents = [value.get("text", {}).get("content", "") for value in value_or_list]
        text_value = "".join(contents)
    elif value_or_list and isinstance(value_or_list, dict):
        text_value = value_or_list.get("text", {}).get("content", "")

    return text_value

def parse_number(number: Union[dict, int, float]) -> Tuple[str, Union[int, float, None]]:
    """Parse a number object.

    This method parses the following number objects:

        >>> # empty number
        >>> number = None
        >>> ptype, value = parse_number(number)
        >>> ptype, value
        ('number', None)

        >>> # number with format spec
        >>> number = {"format": "dollar"}
        >>> ptype, value = parse_number(number)
        >>> ptype, value
        ('number.dollar', None)

        >>> # number with numeric value (int or float)
        >>> number = 2
        >>> ptype, value = parse_number(number)
        >>> ptype, value
        ('number', 2)

    Args:
        number (Union[dict, int, float]): Either a dictionary containing a number spec or a numeric value.

    Returns:
        Optional[Tuple[str, Union[int, float, None]]]: The pair (type, value) as tuple.
    
    """
    if isinstance(number, dict):
        # number contains the either the "format" property or {}
        format = number.get('format')
        type = f'number.{format}' if format else 'number'
        value = None
    else:
        type = 'number'
        value = number

    # numer contains a numeric value
    return (type, value)
        
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
        # number is {"formart": ...} for databases retrieved
        ptype, value = parse_number(number)

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
