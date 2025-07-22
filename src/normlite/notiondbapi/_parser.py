"""Provide the parser for Notion API objects.

This module implements a simple Notion API objects parser for constructing an AST, which
is used by the visitor for cross-compilation of Notion JSON objects into tuples of elements.

Important:
    This module is **private** to the package :mod:`notiondbapi` and it does **not** expose
    its features outside.
"""

from normlite.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty


def parse_text_content(values: dict) -> str:
    if isinstance(values, list) and values:
        contents = [value.get("text", {}).get("content", "") for value in values]
        text_value = "".join(contents)
        return text_value
    return None

def parse_number(value: dict | int | float) -> str | int | float | None:
    if isinstance(value, dict):
        # number object value is a definition (dictionary), 
        # return format or None if format is not specified
        return value.get('format')
    elif isinstance(value, (int, float,)):
        # number object value is a numeric value, return it
        return value

def parse_property(name: str, payload: dict) -> NotionProperty:
    pid = payload.get("id")
    ptype = payload.get("type")
    value = None

    if ptype == "number":
        value = parse_number(payload.get("number"))
    elif ptype == "title":
        value = parse_text_content(payload.get("title", []))
    elif ptype == "rich_text":
        value = parse_text_content(payload.get("rich_text", []))

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
