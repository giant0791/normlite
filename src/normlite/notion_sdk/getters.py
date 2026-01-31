# notion_sdk/getters.py 
# Copyright (C) 2026 Gianmarco Antonini
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

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional


# ---------------------------------------------------------------------------
# Basic object metadata
# ---------------------------------------------------------------------------

def get_object_type(obj: Mapping[str, Any]) -> Optional[str]:
    """Return the Notion object type (page, database, block, etc.)."""
    return obj.get("object")


def get_object_id(obj: Mapping[str, Any]) -> Optional[str]:
    """Return the object's ID."""
    return obj.get("id")


def get_created_time(obj: Mapping[str, Any]) -> Optional[str]:
    return obj.get("created_time")


def get_last_edited_time(obj: Mapping[str, Any]) -> Optional[str]:
    return obj.get("last_edited_time")


def get_created_by_id(obj: Mapping[str, Any]) -> Optional[str]:
    created_by = obj.get("created_by")
    if isinstance(created_by, Mapping):
        return created_by.get("id")
    return None


def get_last_edited_by_id(obj: Mapping[str, Any]) -> Optional[str]:
    edited_by = obj.get("last_edited_by")
    if isinstance(edited_by, Mapping):
        return edited_by.get("id")
    return None


# ---------------------------------------------------------------------------
# Parent relationships
# ---------------------------------------------------------------------------

def get_parent(obj: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    return obj.get("parent")


def get_parent_type(obj: Mapping[str, Any]) -> Optional[str]:
    parent = get_parent(obj)
    if isinstance(parent, Mapping):
        return parent.get("type")
    return None


def get_parent_id(obj: Mapping[str, Any]) -> Optional[str]:
    parent = get_parent(obj)
    if isinstance(parent, Mapping):
        parent_type = parent.get("type")
        if parent_type:
            return parent.get(parent_type)
    return None


# ---------------------------------------------------------------------------
# Rich text helpers
# ---------------------------------------------------------------------------

def rich_text_to_plain_text(rich_text: Iterable[Mapping[str, Any]]) -> str:
    """
    Normalize a Notion rich-text array into plain text by concatenating
    authoritative content fields.
    """
    parts: list[str] = []

    for rt in rich_text:
        # 1. Primary: text.content (most stable)
        text = rt.get("text")
        if isinstance(text, Mapping):
            content = text.get("content")
            if isinstance(content, str):
                parts.append(content)
                continue

        # 2. Equation
        equation = rt.get("equation")
        if isinstance(equation, Mapping):
            expr = equation.get("expression")
            if isinstance(expr, str):
                parts.append(expr)
                continue

        # 3. Fallback: plain_text
        plain = rt.get("plain_text")
        if isinstance(plain, str):
            parts.append(plain)

    return "".join(parts)

def get_rich_text_annotations(rt: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    return rt.get("annotations")

# ---------------------------------------------------------------------------
# Title getters
# ---------------------------------------------------------------------------

def get_title_rich_text(obj: Mapping[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """
    Return the title rich-text array from a database or page object.
    """
    object_type = get_object_type(obj)

    if object_type == "database":
        return obj.get("title")

    if object_type == "page":
        properties = obj.get("properties")
        if not isinstance(properties, Mapping):
            return None

        for prop in properties.values():
            if prop.get("type") == "title":
                return prop.get("title")

    return None


def get_title(obj: Mapping[str, Any]) -> Optional[str]:
    """Return the plain-text title of a database or page."""
    rich_text = get_title_rich_text(obj)
    if not rich_text:
        return None
    return rich_text_to_plain_text(rich_text)


# ---------------------------------------------------------------------------
# Properties (pages & databases)
# ---------------------------------------------------------------------------

def get_properties(obj: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """Return the properties mapping (pages only)."""
    props = obj.get("properties")
    if isinstance(props, Mapping):
        return props
    return None


def get_property(obj: Mapping[str, Any], name: str) -> Optional[Mapping[str, Any]]:
    """Return a property definition/value by name."""
    props = get_properties(obj)
    if not props:
        return None
    return props.get(name)


def get_property_type(prop: Mapping[str, Any]) -> Optional[str]:
    return prop.get("type")


# ---------------------------------------------------------------------------
# Common property value getters
# ---------------------------------------------------------------------------

def get_title_property_value(prop: Mapping[str, Any]) -> Optional[str]:
    if prop.get("type") != "title":
        return None
    return rich_text_to_plain_text(prop.get("title", []))


def get_rich_text_property_value(prop: Mapping[str, Any]) -> Optional[str]:
    if prop.get("type") != "rich_text":
        return None
    return rich_text_to_plain_text(prop.get("rich_text", []))


def get_number_property_value(prop: Mapping[str, Any]) -> Optional[float]:
    if prop.get("type") != "number":
        return None
    return prop.get("number")


def get_checkbox_property_value(prop: Mapping[str, Any]) -> Optional[bool]:
    if prop.get("type") != "checkbox":
        return None
    return prop.get("checkbox")


def get_select_property_value(prop: Mapping[str, Any]) -> Optional[str]:
    if prop.get("type") != "select":
        return None
    select = prop.get("select")
    if isinstance(select, Mapping):
        return select.get("name")
    return None


def get_multi_select_property_values(prop: Mapping[str, Any]) -> Optional[List[str]]:
    if prop.get("type") != "multi_select":
        return None
    values = prop.get("multi_select")
    if not isinstance(values, list):
        return None
    return [v.get("name") for v in values if "name" in v]


def get_date_property_value(prop: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    if prop.get("type") != "date":
        return None
    return prop.get("date")


def get_url_property_value(prop: Mapping[str, Any]) -> Optional[str]:
    if prop.get("type") != "url":
        return None
    return prop.get("url")


def get_email_property_value(prop: Mapping[str, Any]) -> Optional[str]:
    if prop.get("type") != "email":
        return None
    return prop.get("email")


def get_phone_number_property_value(prop: Mapping[str, Any]) -> Optional[str]:
    if prop.get("type") != "phone_number":
        return None
    return prop.get("phone_number")


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def get_property_plain_text(obj: Mapping[str, Any], name: str) -> Optional[str]:
    """
    Best-effort plain-text getter for common textual properties.
    """
    prop = get_property(obj, name)
    if not prop:
        return None

    prop_type = prop.get("type")

    if prop_type == "title":
        return get_title_property_value(prop)

    if prop_type == "rich_text":
        return get_rich_text_property_value(prop)

    if prop_type == "select":
        return get_select_property_value(prop)

    return None
