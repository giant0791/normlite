# tests/fixtures/schema.py
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

NOTION_FIXTURE_SCHEMA: dict = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["version", "pages", "databases", "pages_in_databases"],
    "additionalProperties": False,

    "properties": {
        "version": {
            "type": "integer",
            "const": 1,
        },

        "pages": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "title", "parent"],
                "additionalProperties": False,
                "properties": {
                    "id": {
                        "type": "string",
                        "pattern": "^[a-zA-Z_][a-zA-Z0-9_]*$",
                    },
                    "title": {
                        "type": "string",
                        "minLength": 1,
                    },
                    "parent": {
                        "anyOf": [
                            {"type": "null"},
                            {"type": "string"},
                        ]
                    },
                },
            },
        },

        "databases": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "parent", "title", "properties"],
                "additionalProperties": False,
                "properties": {
                    "id": {
                        "type": "string",
                        "pattern": "^[a-zA-Z_][a-zA-Z0-9_]*$",
                    },
                    "parent": {
                        "type": "string",
                    },
                    "title": {
                        "type": "string",
                        "minLength": 1,
                    },
                    "properties": {
                        "type": "object",
                        "minProperties": 1,
                        "additionalProperties": {
                            "type": "string",
                            "enum": ["title", "rich_text"],
                        },
                    },
                },
            },
        },

        "pages_in_databases": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["database", "rows"],
                "additionalProperties": False,
                "properties": {
                    "database": {
                        "type": "string",
                    },
                    "rows": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "minProperties": 1,
                        },
                    },
                },
            },
        },
    },
}
