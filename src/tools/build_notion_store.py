# tools/build_notion_store.py 
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


#!/usr/bin/env python3
"""
Compile a declarative Notion fixture into a persisted store.

Usage:
    python build_notion_store.py \
        --fixture path/to/fixture.yaml \
        --output path/to/store.json
"""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from jsonschema import validate, ValidationError

from normlite.notion_sdk.client import InMemoryNotionClient, FileBasedNotionClient, NotionError

from tests.fixtures.schema import NOTION_FIXTURE_SCHEMA


# ----------------------------------------------------------------------
# Fixture loading & validation
# ----------------------------------------------------------------------

def load_and_validate_fixture(path: Path) -> dict:
    try:
        data = yaml.safe_load(path.read_text())
    except Exception as exc:
        raise NotionError(
            f"Failed to read fixture file: {exc}",
            status_code=400,
            code="invalid_yaml",
        )

    try:
        validate(instance=data, schema=NOTION_FIXTURE_SCHEMA)
    except ValidationError as exc:
        raise NotionError(
            f"Fixture validation failed: {exc.message}",
            status_code=400,
            code="validation_error",
        )

    return data


# ----------------------------------------------------------------------
# Fixture execution (pure utility logic)
# ----------------------------------------------------------------------

def create_store_from_fixture(
    data: dict,
    client: InMemoryNotionClient,
) -> None:
    logical_id_map: dict[str, str] = {}

    # ---- Pages (roots / hierarchy)
    for page in data["pages"]:
        parent = page["parent"]

        payload = {
            "parent": (
                {"type": "page_id", "page_id": logical_id_map[parent]}
                if parent
                else {"type": "page_id", "page_id": None}
            ),
            "properties": {
                "title": {
                    "title": [{"text": {"content": page["title"]}}]
                }
            },
        }

        obj = client.pages_create(payload)
        logical_id_map[page["id"]] = obj["id"]

    # ---- Databases
    for db in data["databases"]:
        parent_id = logical_id_map.get(db["parent"])
        if not parent_id:
            raise NotionError(
                f"Unknown parent page '{db['parent']}'",
                status_code=400,
                code="validation_error",
            )

        payload = {
            "parent": {"type": "page_id", "page_id": parent_id},
            "title": [{"text": {"content": db["title"]}}],
            "properties": {
                name: {prop_type: {}}
                for name, prop_type in db["properties"].items()
            },
        }

        obj = client.databases_create(payload)
        logical_id_map[db["id"]] = obj["id"]

    # ---- Pages in databases (bulk rows)
    for group in data["pages_in_databases"]:
        db_id = logical_id_map.get(group["database"])
        if not db_id:
            raise NotionError(
                f"Unknown database '{group['database']}'",
                status_code=400,
                code="validation_error",
            )

        database = client._store[db_id]
        schema = database["properties"]

        for row in group["rows"]:
            props = {}

            for name, schema_prop in schema.items():
                value = row.get(name)
                prop_type = schema_prop["type"]

                if prop_type == "title":
                    props[name] = {
                        "title": [{"text": {"content": str(value)}}]
                    }
                else:
                    props[name] = {
                        "rich_text": [{"text": {"content": str(value)}}]
                    }

            payload = {
                "parent": {"type": "database_id", "database_id": db_id},
                "properties": props,
            }

            client.pages_create(payload)


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a persisted Notion store from a fixture file."
    )
    parser.add_argument(
        "--fixture",
        required=True,
        type=Path,
        help="Path to the declarative YAML fixture",
    )
    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="Path where the store will be saved",
    )

    args = parser.parse_args()

    data = load_and_validate_fixture(args.fixture)

    client = FileBasedNotionClient(store_path=args.output)
    client.reset()

    create_store_from_fixture(data, client)

    client.save()

    print(f"✔ Store written to {args.output}")


if __name__ == "__main__":
    main()
