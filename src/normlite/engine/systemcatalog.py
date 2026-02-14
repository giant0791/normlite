# normlite/engine/systemcatalog.py 
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

"""Provide the implementation for the information schema.

The information schema consists of a set of views that contain information about the Notion objects
defined in the current database.
The primary view is the Notion database object "tables". This object stores all the SQL tables created in 
the current database.

.. versionadded:: 0.8.0
"""

from typing import TYPE_CHECKING, Optional

from normlite.engine.reflection import SystemTablesEntry
from normlite.notiondbapi.dbapi2 import InternalError, ProgrammingError
from normlite.notion_sdk.client import AbstractNotionClient

class SystemCatalog:
    def __init__(
        self,
        client: AbstractNotionClient,
        user_database_name: str,
        root_page_id: str,
        default_catalog: str,
    ):
        self._client = client
        self._user_database_name = user_database_name
        self._root_page_id = root_page_id
        self._tables_id = None
        self._default_catalog = default_catalog

    def find_sys_tables_row(
        self,
        table_name: str,
        *,
        table_catalog: Optional[str] = None,
    ) -> Optional[SystemTablesEntry]:
        """Return the tables row (Notion page object) for a table or None if it does not exist."""

        catalog = table_catalog or self._default_catalog

        response = self._client.databases_query(
            path_params={
                "database_id": self._tables_id,
            },

            payload={
                "filter": {
                    "and": [
                        {
                            "property": "table_name",
                            "title": {"equals": table_name},
                        },
                        {
                            "property": "table_catalog",
                            "rich_text": {"equals": catalog},
                        },
                    ]
                },
            }
        )

        results = response.get("results", [])

        if len(results) > 1:
        # catalog corruption invariant.
            raise InternalError(
                f"Catalog invariant violated: multiple tables named "
                f"'{table_name}' in catalog '{catalog}'"
            )

        return SystemTablesEntry.from_dict(results[0]) if results else None

    def bootstrap(self) -> None:
        # 1. information_schema page
        self._ischema_page_id = self._get_or_create_page(
            parent_id=self._root_page_id,
            name="information_schema",
        )

        # 2. tables database
        self._tables_id = self._get_or_create_database(
            parent_id=self._ischema_page_id,
            name="tables",
            properties={
                "table_name": {"title": {}},
                "table_schema": {"rich_text": {}},
                "table_catalog": {"rich_text": {}},
                "table_id": {"rich_text": {}},
            },
        )

       # 3. ensure tables self-row exists
        self._ensure_sys_tables_self_row()

        # 4. user tables page
        self._user_tables_page_id = self._get_or_create_page(
            parent_id=self._root_page_id,
            name=self._user_database_name,
        )

    # -------------------------------------------------
    # Find-or-create helpers
    # -------------------------------------------------

    def _get_or_create_page(self, parent_id: str, name: str) -> str:
 
        page = self._client.find_child_page(parent_id, name)
        if page:
            return page["id"]
        
        page = self._client._add(
            "page",
            {
                "parent": {"type": "page_id", "page_id": parent_id},
                "properties": {
                    "Name": {"title": [{"text": {"content": name}}]}
                },
            },
        )

        return page['id']

    def _get_or_create_database(
        self,
        parent_id: str,
        name: str,
        properties: dict,
    ) -> str:
        db = self._client.find_child_database(parent_id, name)
        if db:
            return db["id"]

        db = self._client._add(
            "database",
            {
                "parent": {"type": "page_id", "page_id": parent_id},
                "title": [{"type": "text", "text": {"content": name}}],
                "properties": properties,
            },
        )

        return db['id']

    def _ensure_sys_tables_self_row(self) -> None:
        self._ensure_sys_tables_row(
            name='tables',
            schema='information_schema',
            catalog=self._user_database_name,
            table_id=self._tables_id 
        )

    def _ensure_sys_tables_row(
            self,
            name: str,
            schema: Optional[str] = 'not_used',
            *,
            catalog: str,
            table_id: str
    ) -> None:
        self.get_or_create_sys_tables_row(
            name,
            schema,
            table_catalog=catalog,
            table_id=table_id
        )

    def get_or_create_sys_tables_row(
        self, 
        table_name: str, 
        table_schema: Optional[str] = 'not_used',
        *,
        table_catalog: str, 
        table_id: str,
        if_exists: bool = False
    ) -> SystemTablesEntry:
        existing = self.find_sys_tables_row(table_name, table_catalog=table_catalog)

        if existing is not None and not existing.is_dropped:
            if if_exists:
                raise ProgrammingError(
                    f"Table '{table_name}' already exists in catalog '{table_catalog}'"
                )

            return existing

        page_obj = self._client.pages_create(
            payload={
                "parent": {
                    "type": "database_id",
                    "database_id": self._tables_id,
                },
                "properties": {
                    "table_name": {
                        "title": [{"text": {"content": table_name}}]
                    },
                    "table_schema": {
                        "rich_text": [{"text": {"content": table_schema}}]
                    },
                    "table_catalog": {
                        "rich_text": [{"text": {"content": table_catalog}}]
                    },
                    "table_id": {
                        "rich_text": [{"text": {"content": table_id}}]
                    },
                },
            },
        )

        return SystemTablesEntry.from_dict(page_obj)
 