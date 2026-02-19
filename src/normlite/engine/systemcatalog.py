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

import pdb
from typing import TYPE_CHECKING, Optional

from normlite.engine.reflection import ReflectedTableInfo, SystemTablesEntry, TableState
from normlite.notiondbapi.dbapi2 import InternalError, ProgrammingError
from normlite.notion_sdk.client import AbstractNotionClient, NotionError

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

    def find_sys_tables_row(
        self,
        table_name: str,
        *,
        table_catalog: Optional[str] = None,
    ) -> Optional[SystemTablesEntry]:
        """Return the tables row (Notion page object) for a table or None if it does not exist.

        Args:
            table_name (str): _description_
            table_catalog (Optional[str], optional): _description_. Defaults to None.

        Raises:
            InternalError: _description_

        Returns:
            Optional[SystemTablesEntry]: _description_
        """

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

    def ensure_sys_tables_row(
            self,
            table_name: str,
            table_schema: Optional[str] = 'not_used',
            *,
            table_catalog: str,
            table_id: str,
            if_not_exists: bool = False
    ) -> SystemTablesEntry:
        return self.get_or_create_sys_tables_row(
            table_name,
            table_schema,
            table_catalog=table_catalog,
            table_id=table_id,
            if_not_exists=if_not_exists
        )

    def get_or_create_sys_tables_row(
        self, 
        table_name: str, 
        table_schema: Optional[str] = 'not_used',
        *,
        table_catalog: str, 
        table_id: str,
        if_not_exists: bool = False
    ) -> Optional[SystemTablesEntry]:
        """Return the system tables catalog entry for the specified table name.

        This method checks first for existance and creates a new entry if the given
        table name was not found.

        .. versionadded:: 0.8.0

        Args:
            table_name (str): _description_
            table_catalog (str): _description_
            table_id (str): _description_
            table_schema (Optional[str], optional): _description_. Defaults to 'not_used'.
            if_not_exists (bool, optional): _description_. Defaults to False.

        Raises:
            ProgrammingError: If a table with the same name exists in the catalog.

        Returns:
            Optional[SystemTablesEntry]: The system tables entry.
        """
        existing = self.find_sys_tables_row(table_name, table_catalog=table_catalog)

        if existing is not None and not existing.is_dropped:
            if if_not_exists:
                return existing

            raise ProgrammingError(
                f"Table '{table_name}' already exists in catalog '{table_catalog}'"
            )

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

    def set_dropped_by_page_id(
        self,
        *,
        page_id: str,
        dropped: bool,
    ) -> SystemTablesEntry:

        try:
            page_obj = self._client.pages_update(
                path_params={"page_id": page_id},
                payload={"in_trash": dropped},
            )

        except NotionError as exc:
            if exc.code == "object_not_found":
                raise ProgrammingError(page_id)
            raise

        return SystemTablesEntry.from_dict(page_obj)


    def set_dropped(
        self,
        *,
        table_name: str,
        table_catalog: str,
        dropped: bool,
    ) -> Optional[SystemTablesEntry]:
        """Soft-delete or restore the page corresponding to the entry in the system tables catalog.

        .. note::
            This methods sets the "in_trash" property at page level (the Notion page object in the system
            tables database). The Notion database object implementing the table itself is **not affected**
            by this method.

        .. versionadded:: 0.8.0

        Args:
            table_name (str): Name of the table to soft-delete or restore
            table_catalog (str): Name of the catalog the table belongs
            dropped (bool): Sof-delete if ``True``, restore if ``False``.

        Raises:
            ProgrammingError: If the given table name does not exists.

        Returns:
            Optional[SystemTablesEntry]: The updated system tables catalog entry.
        """
        entry = self.find_sys_tables_row(
            table_name,
            table_catalog=table_catalog,
        )

        if entry is None:
            raise ProgrammingError(
                f"Table '{table_name}' does not exist"
            )

        return self.set_dropped_by_page_id(
            page_id=entry.sys_tables_page_id,
            dropped=dropped
        )

    def repair_missing(
        self,
        *,
        table_name: str,
        table_catalog: str,
        table_id: Optional[str] = None,
    ) -> SystemTablesEntry:

        entry = self.find_sys_tables_row(
            table_name,
            table_catalog=table_catalog,
        )

        if entry:
            return entry

        if table_id is None:
            raise InternalError(
                f"Cannot repair missing catalog entry for "
                f"'{table_name}' without database_id"
            )

        # recreate missing metadata row
        return self.ensure_sys_tables_row(
            table_name=table_name,
            table_catalog=table_catalog,
            table_id=table_id,
            if_not_exists=True,
        )
    
    def set_dropped_by_page_id(
        self,
        *,
        page_id: str,
        dropped: bool,
    ) -> Optional[SystemTablesEntry]:
        """Soft-delete or restore a table entry in the system catalog by supplying the page id.

        .. note::
            This methods sets the "in_trash" property at page level (the Notion page object in the system
            tables database). The Notion database object implementing the table itself is **not affected**
            by this method.

        .. versionadded:: 0.8.0

        Args:
            page_id (str): System tables catalog page id of the table to soft-delete or restore.
            dropped (bool): _description_

        Raises:
            ProgrammingError: _description_

        Returns:
            Optional[SystemTablesEntry]: _description_
        """
        try:
            page_obj = self._client.pages_update(
                path_params={"page_id": page_id},
                payload={"in_trash": dropped},
            )
        except NotionError as exc:
            if exc.code == "object_not_found":
                raise ProgrammingError(page_id)
            raise

        return SystemTablesEntry.from_dict(page_obj)

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
        self.ensure_sys_tables_row(
            table_name='tables',
            table_schema='information_schema',
            table_catalog=self._user_database_name,
            table_id=self._tables_id 
        )

    def _delete_restore_table(
            self, 
            page_id: str, 
            delete: bool
    ) -> Optional[SystemTablesEntry]:
        """Soft-delete/restore a table in system tables."""
        try: 
            page_obj = self._client.pages_update(
                path_params= {                
                    'page_id': page_id,
                },
                payload={
                    'in_trash': delete
                }
            )
        except NotionError as exc:
            # a NotionError at this point can only have one meaning:
            # A programming error <=> page_id not found
            if exc.code == 'object_not_found':
                raise ProgrammingError(page_id)
            
            # All other DBAPI errors propagate unchanged
            # This is a fallback and it is expected to never happen
            raise

        return SystemTablesEntry.from_dict(page_obj)

    def _find_database_by_name(
            self,
            table_name: str,
    ) -> Optional[ReflectedTableInfo]:
        
        response = self._client.search(
            payload={
                "query": table_name,
                "filter": {
                    "property": "object",
                    "value": "database"
                }
            }
        )

        results = response.get("results", [])

        if len(results) > 1:
        # catalog corruption invariant.
            raise InternalError(
                f"Catalog invariant violated: multiple tables named "
                f"'{table_name}' found"
            )

        return ReflectedTableInfo.from_dict(results[0]) if results else None        

    def get_table_state(
        self,
        table_name: str,
        *,
        table_catalog: str,
    ) -> TableState:
        """Derive the lifecycle state of a table from the system catalog.

        ..  versionadded:: 0.8.0
        """

        entry = self.find_sys_tables_row(
            table_name,
            table_catalog=table_catalog,
        )

        # --------------------------------------------
        # Case 1: No metadata entry
        # --------------------------------------------
        if entry is None:
            # try to detect stray database object
            db = self._find_database_by_name(table_name)

            if db is not None:
                return TableState.ORPHANED

            return TableState.MISSING

        # --------------------------------------------
        # Case 2: Metadata exists
        # --------------------------------------------
        try:
            db = self._client.databases_retrieve(
                path_params={
                    "database_id": entry.table_id
                }
            )
        except NotionError as exc:
            if exc.code == "object_not_found":
                return TableState.ORPHANED
            raise

        sys_in_trash = entry.is_dropped
        db_in_trash = db.get("in_trash", False)

        # --------------------------------------------
        # Case 3: Both exist → derive state
        # --------------------------------------------

        if not sys_in_trash and not db_in_trash:
            return TableState.ACTIVE

        if sys_in_trash and db_in_trash:    
            return TableState.DROPPED

        # mismatch
        return TableState.ORPHANED