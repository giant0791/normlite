# normlite/txmodel/operations.py
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
"""Provide a protocol for commit operation and all proxy server operations.

.. versionadded:: 0.6.0

"""
from typing import Protocol

from normlite.notion_sdk.client import AbstractNotionClient


class Operation(Protocol):
    """Interface for change requests that process data in the context of a transaction.
    
    Change requests follow a well-defined protocol to accomplish their task of modifying data in a consistent way.
    Each operation must define:

        * :meth:`stage()`: These are the pre-commit activities to validate data prior to committing them. 
        
        * :meth:`do_commit()`: All activities to commit data to the database.

        * :meth:`do_rollback()`: All activities to revert changes committed prior to this failed change.

    """
        
    def stage(self) -> None:
        """Stage and validate the data to be committed."""
        
    def do_commit(self) -> None:
        """Perform the commit activities associated with this operation."""
        
    def do_rollback(self) -> None:
        """Perform the rollback activities associated to this operation."""

    def get_result(self) -> dict:
        """Return the result of the last executed :class:`Operation.do_commit()` or :class:`Operation.do_rollback()`."""
    
class StagedInsert(Operation):
    """Operation to create a new page in a Notion database."""
    def __init__(self, notion: AbstractNotionClient, page_payload: dict, tx_id: str):
        self.notion = notion
        self.page_payload = page_payload
        self.tx_id = tx_id
        self.page_id = None  # Will be set on commit
        self._result = None # will be set on commit or on rollback

    def stage(self) -> None:
        # Pre-check — validate payload minimally
        try:
            self.page_payload['parent']
            self.page_payload['properties']
        except KeyError as ke:
            raise ValueError(
                f'Invalid Notion page pages.create payload: '
                f'Missing "{ke.args[0]}".'
            )

    def do_commit(self) -> None:
        self._result = self.notion('pages', 'create', self.page_payload)
        self.page_id = self._result.get("id")

    def do_rollback(self) -> None:
        # Only rollback if commit actually happened
        if self.page_id:
            try:
                self._result = self.notion('pages', 'update', {'id': self.page_id, 'data': {'archived': True}})
            except Exception:
                pass  # best-effort rollback

    def get_result(self) -> dict:
        return self._result
    
class StagedSelect(Operation):
    """Operation to query the database."""
    def __init__(self, notion: AbstractNotionClient, payload: dict, tx_id: str):
        self.notion = notion
        self.payload = payload
        self.tx_id = self.tx_id
        self._result = None

    def stage(self) -> None:
        if 'database_id' not in self.payload or 'filter' not in self.payload:
            raise ValueError('Missing database id or filters or both.')
        
    def do_commit(self) -> None:
        self._result = self.notion('databases', 'query', self.payload)

    def do_rollback(self):
        """Nothing to rollback, ``SELECT`` is non mutating."""
        ...

    def get_result(self) -> dict:
        return self._result

            
