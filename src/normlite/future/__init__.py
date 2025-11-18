# normlite/future/__init__.py
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
"""Explorations and early prototyping of future features.

This package is a playgorund to test new ideas in explorations and for early prototyping.
It resembles the main packge structure, and it allows to add new modules without disrupting
the code base.

Here a quick example of how it is used.

.. code-block:: python

    # normlite/future/notion_sdk/dbapi2.py
    # prototype new commit and rollback in DBAPI connection.
    # ...

    def commit(self) -> None:
        response = self._proxy_client.post(
            f'/transactions/{self._tx_id}/commit'
        )

        if response.status_code != 200:
            raise DatabaseError(
                f'Failed to commit transaction: {self._tx_id}. '
                f'Reason: {response.get_json()['error']}'
            )
        
        # create cursors
        self._create_cursors(response.get_json()['data'])

"""
