# notion_sdk/__init__.py 
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

"""This package provide client abstractions with a uniform API for the :mod:`normlite.notiondbapi.dbapi2`.

The major goal of this package is to abstract the Notion client Rest-full API. Thus, it provides an
adpatation layer to be used at DBAPI level.
A key adapter is represented by the :class:`normlite.notion_sdk.client.InMemoryNotionClient` class, which offers
a quick and easy way to learn and get familiar with ``normlite`` without having to access the real Notion backend.
There is also a file based version of this class, which adds persistence of the created objects.

.. seealso::
    
    - :class:`normlite.notion_sdk.client.InMemoryNotionClient` 
    - :class:`normlite.notion_sdk.client.FileBasedNotionClient`

.. note::
    This version does not yet provide a Rest-full client to connect to Notion.
"""



        

