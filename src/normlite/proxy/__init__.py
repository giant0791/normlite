# normlite/proxy/__init__.py
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
"""Provide a proxy server for ``normlite`` clients for transaction management on Notion REST API requests.

This package provides transaction management for Notion.
It implements under the wood a Flask proxy that sits between the ``normlite`` client and Notion.
The ``normlite`` client application interacts with the Flask proxy via the :class:`normlite.engine.base.Engine` class.

.. versionchanged:: 0.7.0
    The package implements an in-process test client based on the pytest Flask plugin via :class:`normlite.proxy.client.InProcessProxyClient`
    through a uniform interface (defined by :class:`normlite.proxy.base.BaseProxyClient`).

.. versionadded: 0.6.0
    Initial version. Experimental code. Tests successful.

Warning:
    The code in this package is experimental and subject to breaking changes in the future.
    
    **Do not use it yet!**
"""