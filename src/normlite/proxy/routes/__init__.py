# normlite/proxy/routes/__init__.py
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

"""Transaction Proxy Server Routes.

This module provides the Flask routes that constitute the REST API for the transaction proxy server.
All endpoints return JSON objects according to the following schema.
** Success (health)**:

..code-block:: json
  {
    "state": "ALIVE"
  }

**Success (non-commit, e.g. insert)**:

.. code-block:: json

  {
    "transaction_id", "<tx_id>",    // <tx_id> = uuid4 string, returned by POST /transactions only
    "state": "<tx_state>",          // <tx_state> = transaction state, returned by
                                    // POST /transactions/<id>/insert
                                    // POST /transactions/<id>/commit
                                    // POST /transactions/<id>/rollback
  }
    
**Success (commit)**:

.. code-block:: json
  {
    "transaction_id", "<tx_id>",    // <tx_id> = uuid4 string, id  of the committed transaction
    "state": "<tx_state>",          // <tx_state> = "COMMITTED"
    "data": "<result_sets>",        // <result_sets> = a list containing all results returned by each operation committed/rolled back
  }

**Error**:

..code-block:: json
  {
    "error": {
      "code": "<error_code>",       // <error_code> = e.g. "invalid_payload"    
      "message": "<error message"   // <error_message>" = e.g. "Missing parent in payload"
    },
    "transaction_id": "<tx_id>",    // optional, if error occurred in a transaction that was already started
    "state": "<tx_state>"           // optional, if error occurred in a transaction that was already started
  }

    
.. list-table:: Transaction Proxy Server REST API
   :header-rows: 1
   :widths: 15, 25, 60
   :class: longtable

   * - Method
     - Endpoint
     - Description
   * - ``GET``
     - ``/health``
     - Let clients enquiry server health.
   * - ``POST``
     - ``/transactions``
     - Begin a new transaction (see :func:`normlite.proxy.routes.transactions.begin_transaction()` for more details).
   * - ``POST``
     - ``/transactions/<id>/insert``
     - Add an inser operation to an existing transaction (see :func:`normlite.proxy.routes.insert.insert()` for more details).
   * - ``POST``
     - ``/transactions/<id>/commit``
     - Commit an existing transaction (see :func:`normlite.proxy.routes.transactions.commit_transaction()` for more details).
   * - ``POST``
     - ``/transactions/<id>/rollback``
     - Roll back an existing transaction (see :func:`normlite.proxy.routes.transactions.rollback_transaction()` for more details).
    
.. versionadded:: 0.6.0

.. versionchanged:: 0.7.0
  ``GET /health`` added, response schema redefin

"""
