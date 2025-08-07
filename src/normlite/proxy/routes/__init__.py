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
All endpoints return JSON objects of the following form:

.. code-block:: json

    {
        "transaction_id", "<tx_id>",    // <tx_id> = uuid4 string, returned by POST /transactions only
        "state": "<tx_state>",          // <tx_state> = transaction state, returned by
                                        // POST /transactions/<id>/insert
                                        // POST /transactions/<id>/commit
                                        // POST /transactions/<id>/rollback
        "data": "<result_sets>",        // <result_sets> = a list containing all results returned by each operation committed/rolled back
        "error": "<error>"              // <error> = error message string, returned in case of error only    
    }

.. list-table:: Transaction Proxy Server REST API
   :header-rows: 1
   :widths: 15, 25, 60
   :class: longtable

   * - Method
     - Endpoint
     - Description
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

"""
from typing import Optional
from flask import Response, jsonify

from normlite.proxy.state import transaction_manager

def _make_response_obj(obj: dict, tx_id: Optional[str] = None) -> Response:
    """Helper to generate standard response objects."""
    
    if tx_id:
        # the transaction exists, state is defined
        tx = transaction_manager.active_txs.get(tx_id)
        state = tx.state.name

    else:
        # the transaction does not exists, state is undefined, add fictious state
        state = 'NOT_ACTIVE'

    obj['state'] = state
    return jsonify(obj)
