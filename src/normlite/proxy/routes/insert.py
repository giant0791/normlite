# normlite/proxy/routes/insert.py
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

import pdb
from flask import Blueprint, request, jsonify

from normlite.proxy.state import transaction_manager, notion
from normlite.txmodel.operations import StagedInsert

insert_bp = Blueprint("insert", __name__)


@insert_bp.route("/transactions/<tx_id>/insert", methods=["POST"])
def insert(tx_id):
    tx = transaction_manager.active_txs.get(tx_id)
    if not tx:
        return jsonify({
            "error": {
                "code": "invalid_tx_id",
                "message": f"Transaction with id:{tx_id} not found"
            }
        }), 400

    try:
        page_payload = request.get_json()
        insert_op = StagedInsert(notion=notion, page_payload=page_payload, tx_id=tx_id)

        # IMPORTANT: For insert operations, the resource_id to lock is the database_id
        tx.add_change(
            resource_id=page_payload["parent"]["database_id"],
            mode="write",
            operation=insert_op
        )
        return jsonify(
            {
                'state': transaction_manager.active_txs[tx_id].state.name, 
                'transaction_id': tx_id
            }
        ), 202

    except KeyError as ke:
        # the only reason why Transaction.add_change() can fail is no database_id found
        return jsonify(
            {
                "error": {
                    "code": "invalid_payload",
                    "message": f'Missing "{ke.args[0]}" in payload'
                }
            }
        ), 400
