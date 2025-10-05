# normlite/proxy/routes/transactions.py
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
from flask import Blueprint, jsonify

from normlite.proxy.state import transaction_manager

transaction_bp = Blueprint("transactions", __name__)

@transaction_bp.route("/transactions", methods=["POST"])
def begin_transaction():
    tx = transaction_manager.begin()
    return jsonify(
        {"transaction_id": tx.tid, "state": tx.state.name}
    ), 200

@transaction_bp.route("/transactions/<tx_id>/commit", methods=["POST"])
def commit_transaction(tx_id):
    tx = transaction_manager.active_txs.get(tx_id)
    if not tx:
        return jsonify(
            {
                "error": {
                    "code": "txn_not_found", 
                    "message": f"Transaction: {tx_id} not found"
                }
            }
        ), 404
    try:
        tx.commit() 
        return jsonify(
            {
                "transaction_id": tx_id,
                "state": tx.state.name,
                "data": tx.results
            }
        ), 200
    
    except Exception as e:
        return jsonify(
            {
                "transaction_id": tx_id,
                "state": tx.state.name,
                "error": {
                    "code": "commit_failed",
                    "message": str(e)
                },
                "data": tx.results if tx.results else None
            }
        ), 500

@transaction_bp.route("/transactions/<tx_id>/rollback", methods=["POST"])
def rollback_transaction(tx_id):
    tx = transaction_manager.active_txs.get(tx_id)
    if not tx:
        return jsonify(
            {
                "error": {
                    "code": "txn_not_found", 
                    "message": f"Transaction: {tx_id} not found"
                }
            }
        ), 404
    try:
        tx.rollback()
        return jsonify({
            "transaction_id": tx_id,
            "state": tx.state.name,

        })
    except Exception as e:
        data = tx.results if tx.results else None           # if residual results are available, return them
        return jsonify(
            {
                "transaction_id": tx_id,
                "state": tx.state.name,
                "error": {
                    "code": "rollback_failed",
                    "message": str(e)
                },
                "data": data
            }
        ), 500

