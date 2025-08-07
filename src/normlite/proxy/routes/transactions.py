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

from flask import Blueprint

from normlite.proxy.routes import _make_response_obj
from normlite.proxy.state import transaction_manager

transaction_bp = Blueprint("transactions", __name__)

@transaction_bp.route("/transactions", methods=["POST"])
def begin_transaction():
    tx = transaction_manager.begin()
    return _make_response_obj({"transaction_id": tx.tid}), 200

@transaction_bp.route("/transactions/<tx_id>/commit", methods=["POST"])
def commit_transaction(tx_id):
    tx = transaction_manager.active_txs.get(tx_id)
    if not tx:
        return _make_response_obj({"error": "Transaction not found"}), 404
    try:
        tx.commit()
        data = [op.get_result() for _, _, op in tx.operations] 
        return _make_response_obj({"data": data}, tx_id), 200
    
    except Exception as e:
        return _make_response_obj({"error": str(e)}, tx_id), 500

@transaction_bp.route("/transactions/<tx_id>/rollback", methods=["POST"])
def rollback_transaction(tx_id):
    tx = transaction_manager.active_txs.get(tx_id)
    if not tx:
        return _make_response_obj({"error": "Transaction not found"}), 404

    tx.rollback()

    data = [op.get_result() for _, _, op in tx.operations]
    return _make_response_obj({"data": data}, tx_id), 200

