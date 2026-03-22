"""
.. caution::
    

.. versionadded:: 0.9.0
"""
from dataclasses import dataclass, field
from datetime import datetime
import uuid
from typing import Dict, Set, Protocol, Optional
import copy

from normlite.notion_sdk.client import InMemoryNotionClient, NotionError

@dataclass
class Transaction:
    """Provide context for transactional operations."""
    id: str
    status: str = "active"
    created_time: str = field(default_factory=lambda: datetime.now().isoformat())

    overlay: Dict[str, dict] = field(default_factory=dict)
    original: Dict[str, Optional[dict]] = field(default_factory=dict)

    read_set: Set[str] = field(default_factory=set)
    write_set: Set[str] = field(default_factory=set)

    def record_read(self, page_id: str):
        self.read_set.add(page_id)

    def record_write(self, page_id: str, base_store: dict):
        if page_id not in self.original:
            self.original[page_id] = copy.deepcopy(base_store.get(page_id))
        self.write_set.add(page_id)

    def stage_write(self, page_id: str, updated: dict):
        self.overlay[page_id] = updated

class SupportsTransactions(Protocol):
    """Mixin for transactional operation support.
    
    .. versionadded:: 0.9.0
    """
    supports_transactions: bool

    def transactions_create(
        self,
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None,
        payload: Optional[dict] = None
    ) -> dict:
        ...

class TransactionalInMemoryNotionClient(
    InMemoryNotionClient,
    SupportsTransactions
):
    supports_transactions = True

    def __init__(self):
        super().__init__()
        self._transactions: dict[str, Transaction] = {}

    def _get_txn(self, transaction_id: Optional[str]) -> Optional[Transaction]:
        if not transaction_id:
            return None

        txn = self._transactions.get(transaction_id)
        if not txn:
            raise NotionError(
                message=f"Could not find transaction with ID: {transaction_id}.",
                status_code=404,
                code="object_not_found"
            )

        return txn
    
    def _execute_in_txn_overlay(self, txn: Transaction, fn):
        """
        Execute a superclass write operation into a temporary store,
        then move results into the transaction overlay.
        """
        original_store = self._store
        temp_store = {}

        try:
            # redirect writes
            self._store = temp_store

            result = fn()

            # move staged objects into txn overlay
            for obj_id, obj in temp_store.items():
                txn.record_write(obj_id, original_store)
                txn.stage_write(obj_id, obj)

            return result

        finally:
            # restore real store
            self._store = original_store    
    
    # ------------------------
    # TRANSACTIONS ENDPOINT
    # ------------------------

    def transactions_begin(
        self,
        path_params: Optional[dict] = None,
        query_params: Optional[dict] = None,
        payload: Optional[dict] = None
    ) -> dict:

        txn_id = str(uuid.uuid4())

        txn = Transaction(id=txn_id)

        self._transactions[txn_id] = txn

        return {
            "object": "transaction",
            "id": txn.id,
            "status": txn.status,
            "created_time": txn.created_time
        }

    def databases_create(
        self,
        path_params=None,
        query_params=None,
        payload=None
    ):
        txn = self._get_txn(query_params.get("transaction_id") if query_params else None)

        if not txn:
            # normal behavior
            return super().databases_create(path_params, query_params, payload)

        # transactional behavior
        return self._execute_in_txn_overlay(
            txn,
            lambda: super().databases_create(path_params, query_params, payload)
        )

    def pages_create(
        self,
        path_params=None,
        query_params=None,
        payload=None
    ):
        txn = self._get_txn(query_params.get("transaction_id") if query_params else None)

        if not txn:
            return super().pages_create(path_params, query_params, payload)

        return self._execute_in_txn_overlay(
            txn,
            lambda: super().pages_create(path_params, query_params, payload)
        )