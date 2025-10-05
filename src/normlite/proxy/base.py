# normlite/proxy/base.py
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

from abc import ABC, abstractmethod
import requests

class BaseProxyClient(ABC):
    """Abstract base class for proxy clients."""

    @abstractmethod
    def connect(self) -> requests.Response:
        """Check if the proxy server is alive.
        Must raise ProxyError on failure.
        """
        raise NotImplementedError
    
    @abstractmethod
    def begin(self) -> requests.Response:
        """Begin a new transaction.

        Returns:
            requests.Response: The returned response object.
        """
        raise NotImplementedError
    
    @abstractmethod
    def insert(self, tx_id: str, payload: dict) -> requests.Response:
        """Add an insert operation to the specified transaction.

        This method is a wrapper around the REST API `transactions/<id>/insert`.

        Args:
            txn (str): The transaction to add the insert operation to.
            payload (dict): The payload of the insert operation.

        Returns:
            requests.Response: The returned response object. 
        """

    @abstractmethod
    def commit(self, tx_id: str) -> requests.Response:
        """Commit the specified transaction.

        This method is a wrapper around the REST API `transactions/<id>/commit`.

        Args:
            tx_id (str): The transaction to be committed.

        Returns:
            requests.Response: The returned response object.
        """

    @abstractmethod
    def rollback(self, tx_id: str) -> requests.Response:
        """Roll back the specified transaction.

        This method is a wrapper around the REST API `transactions/<id>/rollback`.

        Args:
            tx_id (str): The transaction to be rolled back.

        Returns:
            requests.Response: The returned response object.
        """

