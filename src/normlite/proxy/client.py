# normlite/proxy/client.py
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
from __future__ import annotations
import pdb
from typing import TYPE_CHECKING, Any
import requests
from requests.structures import CaseInsensitiveDict
from werkzeug import wrappers

from normlite.proxy.base import BaseProxyClient

if TYPE_CHECKING:
    from flask.testing import FlaskClient

class InProcessProxyClient(BaseProxyClient):
    """ Provide a proxy client in-process implementation for test purposes.

    .. versionadded:: 0.7.0
        Proxy client based on Flask test client enables easy testing.
    """
    def __init__(self, flask_client: FlaskClient):
        self._client = flask_client

    def connect(self) -> requests.Response:
        flask_resp = self._client.get("/health")
        return self._flask_to_requests_response(flask_resp)
    
    def begin(self) -> requests.Response:
        flask_resp = self._client.post("/transactions")
        return self._flask_to_requests_response(flask_resp)
    
    def insert(self, tx_id: str, payload: dict) -> requests.Response:
        flask_resp = self._client.post(f"/transactions/{tx_id}/insert", json=payload)
        return self._flask_to_requests_response(flask_resp)
    
    def commit(self, tx_id: str) -> requests.Response:
        flask_resp = self._client.post(f"/transactions/{tx_id}/commit")
        return self._flask_to_requests_response(flask_resp)
    
    def rollback(self, tx_id: str) -> requests.Response:
        flask_resp = self._client.post(f"/transactions/{tx_id}/rollback")
        return self._flask_to_requests_response(flask_resp)
 
    def _flask_to_requests_response(self, flask_resp: wrappers.Response) -> requests.Response:
        """Convert a Flask TestResponse into a requests.Response."""
        resp = requests.Response()

        # Status
        resp.status_code = flask_resp.status_code
        resp.reason = flask_resp.status
        resp.url = flask_resp.request.path if flask_resp.request else None

        # Headers
        resp.headers = CaseInsensitiveDict(flask_resp.headers)

        # Content
        resp._content = flask_resp.data
        resp.encoding = flask_resp.content_encoding

        # Make .json() work
        resp._content_consumed = True

        return resp

class ProxyClient(BaseProxyClient):
    def __init__(self, base_url: str, timeout: int) -> None:
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout

    def connect(self) -> requests.Response:
        return requests.get(f"{self.base_url}/health", timeout=self.timeout)


def create_proxy_client(**kwargs: Any) -> ProxyClient:
    if "base_url" in kwargs:
        base_url = kwargs.pop("base_url")
        timeout = kwargs.pop("timeout", 5)
        return ProxyClient(base_url=base_url, timeout=timeout)
    
    if "flask_client" in kwargs:
        flask_client = kwargs.pop('flask_client')
        return InProcessProxyClient(flask_client=flask_client)
