# normlite/engine/notionuri.py
# Copyright (C) 2026 Gianmarco Antonini
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

"""Provide the parsed form of a normlite URI.

A normlite URI is the address of a Notion integration. :func:`_parse_uri` reads the URI string
and puts its parts into one :class:`NotionURI` object.

.. versionadded:: 0.15.0
    Before this version, the URI types lived in :mod:`normlite.engine.base`.
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional
from urllib.parse import urlparse, parse_qs, unquote


@dataclass(frozen=True, kw_only=True)
class NotionURI:
    """Hold the parts of a parsed normlite URI.

    One flat type holds the fields of all URI kinds. ``kind`` tells which fields apply.
    Use :func:`_parse_uri` to construct a valid object.

    .. versionchanged:: 0.15.0
        :class:`NotionURI` is one flat, frozen, keyword-only dataclass. It replaces the union of
        ``NotionSimulatedURI`` and ``NotionAuthURI``, which are deleted. The ``file`` field is
        deleted, and the new ``database`` field holds the file stem.
    """

    kind: Literal["simulated", "internal", "external"]
    """The kind of the integration."""

    database: Optional[str] = None
    """The default user database name.

    ``"memory"`` for an in-memory integration, the file stem for a file-based integration,
    ``None`` for an auth integration.
    """

    # simulated
    mode: Optional[Literal["memory", "file"]] = None
    """The mode of a simulated integration (``None`` for auth integrations)."""

    path: Optional[str] = None
    """The absolute path to the store file (``None`` unless mode is ``"file"``)."""

    # internal
    token: Optional[str] = None
    """The integration token of an internal integration."""

    version: Optional[str] = None
    """The Notion API version of an internal integration."""

    # external
    client_id: Optional[str] = None
    """The OAuth client id of an external integration."""

    client_secret: Optional[str] = None
    """The OAuth client secret of an external integration."""

    auth_url: Optional[str] = None
    """The OAuth authorization URL of an external integration."""


def _parse_uri(uri: str) -> NotionURI:
    """Provide helper function to parse a normlite URI.

    .. versionchanged:: 0.15.0
        Returns one flat :class:`NotionURI` and sets its ``database`` field.
    """
    parsed = urlparse(uri)

    if not parsed.scheme in ["normlite+auth", "normlite"]:
        raise ValueError(f"Unsupported URI scheme: {parsed.scheme}")

    host = parsed.hostname
    path = unquote(parsed.path)

    if host in ("internal", "external"):
        params = parse_qs(parsed.query)
        def get_param(name: str) -> Optional[str]:
            return params.get(name, [None])[0]

        if host == "internal":
            return NotionURI(
                kind="internal",
                token=get_param("token"),
                version=get_param("version")
            )
        elif host == "external":
            return NotionURI(
                kind="external",
                client_id=get_param("client_id"),
                client_secret=get_param("client_secret"),
                auth_url=get_param("auth_url")
            )

    elif host is None:
        # Simulated integrations
        if path == "/:memory:":
            return NotionURI(
                kind="simulated",
                mode="memory",
                database="memory"
            )
        elif path.startswith("/"):
            return NotionURI(
                kind="simulated",
                mode="file",
                path=str(Path(path).absolute()),
                database=Path(path).stem
            )
        else:
            raise ValueError(f"Invalid simulated integration path: {path}")

    else:
        raise ValueError(f"Unsupported notion integration type: {host}")
