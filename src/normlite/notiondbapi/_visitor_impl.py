# notiondbapi/_visitor_impl.py
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

"""Provide the visitor implementation for cross-compiling Notion JSON objects to tuples of elements.

This model takes a Notion API objects AST and compiles it into a tuple of elements.

Important:
    This module is **private** to the package :mod:`notiondbapi` and it does **not** expose
    its features outside.
"""

from normlite.notiondbapi._model import NotionDatabase, NotionObjectVisitor, NotionPage, NotionProperty


class ToRowVisitor(NotionObjectVisitor):
    def visit_page(self, page: NotionPage) -> tuple:
        return (
            'page',
            page.id,
            page.archived,
            page.in_trash,
            *[item for prop in page.properties for item in prop.accept(self)]
        )

    def visit_database(self, db: NotionDatabase) -> tuple:
        return (
            'database',
            db.id,
            db.title,
            db.archived,
            db.in_trash,
            *[item for prop in db.properties for item in prop.accept(self)]
        )

    def visit_property(self, prop: NotionProperty) -> tuple:
        return (prop.name, prop.id, prop.type, prop.value)

