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

