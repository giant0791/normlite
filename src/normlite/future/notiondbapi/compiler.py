import pdb
from typing import Optional, Sequence
from normlite._constants import SpecialColumns
from normlite.future.notiondbapi._model import NotionDatabase, NotionPage, NotionProperty, NotionObjectCompiler
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.future.exceptions import CompileError

_typecode_mapper = {
    'title': DBAPITypeCode.TITLE,
    'rich_text': DBAPITypeCode.RICH_TEXT,
    'number': DBAPITypeCode.NUMBER,
    'number_with_commas': DBAPITypeCode.NUMBER_WITH_COMMAS,
    'dollar': DBAPITypeCode.NUMBER_DOLLAR,
    'checkbox': DBAPITypeCode.CHECKBOX,
    'date': DBAPITypeCode.DATE
}

class RowCompiler(NotionObjectCompiler):
    def visit_page(self, page: NotionPage) -> Sequence[tuple]:
        visited_page = list()
        # (column_name, column_arg, column_value)
        special_columns = [
            (SpecialColumns.NO_ID, page.id,),
            (SpecialColumns.NO_ARCHIVED, page.archived,),
            (SpecialColumns.NO_IN_TRASH, page.in_trash,),
        ]

        visited_page.extend(special_columns)
        properties = [p.compile(self) for p in page.properties]
        visited_page.extend(properties)

        return visited_page

    def visit_database(self, database: NotionDatabase) -> Sequence[tuple]:
        visited_database = list()
        special_columns = [
            (SpecialColumns.NO_ID, None, database.id,),
            (SpecialColumns.NO_ARCHIVED, None, database.archived,),
            (SpecialColumns.NO_TITLE, None, database.title)
        ]
        visited_database.extend(special_columns)
        properties = [p.compile(self) for p in database.properties]
        visited_database.extend(properties)

        return visited_database

    def visit_property(self, prop: NotionProperty) -> tuple:
        if prop._is_page_property:
            if prop.is_page_created_or_updated:
                return (prop.name, prop.id,)
            else:
                return (prop.name, prop.value,)
        else:
            return (prop.name, prop.id, prop.value,)
    
class DescriptionCompiler(NotionObjectCompiler):
    def _add_not_used_seq(self, col_desc: tuple, count: int = 5) -> tuple:
        """Helper to fill in the missing elements with ``None`` values."""
        for _ in range(count):
            col_desc += (None,)

        return col_desc

    def visit_page(self, page: NotionPage) -> Sequence[tuple]:
        if page.is_page_created_or_updated:
            raise CompileError(
                'Cannot compile description for a page that has been created or updated. '
                'Properties do not have "type" keys.'
            )

        return [
            self._add_not_used_seq((SpecialColumns.NO_ID, DBAPITypeCode.ID,)), 
            self._add_not_used_seq((SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX,)),
            self._add_not_used_seq((SpecialColumns.NO_IN_TRASH, DBAPITypeCode.CHECKBOX,)),
            *[prop.compile(self) for prop in page.properties]             
        ]

    def visit_database(self, database: NotionDatabase) -> Sequence[tuple]:
        return [
            self._add_not_used_seq((SpecialColumns.NO_ID, DBAPITypeCode.ID,)),
            self._add_not_used_seq((SpecialColumns.NO_ARCHIVED, DBAPITypeCode.CHECKBOX,)),
            self._add_not_used_seq((SpecialColumns.NO_TITLE, DBAPITypeCode.TITLE,)),
            *[prop.compile(self) for prop in database.properties]
        ]

    def visit_property(self, prop: NotionProperty) -> Optional[tuple]:
        try:
            return self._add_not_used_seq((prop.name, _typecode_mapper[prop.type],))
        except KeyError:
            raise TypeError(f'Unexpected or unsupported property type: "{prop.type}"')
