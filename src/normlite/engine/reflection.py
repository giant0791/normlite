from __future__ import annotations
import pdb
from typing import Any, NamedTuple, Optional, Sequence
from normlite._constants import SpecialColumns
from normlite.exceptions import ArgumentError
from normlite.engine.row import Row
from normlite.sql.type_api import Boolean, ObjectId, String, TypeEngine, type_mapper

class ReflectedColumnInfo(NamedTuple):
    name: str
    type: TypeEngine
    id: Optional[str]
    value: Optional[Any]

class ReflectedTableInfo:
    def __init__(self, columns: Sequence[ReflectedColumnInfo]):
        self._colmap = {
            rc.name: index
            for index, rc in enumerate(columns)
        }

        self._columns = columns
        try:
            self._name = columns[self._colmap[SpecialColumns.NO_TITLE]].value
            self._id = columns[self._colmap[SpecialColumns.NO_ID]].value
            self._archived = columns[self._colmap[SpecialColumns.NO_ARCHIVED]].value
            self._in_trash = columns[self._colmap[SpecialColumns.NO_IN_TRASH]].value
        except TypeError as te:
            raise ArgumentError(
                'Columns argument does not contain properly reflected columns info: '
                f'{te}'
            ) from te

    @property
    def name(self) -> str:
        return self._name
    
    @property
    def id(self) -> str:
        return self._id
    
    @property
    def archived(self) -> Optional[True]:
        return self._archived
    
    def get_columns(self) -> Sequence[ReflectedColumnInfo]:
        return self._columns
    
    def get_column_names(self, include_all: Optional[bool] = True) -> Sequence[str]:
        if include_all:
            return [rc.name for rc in self._columns]
        else:
            return [rc.name for rc in self._columns if rc.name not in SpecialColumns.values()]
        
    @classmethod
    def from_rows(cls, cols_as_rows: Sequence[Row]) -> "ReflectedTableInfo":
        """
        Build a ReflectedTableInfo from a sequence of column-definition rows.

        Each row must provide:
            (column_name, column_type, column_id, column_value)

        Special columns carry table-level metadata via column_value.
        """

        columns: list[ReflectedColumnInfo] = []

        for row in cols_as_rows:
            col_name, col_type, col_id, col_value = row
            columns.append(
                ReflectedColumnInfo(
                    name=col_name,
                    type=col_type,
                    id=col_id,
                    value=col_value,
                )
            )

        # ---- validation (fail fast) ----
        # TODO

        return cls(columns=columns)

    @classmethod
    def from_dict(cls, database_obj: dict) -> ReflectedTableInfo:
        cols = []

        # special columns first
        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_ID,
            type=ObjectId(),
            id=None,
            value=database_obj['id'],
        ))

        result_process = String(is_title=True).result_processor(None)
        database_name = result_process({'title': database_obj['title']})
        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_TITLE,
            type=String(is_title=True),
            id=None,
            value=database_name,
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_ARCHIVED,
            type=Boolean(),
            id=None,
            value=database_obj['archived']
        ))

        cols.append(ReflectedColumnInfo(
            name=SpecialColumns.NO_IN_TRASH,
            type=Boolean(),
            id=None,
            value=database_obj['in_trash']
        ))

        # reflect properties
        for name, prop in database_obj["properties"].items():
            cols.append(
                ReflectedColumnInfo(
                    name=name,
                    type=type_mapper[prop["type"]],
                    id=prop["id"],
                    value=None,
                )
            )
        
        return cls(cols)


    

