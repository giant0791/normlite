# normlite/engine/context.py 
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
from typing import TYPE_CHECKING
import copy

from normlite.cursor import CursorResult
from normlite.notiondbapi.dbapi2 import Cursor
from normlite.sql.type_api import TypeEngine

if TYPE_CHECKING:
    from normlite.engine.base import Connection
    from normlite.sql.base import Compiled

class ExecutionContext:
    def __init__(self, connection: Connection, compiled: Compiled):
        self._connection = connection
        self._compiled = compiled
        self._element = compiled._element
        self._binds = compiled.params
        self._result = None

    def _setup(self) -> None:
        """Perform value binding and type adaptation before execution.""" 
        operation = self._compiled.as_dict().get('operation')
        params = self._binds
        
        if self._binds:
            # bind the parameters into the template and build the payload
            payload = self._bind_params(operation['template'], params)

        else:
            payload = operation['template']

        self._compiled._compiled['operation']['payload'] = payload
        
    def _bind_params(self, template: dict, params: dict) -> dict:
        payload = copy.deepcopy(template)
        properties = payload.get('properties')
        payload_properties = {}
        for pname, pvalue in params.items():
            # params = {'student_id': 123456, 'name': 'Galileo Galilei', 'grade': 'A'}
            # pname = 'student_id', pvalue = 123456
            # properties = {{'student_id': {'number': ':student_id'}, {'name': {'title': {'text': {'context': ':name'}}}}, ...}
            col_type: TypeEngine = self._element.get_table()[pname].type_
            # col_type = Integer()

            bind_processor = col_type.bind_processor()
            bound_value = bind_processor(pvalue)
            # bound_value = {'number': 123456}
            
            payload_properties[pname] = bound_value
            # payload_properties = {'student_id': {'number': 123456}}

            properties.pop(pname)
            # properties = {{'name': {'title': {'text': {'context': ':name'}}}}, ...}
        
        if not properties:
            # params does not contain all binding values
            pass

        payload['properties'] = payload_properties
        #pdb.set_trace()
        return payload

    def _setup_cursor_result(self, cursor: Cursor) -> CursorResult:
            self._result = CursorResult(cursor, self._compiled.result_columns())
            return self._result
