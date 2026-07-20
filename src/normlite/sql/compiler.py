# sql/compiler.py
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
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional, Set

from normlite._constants import SpecialColumns
from normlite.exceptions import CompileError, StatementError, InvalidRequestError
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode
from normlite.sql._sentinels import VALUE_PLACEHOLDER
from normlite.sql.base import _CompileState, ClauseElement, SQLCompiler
from normlite.sql.dml import Delete, Update, OrderByClause
from normlite.sql.elements import _BindRole, Operator, OrderByExpression, ColumnElement, BinaryExpression
from normlite.sql.elements import _NoArg
from normlite.sql.elements import BooleanClauseList, UnaryExpression   
from normlite.sql.elements import BindParameter
from normlite.sql.schema import ReadOnlyColumnCollection, Column

if TYPE_CHECKING:
    from normlite.sql.ddl import CreateTable, DropTable, ReflectTable
    from normlite.sql.dml import Insert, Select, Join
    from normlite.sql.elements import UnaryExpression, BindParameter
    from normlite.sql.schema import Table

def compile_residual_filter(residual: BinaryExpression) -> dict:
    """Interim solution until issue #364
    
    Compile the residual expression for the right side of a JOIN statement.
    """
    if not isinstance(residual, BinaryExpression):
        raise InvalidRequestError("Only single-binary expressions supported, …")
    
    return {
        "property": residual.column.name,
        **_compile_type_filter(residual.column, residual.operator, residual.value)
    }

def compile_residual_sorts(residual_sorts: OrderByClause) -> list[dict]:
    """Interim solution until issue #365 — compile held-back right ORDER BY keys."""
    return [
        {"property": c.column.name, "direction": c.direction}
        for c in residual_sorts.clauses
    ]

def _compile_type_filter(
    column: ColumnElement,
    operator: Operator,
    bindparam: BindParameter
) -> dict:
    type_ = column.type_
    if type_ is not bindparam.type_:
        raise CompileError(
            f"""
                Type mismatch between column element: {column.name} 
                and bind parameter: {bindparam.key}:
                column element type: {type(type_).__name__}
                bind parameter type: {type(bindparam.type_).__name__}
                in binary expression: {operator}
            """
        )
    filter_type = type_.get_col_spec()
    filter_op = type_.supported_ops[operator]

    # process the bound value
    # IMPORTANT - Mimic bind paramters resolution with filter value processing
    # TypeEngine subclasses provide filter_value_processor() to process
    # the raw value into a filter value for JSON payloads: 
    # see ExecutionContext._resolve_bindparam()
    filter_raw = bindparam.callable_() if bindparam.callable_ else bindparam.value
    processor = type_.filter_value_processor()

    return {
        filter_type: {
            filter_op: processor(filter_raw) if processor else filter_raw
        }
    }

def _get_expression_parent_tables(expression: ClauseElement) -> Set[Table]:
    """Helper to recursively collect all parent tables corresponding to 
    the columns involved in the expression.
    
    .. note::
        Unrecognized nodes contribute no tables; the top-level guard in visit_select catches 
        only a fully-unattributable expression. 
        A new WHERE node type must extend this fold or it routes silently.
    """

    parents = set()

    if isinstance(expression, Column):
        parents.add(expression.parent)
    
    elif isinstance(expression, BinaryExpression):
        parents |= _get_expression_parent_tables(expression.column)
    
    elif isinstance(expression, UnaryExpression):
        parents |= _get_expression_parent_tables(expression.element)

    elif isinstance(expression, BooleanClauseList):
        for clause in expression.clauses:
            parents |= _get_expression_parent_tables(clause)
    
    elif isinstance(expression, OrderByClause):
        for clause in expression.clauses:
            parents |= _get_expression_parent_tables(clause)

    elif isinstance(expression, OrderByExpression):
        parents |= _get_expression_parent_tables(expression.column)
    
    return parents

class NotionCompiler(SQLCompiler):
    """Notion compiler for SQL statements.

    This class compiles SQL AST of statements into a Notion API compatible payload.

    .. admonition:: Examples
        :collapsible: open

        .. rubric:: Example 1: Create a new Notion database

        .. code-block:: python

            # create new Notion database for the following Table object.
            metadata = MetaData()
            students = Table(
                'students',
                metadata,
                Column('id', Integer()),
                Column('name', String(is_title=True)),
                Column('grade', String()),
                Column('is_active', Boolean()),
                Column('started_on', Date())
            )

            ddl_stmt = CreateTable(students)
            compiled = ddl_stmt.compile(NotionCompiler())
            print(compiled.string)
            
            # this is the stringified version of the compiled object
            {
                "operation": {                                  # a dictionary specifying the Notion API 
                    "endpoint": "databases",                    
                    "request": "create",                        
                },
                "payload": {                                    # parameterized payload
                    "parent": {
                        "type": "page_id",
                        "page_id": ":page_id"                   # bind param for parent page_id
                    },
                    "title": {                                  
                        "text": {
                        "content": ":table_name"                # bind param for table name 
                        }
                    },
                    "properties": {                             # the table schema
                        "id": {
                            "number": {
                                "format": "number"
                            }
                        },
                        "name": {
                            "title": {}
                        },
                        "grade": {
                            "rich_text": {}
                        },
                        "is_active": {
                            "checkbox": {}
                        },
                        "started_on": {
                            "date": {}
                        }
                    }
                }
            }

        .. rubric:: Example 2: Add a new page to a Notion database

        .. code-block:: python
        
            # create a new page belonging to the "students" database
            bind_params = {'student_id': 1234567, 'name': 'Galileo Galilei', 'grade': 'A'}
            stmt: Insert = insert(students).values(**expected_params)
            compiled = stmt.compile(NotionCompiler())

            # 
            {
                "operation": {
                    "endpoint": "pages",                        # INSERT corresponds to pages.create
                    "request": "create",
                    "template": {
                        "parent": {                             # parent database to which this page belongs to
                            "type": "database_id",
                            "database_id": "12345678-9090-0606-1111-123456789012"
                        },
                        "properties": {
                            "student_id": {
                                "number": ":student_id"         # named parameterized value :stundent_id
                            },
                            "name": {
                                "title": [
                                    {
                                        "text": {
                                            "content": ":name"
                                        }
                                    }
                                ]
                            },
                            "grade": {
                                "rich_text": [
                                    {
                                        "text": {
                                            "content": ":grade"
                                        }
                                    }
                                ]
                            }
                        }
                    }
                },
                "parameters": {
                    "student_id": 1234567,
                    "name": "Galileo Galilei",
                    "grade": "A"
                }
            }

        .. rubric:: Example 3: Check whether a given database exists

        .. code-block:: python

            metadata = MetaData()
            students = Table('students', metadata)
            ddl_stmt = HasTable(
                students,
                '66666666-6666-6666-6666-666666666666',             # tables_id
                'university'                                        # table_catalog   
            )
            compiled = ddl_stmt.compile(NotionCompiler())
            compile_dict = compiled.as_dict()

            {
                # operation describes endpoint, request, and template
                # the template uses named parameters which are bound at execution time
                'operation': {
                    'endpoint': 'databases',
                    'request': 'query',
                    'template': {
                        'database_id': ':database_id',                    
                        'filter': {
                            'and': [
                                {
                                    'property': 'table_name',
                                    'title' : {
                                        'equals': ':table_name'
                                    }
                                },
                                {
                                    'property': 'table_catalog',
                                    'rich_text': {
                                        'equals': ':table_catalog'     
                                    }
                                }
                            ]
                        }
                    }
                }

                # bindings for the named parameters
                'parameters': {
                    'database_id': '12345678-9090-0606-1111-123456789012',
                    'table_name': 'students',
                    'table_catalog': 'university'

                }

                # this operation returns the oid of the database found
                'result_columns': ['_no_id']
            }
    """

    def __init__(self):
        self._compiler_state = None
        self._bind_counter = 0

    def construct_params(
        self,
        params: Optional[dict] = None,
        group: Optional[int] = None
    ) -> dict[str, Any]:
        """Inject values to the execution binds from compile time.
        
        This methods constructs a dictionary with values computed based on the bind parameter role.
        if ``params`` is supplied, it is merged into the values known at compile time (e. g., from a 
        ``VALUES`` clause) and it is used to resolve the bind parameter values.

        .. versionadded:: 0.9.0
        """

        statement = self._compiler_state.stmt
        bindparams = self._compiler_state.execution_binds
        resolved = {}

        base: dict[str, Any] = {}

        if (statement.is_insert or statement.is_update) and not statement._has_multi_parameters:
            base = statement._single_parameters or {}

        if params:
            base = {**base, **params}

        for key, bindparam in bindparams.items():

            # user supplied values INSERT+SELECT
            if bindparam.role in (_BindRole.COLUMN_VALUE, _BindRole.COLUMN_FILTER):
                if key in base:
                    resolved[key] = base[key]
                elif bindparam.value is not _NoArg.NO_ARG:
                    resolved[key] = bindparam.value
                else:
                    err_msg = (
                        f"A value is required for bind parameter '{key}' (in parameter group {group})"
                        if group is not None
                        else f"A value is required for bind parameter '{key}'"
                    )
                    raise StatementError(err_msg)

            # SYSTEM PARAM
            elif bindparam.role == _BindRole.DBAPI_PARAM:
                if bindparam.value is None or bindparam.value is _NoArg.NO_ARG:
                    raise StatementError(
                        f"Internal bind parameter '{key}' has no value"
                    )
                resolved[key] = bindparam.value

            else:
                raise StatementError(
                    f"Unknown bind role for parameter '{key}'"
                )

        # --- EXTRA KEYS VALIDATION ---
        if params:
            extra_keys = set(params.keys()) - set(bindparams.keys())
            if extra_keys:
                err_msg = (
                    f"Unknown parameter(s): {extra_keys} (in parameter group {group})"
                    if group is not None
                    else f"Unknown parameter(s): {extra_keys}"
                )
                raise StatementError(err_msg)

        return resolved

    def visit_create_table(self, ddl_stmt: CreateTable) -> dict:
        """Compile a ``CREATE TABLE`` statement.
        
        This visit method compiles the DDL :class:`normlite.sql.ddl.CreateTable` construct into the 
        corresponding Notion payload.

        .. versionchanged:: 0.12.0
            This method now supports compilation for data sources as of Notion API 2025-09-03.

        .. versionchanged:: 0.8.0
            This method now produces a fully parameterized template dictionary and 
            provides the binds in the parameter dictionary.
            It fixes also the returning columns to be set to the **meta columns**.

        Args:
            ddl_stmt (CreateTable): The DDL statement to be compiled.

        Returns:
            dict: The compiled object as dictionary.
        """
        self._compiler_state.is_ddl = True
        self._compiler_state.stmt = ddl_stmt
        payload = {}
        stmt_table = ddl_stmt.get_table()
        
        if stmt_table._db_parent_id is None:
            # changed back to CompileError:
            # normlite compiler is a payload builder, not a "real" SQL compiler
            # so it must enforce payload schema invariants such as
            # database_id not being None 
            raise CompileError(f'Table: {stmt_table.name} has been previously neither created or reflected.')

        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            # emit code for parent object
            parent_id_key = self._add_bindparam(
                BindParameter(
                    key='page_id', 
                    value=stmt_table._db_parent_id, 
                )
            )

            payload['parent'] = {
                'type': 'page_id',
                'page_id': f':{parent_id_key}'
            }
        
           # emit code for title object
            title_key = self._add_bindparam(
                BindParameter(
                    key='table_name',
                    value=stmt_table.name
                )
            )

            payload['title'] = [{
                'text': {
                    'content': f':{title_key}'
                }
            }]

        # emit code for properties object
        properties = self._compile_table_columns(
            stmt_table.user_columns
        )
        payload["initial_data_source"] = {
            "properties": properties
        } 
        
        self._compiler_state.result_columns = [
            col.name
            for col in stmt_table.c
        ]

        operation = dict(endpoint='databases', request='create')
        # columns to be returned are meta columns!!!
        self._compiler_state.result_columns = [
            DBAPITypeCode.META_COL_NAME, 
            DBAPITypeCode.META_COL_TYPE, 
            DBAPITypeCode.META_COL_ID, 
            DBAPITypeCode.META_COL_VALUE
        ]
        
        return {'operation': operation, 'payload': payload}  
    
    def visit_drop_table(self, ddl_stmt: DropTable) -> dict:
        self._compiler_state.is_ddl = True
        self._compiler_state.stmt = ddl_stmt
        path_params = {}
        payload = {}
        stmt_table = ddl_stmt.get_table()
        database_id = stmt_table.get_oid()

        if database_id is None:
            # changed back to CompileError:
            # normlite compiler is a payload builder, not a "real" SQL compiler
            # so it must enforce payload schema invariants such as
            # database_id not being None 
            raise CompileError(f'Table: {stmt_table.name} has been previously neither created or reflected.')
        
        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            db_id_key = self._add_bindparam(
                BindParameter(
                    key='database_id',
                    value=database_id
                )
            )
            path_params['database_id'] = f':{db_id_key}'

            in_trash_key = self._add_bindparam(
                BindParameter(
                    key='in_trash',
                    value=True
                )
            )
            payload['in_trash'] = f':{in_trash_key}'

        operation = dict(endpoint='databases', request='update')

        return {
            'operation': operation, 
            'path_params': path_params,
            'payload': payload
        }  
        
    def visit_reflect_table(self, ddl_stmt: ReflectTable) -> dict:
        self._compiler_state.is_ddl = True
        self._compiler_state.stmt = ddl_stmt
        path_params = {}
        stmt_table = ddl_stmt.get_table()
        data_source_id = stmt_table.get_data_source_id()

        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            db_id_key = self._add_bindparam(
                BindParameter(
                    key='data_source_id',
                    value=data_source_id
                )
            )
            path_params['data_source_id'] = f':{db_id_key}'

        operation = dict(endpoint = 'data_sources', request='retrieve')
        return {
            'operation': operation, 
            'path_params': path_params,
        }

    def visit_insert(self, insert: Insert) -> dict:
        """Compile the ``INSERT`` DML statement.

        This visit method compiles the DML :class:`normlite.sql.dml.Insert` construct into a Notion payload 
        for the pages.create request.

        Raises:
            CompileError: If the RETURNING clause does not include the system column "object_id"

        Args:
            insert (Insert): The DML statement to be compiled.

        .. versionchanged:: 0.12.0
            This version adds support for emitting code compatible with Notion 2025-09-03

        .. versionchanged:: 0.9.0
            This version adds full support for INSERT ... RETURNING.
            It initializes the :attr:`normlite.sql.base.CompilerState.result_columns



        .. versionchanged:: 0.8.0
            This method extends parameterization via named argument also to the "database_id" key. Thus, the "parameters" dictionary 
            now contains the binding for this key.

        .. versionadded:: 0.7.0
            Initial version supports binding of named arguments for insert values.

        Returns:
            dict: The dictionary containing the compiled object.
        """
        self._compiler_state.is_insert = True
        payload = {}
        db_id_key = None

        # select the user columns to be included in the returned rows
        self._compiler_state.result_columns = [
            col.name 
            for col in insert._returning
        ]

        if insert._values is None:
            # create a mapping for all user columns with dummy values
            placeholders = {
                col.name: VALUE_PLACEHOLDER
                for col in insert.get_table().user_columns
            }
            if insert._has_multi_parameters:
                multi_placehoders = [placeholders] * len(insert._multi_parameters)
                insert = insert.values(multi_placehoders)
            else:
                insert = insert.values(**placeholders)

        # IMPORTANT: initialize the stmt in the compiler state after the values check
        # .values() is generative and returns a new instance
        self._compiler_state.stmt = insert

        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            db_id_key = self._add_bindparam(
                BindParameter(
                    key='data_source_id', 
                    value=insert._table.get_data_source_id(), 
                )
            )

            payload['parent'] = {
                'type': 'data_source_id',
                'data_source_id': f':{db_id_key}'
            }

        with self._compiling(new_state=_CompileState.COMPILING_VALUES):
            payload['properties'] = self._compile_insert_update_values(insert._values)

        operation = dict(endpoint='pages', request='create')
        return {'operation': operation, 'payload': payload}  

    def visit_order_by_clause(self, clause: OrderByClause) -> dict:
        if not clause.clauses:
            return {}

        sorts = []
        for expr in clause.clauses:
            compiled = expr._compiler_dispatch(self)
            sorts.append(compiled)

        return sorts
    
    def visit_order_by_expression(self, expr: OrderByExpression) -> dict:
        column = expr.column

        if not isinstance(column, ColumnElement):
            raise CompileError(
                f"""
                    order_by() only supports column elements,
                    supplied: {column.__class__.__name__}
                """
            )
        
        return {
            'property': column.name,
            'direction': expr.direction
        }

    def visit_join(self, join: Join) -> dict:
        return {
            "left": join.left.name,
            "right": join.right.name,
            "onclause": join.onclause.name,
            "isouter": join.isouter
        }

    def visit_select(self, select: Select) -> dict:
        self._compiler_state.is_select = select.is_select
        self._compiler_state.stmt = select
        self._compiler_state.result_columns = []

        operation = dict(endpoint='data_sources', request='query')
        compiled_dict = {
            'operation': operation, 
        }

        path_params = {}
        query_params = {}
        payload = {
            'page_size': 100,        # Notion imposed max page size
        }

        # add a new top-level 'joins' key to store the joins, if any
        joins = [j._compiler_dispatch(self) for j in select._joins]
        if joins:
            # emit only when non-empty
            compiled_dict["joins"] = joins

        table = select.get_table()
        if table is None:
            # aggregate with no operand column (a columnless COUNT(*)) and no explicit
            # select_from(): the FROM is unresolvable. Fail loud at compile, like 
            # visit_update's missing clause guard, rather that crashing on None.get_iod()
            raise CompileError(
                "Aggregate select has no FROM: columnless func.count() (COUNT(*)) "
                "must be anchored with select_from(table)"
            )

        data_source_id = table.get_data_source_id()
        if data_source_id is None:
            raise CompileError(f'Table: {table.name} has not been previously reflected.')
        
        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            db_id_key = self._add_bindparam(
                BindParameter(
                    key='data_source_id',
                    value=data_source_id
                )
            )
            path_params['data_source_id'] = f':{db_id_key}'
            compiled_dict["path_params"] = path_params 

     
        if select._whereclause.has_expression():
            expression = select._whereclause.expression
            parent_tables = _get_expression_parent_tables(expression)
            if not parent_tables:
                # A WHERE expression is present but the router could attribute it
                # to no source table. This is not a routing outcome but a failure
                # to route — most likely an unsupported expression node type.
                # Fail loudly at compile time rather than silently dropping the
                # filter (which would return wrong rows with no error).
                raise CompileError(
                    f"Cannot route WHERE expression: no source table could be "
                    f"determined for {type(expression).__name__}."
                )
            if parent_tables <= {select._table, select._right}:
                with self._compiling(new_state=_CompileState.COMPILING_WHERE):
                    # emit the JSON code for the filter object of the query
                    # in the right context
                    if (
                        select._joins
                        and isinstance(expression, BooleanClauseList)
                        and expression.operator == "and"
                        and parent_tables == {select._table, select._right}
                    ):
                        # Compound AND spanning both join sides: split per-clause.
                        # Left-only conjuncts narrow phase-1 to a SUPERSET of the
                        # answer, so push them into payload['filter']; the remaining
                        # conjuncts are held back as the residual AST for client-side
                        # evaluation after the merge. (See #311, #363.)
                        left_conjuncts = [
                            clause._compiler_dispatch(self)
                            for clause in expression.clauses
                            if _get_expression_parent_tables(clause) == {select._table}
                        ]
                        if left_conjuncts:
                            payload['filter'] = (
                                left_conjuncts[0] if len(left_conjuncts) == 1
                                else {"and": left_conjuncts}
                            )

                        # Hold the right-side conjuncts as raw AST for client-side
                        # evaluation after the merge; do NOT dispatch them (that would
                        # register unconsumed binds — see #363).
                        right_clauses = [
                            clause
                            for clause in expression.clauses
                            if _get_expression_parent_tables(clause) != {select._table}
                        ]
                        if right_clauses:
                            self.planning_context.residual_where = (
                                right_clauses[0] if len(right_clauses) == 1
                                else BooleanClauseList("and", right_clauses)
                            )
                    else:
                        if parent_tables == {select._table}:
                            # for the left table, add "filter" to the payload for the databases.query
                            payload['filter'] = expression._compiler_dispatch(self)
                        else:
                            # for the right table, hold the residual as raw AST for
                            # client-side evaluation after the merge; do NOT dispatch it
                            # (that would register an unconsumed bind — see #363).
                            self.planning_context.residual_where = expression
        
        projection = self._compiler_state.stmt._projection

        if select._is_aggregate:
            raw_cols = self._compiler_state.stmt._raw_columns
            operand_names = [f.column.name for f in raw_cols if f.column is not None]
            # a pure COUNT(*) has no operand columns; fall back fetching object_id so
            # each matched page still yields one row for reduce() to count
            self._compiler_state.fetch_columns = operand_names or ["object_id"]
        
        else:
            if projection:
                # use select projections for the result columns
                self._compiler_state.fetch_columns = [
                    col.name
                    for col in projection
                    if col.parent is select._table  # join path supplies only its left-owned projection
                ]

                if select._joins:
                    # add the onclause column name to the set of columns to be fetched
                    # this ensures it is encoded in the filter properties
                    self._compiler_state.fetch_columns.append(
                        compiled_dict["joins"][0]["onclause"]
                    )

                self._compiler_state.result_columns = [
                    col 
                    for col in self._compiler_state.fetch_columns
                    if col not in SpecialColumns
                ]

                uc_names = [uc.name for uc in select._table.uc]

                if (
                    self._compiler_state.result_columns and
                    len(self._compiler_state.result_columns) < len(uc_names)
                ):
                    # add the filter_properties query parameters only
                    # if any user column was projected and the projected user colums are 
                    # a subset of all user columns
                    query_params['filter_properties'] = self._compiler_state.result_columns

        if select._order_by.has_expression():
            order_by_clause = select._order_by
            parent_tables = _get_expression_parent_tables(order_by_clause)
            if not parent_tables:
                # An ORDER BY expression is present but the router could attribute it
                # to no source table. This is not a routing outcome but a failure
                # to route — most likely an unsupported expression node type.
                # Fail loudly at compile time rather than silently dropping the
                # sort (which would return rows in the wrong order with no error).
                raise CompileError(
                    f"Cannot route ORDER BY expression: no source table could be "
                    f"determined for {type(order_by_clause).__name__}."
                )
            
            # Sort pushability is POSITIONAL: only the LEADING RUN of left-table
            # keys can ride in phase-1 (databases.query sorts by left-table
            # properties only). Stop the prefix at the first key that isn't purely
            # left-table — a right-side (or mixed) primary key makes the remaining
            # sort a client-side concern. This unifies the single-table case (every
            # key is left-table, so the whole sort is pushed) with the join case.
            sorts_obj = []
            for clause in order_by_clause.clauses:
                if _get_expression_parent_tables(clause) != {select._table}:
                    break
                sorts_obj.append(clause._compiler_dispatch(self))

            right_clauses = tuple(
                clause
                for clause in order_by_clause.clauses
                if _get_expression_parent_tables(clause) != {select._table}
            )

            if right_clauses:
                self.planning_context.residual_sorts = OrderByClause(right_clauses)

            if sorts_obj:
                payload['sorts'] = sorts_obj

        compiled_dict ["payload"] = payload
        
        if query_params:           
            compiled_dict['query_params'] = query_params

        return compiled_dict
    
    def visit_delete(self, delete: Delete):
        self._compiler_state.is_delete = delete.is_delete
        self._compiler_state.stmt = delete
 
        operation = dict(endpoint='data_sources', request='query')
        path_params = {}
        payload = {
            'page_size': 100,        # Notion imposed max page size
        }
 
        # select the user columns to be included in the returned rows
        self._compiler_state.result_columns = [
            col.name 
            for col in delete._returning
        ]

        table = delete.get_table()
        data_source_id = table.get_data_source_id()
        if data_source_id is None:
            raise CompileError(f'Table: {table.name} has not been previously reflected.')
        
        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            db_id_key = self._add_bindparam(
                BindParameter(
                    key='data_source_id',
                    value=data_source_id
                )
            )
            path_params['data_source_id'] = f':{db_id_key}'

        if delete._whereclause.has_expression():
            with self._compiling(new_state=_CompileState.COMPILING_WHERE):
                # emit the JSON code for the filter object of the query
                # in the right context
                filter_obj = delete._whereclause.expression._compiler_dispatch(self)
                payload['filter'] = filter_obj

        compiled_dict = {
            'operation': operation, 
            'path_params': path_params, 
            'payload': payload
        }
        return compiled_dict

    def visit_update(self, update: Update) -> dict:
        self._compiler_state.is_update = True
        self._compiler_state.stmt = update

        if update._values is None:
            raise CompileError(
                "update() requires .values() to be called before compilation"
            )

        self._compiler_state.result_columns = [
            col.name for col in update._returning
        ]

        operation = dict(endpoint='data_sources', request='query')
        path_params = {}
        payload = {
            'page_size': 100,
        }

        table = update.get_table()
        data_source_id = table.get_data_source_id()
        if data_source_id is None:
            raise CompileError(f'Table: {table.name} has not been previously reflected.')
        
        with self._compiling(new_state=_CompileState.COMPILING_DBAPI_PARAM):
            db_id_key = self._add_bindparam(
                BindParameter(
                    key='data_source_id',
                    value=data_source_id
                )
            )
            path_params['data_source_id'] = f':{db_id_key}'

        if update._whereclause.has_expression():
            with self._compiling(new_state=_CompileState.COMPILING_WHERE):
                filter_obj = update._whereclause.expression._compiler_dispatch(self)
                payload['filter'] = filter_obj

        with self._compiling(new_state=_CompileState.COMPILING_VALUES):
            update_payload = self._compile_update_values(update._values)

        return {
            'operation': operation,
            'path_params': path_params,
            'payload': payload,
            'update_payload': update_payload,
        }

    def _compile_update_values(self, values: dict) -> dict:
        properties = {}
        for col_name, bindparam in values.items():
            param_key = self._add_bindparam(bindparam, col_name)
            properties[col_name] = f':{param_key}'
        return properties

    def visit_binary_expression(self, expression: BinaryExpression) -> dict:
        return {
            "property": expression.column.name,
            **self._compile_type_filter(
                expression.column,
                expression.operator,
                expression.value
            )
        }
    
    def visit_unary_expression(self, expression: UnaryExpression) -> dict:
        if expression.operator != "not":
            raise NotImplementedError(
                f"Unsupported unary operator: {expression.operator}"
            )

        inner = expression.element._compiler_dispatch(self)

        return {
            "not": inner
        }
    
    def visit_boolean_clause_list(self, expression: BooleanClauseList) -> dict:
        clauses = [
            clause._compiler_dispatch(self)
            for clause in expression.clauses
        ]
        return {
            expression.operator: clauses
        }

    def _next_bind_key(self) -> str:
        key = f"param_{self._bind_counter}"
        self._bind_counter += 1
        return key

    def _add_bindparam(
            self, 
            bindparam: BindParameter, 
            column_name: Optional[str] = None,
        ) -> str:
        """Assign keys, types and roles to the bind parameter argument based on the actual compilation phase.
        
        At compilation phase, bind parameters' keys, types and roles only are known.
        The values are associated **only** at execution time by :meth:`construct_params`.
        This method assigns the bind parameters attributes according to the following scheme:

        * ``WHERE`` clause: key is an auto-generated anonymous parameter "params_n", type is the column's type and role is
            :attr:`normlite.sql.base._CompileState.COLUMN_FILTER` (meaning the ``filter_value_processor()`` 
            shall be used).

        * ``VALUES`` clause: key is the column's name, type is the column's type and role is
            :attr:`normlite.sql.base._CompileState.COLUMN_VALUE` (meaning the ``bind_processor()`` 
            shall be used). 

        * DBAPI parameters: the already assigned key remains, only the bind role is assigned to 
            :attr:`normlite.sql.base._CompileState.DBAPI_PARAM` (meaning the value shall be used).
        """

        if bindparam.role != _BindRole.NO_BINDROLE:
            raise CompileError("BindParameter role already assigned")

        state = self._compiler_state.compile_state

        if state == _CompileState.COMPILING_WHERE:
            # SELECT / WHERE: autogenerated key
            if column_name is not None:
                raise CompileError('Bind parameters in a where clause shall not have a column name.')
            
            key = self._next_bind_key()
            bindparam.key = key
            bindparam.role = _BindRole.COLUMN_FILTER
        
        elif state == _CompileState.COMPILING_VALUES:
           # INSERT / UPDATE: key must be column name
            if column_name is None:
                raise CompileError(
                    "Bind parameters in insert/update require a column name"
                )
            key = column_name
            bindparam.key = key
            stmt_table = getattr(self._compiler_state.stmt, '_table', None)
            if stmt_table is None:
                stmt = self._compiler_state.stmt
                raise CompileError(
                    f"""
                        Expected an insert or update statement, 
                        received: {repr(stmt)}
                    """
                )

            try:
                column = stmt_table.c[column_name]
                bindparam.type_ = column.type_
                bindparam.role = _BindRole.COLUMN_VALUE
            except KeyError as ke:
                raise CompileError(
                    f'Column name: {ke.args[0]} not found in table: {stmt_table.name}'
                )

        elif state == _CompileState.COMPILING_DBAPI_PARAM:
            # DBAPI parameter: use the already available key
            if bindparam.key is None:
                raise CompileError('Bind parameter supplied for DBAPI has a None key.')

            key = bindparam.key
            bindparam.role = _BindRole.DBAPI_PARAM
            
        else:
            stmt = self._compiler_state.stmt
            raise CompileError(
                f"""
                    Invalid compiler state: {state}, 
                    while compiling statement: {repr(stmt)}.
                """
            )

        self._compiler_state.execution_binds[key] = bindparam
        return key

    def _compile_type_filter(
            self, 
            column: ColumnElement, 
            operator: Operator, 
            bindparam: BindParameter
    ) -> dict:
        type_ = column.type_
        if type_ is not bindparam.type_:
            raise CompileError(
                f"""
                    Type mismatch between column element: {column.name} 
                    and bind parameter: {bindparam.key}:
                    column element type: {type_.__class__.__name__}
                    bind parameter type: {bindparam.type_.__class__.__name__}
                    in binary expression: {operator}
                """
            )
        notion_type = type_.get_col_spec()
        notion_op = type_.supported_ops[operator]

        # allocate placeholder
        key = self._add_bindparam(bindparam)

        # IMPORTANT: No processing here.
        # Compiler must stay syntactic, binding (and processing) is done at execution time
        return {
            notion_type: {
                notion_op: f':{key}'
            }
        }
    
    def _compile_insert_update_values(self, values: dict) -> dict:
        properties = {}
        stmt_table = self._compiler_state.stmt._table
        user_cols = stmt_table.user_columns
        uc_names = set([c.name for c in user_cols])
        val_names = set(values.keys())
        remaining = uc_names - val_names

        if remaining:
            missing = ", ".join(remaining)
            format_val = "Values" if len(remaining) > 1 else "Value"
            format_col = "columns" if len(remaining) > 1 else "column"
            raise CompileError(
                f"{format_val} for {format_col} '{missing}' not supplied in INSERT statement"
            )

        # reorder the keys in values according to the order in user_cols
        ordered_values = {}
        try:
            ordered_values = {
                col.name: values[col.name]
                for col in user_cols
            }
        except KeyError as ke:
            raise CompileError(
                f'No value for column "{ke.args[0]}" found in values {values}'
            ) from ke

        for col, bindparam in zip(user_cols, ordered_values.values()):
            param_key = self._add_bindparam(bindparam, col.name)
            properties[col.name] = f':{param_key}'

        return properties
    
    def _compile_table_columns(self, user_cols: ReadOnlyColumnCollection) -> dict:
        from normlite.sql.type_api import Relation

        # resolve oids for the all referenced columns         
        stmt_table: Table = self._compiler_state.stmt._table
        referenced_ids = {
            c.column.name: c.reftable.get_data_source_id()
            for c in stmt_table.foreign_keys
        }

        # construct the properties payload
        properties = {}
        for col in user_cols:
            prop_val = col.type_.get_notion_spec()
            if isinstance(col.type_, Relation):
                # inject the data_source_id into the Notion spec for Relation objects
                # ref_oid now contains col.name's data_source_id
                ref_oid = referenced_ids.get(col.name)
                if ref_oid is None:
                    raise CompileError(f"Relation column '{col.name}' on table '{stmt_table.name}' has no ForeignKeyConstraint registered")
                
                prop_val["relation"]["data_source_id"] = referenced_ids[col.name]

            properties[col.name] = prop_val    

        return properties
    
    @contextmanager
    def _compiling(self, new_state: _CompileState):
        prev = self._compiler_state.compile_state
        self._compiler_state.compile_state = new_state
        try:
            yield
        finally:
            self._compiler_state.compile_state = prev
