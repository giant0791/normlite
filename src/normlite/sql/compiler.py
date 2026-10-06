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
from typing import TYPE_CHECKING, Any, Literal, NoReturn, Optional, Set

from normlite._constants import SpecialColumns
from normlite.exceptions import CompileError, StatementError, UnsupportedCompilationError
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

_NOTION_MAX_FILTER_DEPTH = 2

# Private refusal reasons for the aggregate SELECT, DELETE and UPDATE.
# The two refusal helpers below use these strings, and the tests match on them.
# They are REMOVED, not relaxed: DELETE and UPDATE with #397, the aggregate with #399.
_FILTER_MAX_DEPTH_EXCEEDED = "It exceeds Notion filter max depth"
_UNPUSHABLE_FILTER_TERM = "Notion filter syntax does not support negated terms or null comparison values"

# Refusal helpers.
# The compiler calls them to refuse a WHERE clause that Notion cannot evaluate exactly
# AND that no client-side recheck corrects.
# They are REMOVED with the last refusing statement: #397 for DML, #399 for the aggregate.
# The helpers add the periods and select the consequence by the _RefusingStmt key.
_RefusingStmt = Literal["An aggregate SELECT", "UPDATE", "DELETE"]

_NO_RECHECK_CONSEQUENCE: dict[_RefusingStmt, str] = {
    "DELETE": "DELETE bypasses the query plan, so its WHERE has no recheck (#397)",
    "UPDATE": "UPDATE bypasses the query plan, so its WHERE has no recheck (#397)",
    "An aggregate SELECT": "The aggregate would reduce rows this WHERE excludes (#399)",
}


def _raise_if_filter_max_depth_exceeded(
    stmt: _RefusingStmt,
    filter_obj: dict[str, Any],
    remedy: str,
) -> None:
    """Refuse ``filter_obj`` if it nests deeper than Notion accepts.

    Only a path with no recheck calls this. A plain ``SELECT`` prunes instead
    (:func:`_prune`), because its recheck restores the rows the prune admits.

    Args:
        stmt: The refusing statement. It selects the consequence in the message.
        filter_obj: The emitted filter object, before any prune.
        remedy: What the user can do instead. The caller supplies it.

    Raises:
        CompileError: If ``_depth(filter_obj)`` exceeds
            ``_NOTION_MAX_FILTER_DEPTH``. The message holds
            ``_FILTER_MAX_DEPTH_EXCEEDED``.

    .. versionadded:: 0.13.0
    """
    if _depth(filter_obj) > _NOTION_MAX_FILTER_DEPTH:
        raise CompileError(
            f"{stmt} cannot compile this WHERE clause: {_FILTER_MAX_DEPTH_EXCEEDED}. {_NO_RECHECK_CONSEQUENCE[stmt]}. {remedy}."
        )


def _raise_if_filter_term_unpushable(
    stmt: _RefusingStmt,
    where_expression: ColumnElement,
    remedy: str,
) -> None:
    """Refuse ``where_expression`` if the emitted filter would drop a term.

    Args:
        stmt: The refusing statement. It selects the consequence in the message.
        where_expression: The WHERE expression, before emission.
        remedy: What the user can do instead. The caller supplies it.

    Raises:
        CompileError: If :func:`_all_terms_pushable` returns ``False``. The
            message holds ``_UNPUSHABLE_FILTER_TERM``.

    .. versionadded:: 0.13.0
    """
    if not _all_terms_pushable(where_expression):
        raise CompileError(
            f"{stmt} refuses this WHERE clause: {_UNPUSHABLE_FILTER_TERM}. {_NO_RECHECK_CONSEQUENCE[stmt]}. {remedy}."
        )

def compile_residual_sorts(residual_sorts: OrderByClause) -> list[dict]:
    """Interim solution until issue #365 — compile held-back right ORDER BY keys."""
    return [
        {"property": c.column.name, "direction": c.direction}
        for c in residual_sorts.clauses
    ]

def _get_expression_columns(expression: ClauseElement) -> Set[Column]:
    """Helper to recursively collect all columns involved in the expression.

    Returns an unordered set. A caller that needs a stable column order must
    impose one itself -- ``Planner.plan`` sorts by name when it widens
    ``execution_names``, because a set's iteration order varies per process
    and would otherwise make the ``Scan``'s schema vary run to run.

    .. note::
        Unrecognized nodes contribute no columns; the top-level guard in visit_select catches
        only a fully-unattributable expression.
        A new WHERE node type must extend this fold or it routes silently.
    """
    columns = set()

    if isinstance(expression, Column):
        columns.add(expression)

    elif isinstance(expression, BinaryExpression):
        columns |= _get_expression_columns(expression.column)

    elif isinstance(expression, UnaryExpression):
        columns |= _get_expression_columns(expression.element)

    elif isinstance(expression, BooleanClauseList):
        for clause in expression.clauses:
            columns |= _get_expression_columns(clause)

    elif isinstance(expression, OrderByClause):
        for clause in expression.clauses:
            columns |= _get_expression_columns(clause)

    elif isinstance(expression, OrderByExpression):
        columns |= _get_expression_columns(expression.column)

    return columns

def _get_expression_parent_tables(expression: ClauseElement) -> Set[Table]:
    """Helper to recursively collect all parent tables corresponding to
    the columns involved in the expression.

    .. note::
        Unrecognized nodes contribute no tables; the top-level guard in visit_select catches
        only a fully-unattributable expression.
        A new WHERE node type must extend this fold or it routes silently.
    """

    return {c.parent for c in _get_expression_columns(expression)}

def _is_pushable(node: ColumnElement, strict: bool = False) -> bool:
    """``True`` if the node is pushable into the Notion API filter.

    Helper that implements the following rule:
        - **leaf** - pushable, unless it compares a ``None`` literal under ``==`` or ``!=``.
        - **not** - never pushable.
        - **and** - lax: drop unpushable children, push the survivors.
          strict: any unpushable child makes the whole **and** unpushable.
        - **or** - if any child is unpushable, the whole or is unpushable. Push nothing for it.

    Args:
        node: The WHERE subtree to judge.
        strict: Selects which question is asked of the WHOLE subtree, not just of
            ``node``, and is therefore carried down every recursive call. ``False``
            (the default, used by ``SELECT``) asks *"can anything be pushed?"*;
            ``True`` (used by the DML gate) asks *"can EVERYTHING be pushed?"*.

    Returns:
        ``True`` if the node is pushable under the requested mode.

    Raises:
        NotImplementedError: If the node is not a recognized WHERE node type. Falling
            off the end loudly is deliberate: a node silently judged pushable would
            be dispatched and emitted as a filter.

    .. note::
        **Why two modes exist.** The two operators are asymmetric only under **and**,
        and the asymmetry is not stylistic. Dropping a conjunct WEAKENS the filter, so
        the backend returns a SUPERSET of the answer. On the ``SELECT`` path that is
        sound, because the push never decides -- the client-side recheck does
        (ADR-0022) -- so lax is correct there. ``DELETE``/``UPDATE`` have no recheck
        (#397): the pushed filter IS the decision, and a dropped conjunct is a row
        mutated that the WHERE excludes. Hence strict.

        **or** applies ``all()`` in both modes, but ``strict`` still has to reach it:
        the mode selects the question put to the CHILDREN, and an **and** nested under
        an **or** must be judged in the caller's mode. A recursive call that omits
        ``strict`` silently reverts the whole subtree below it to lax.

    .. seealso::

        Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.

    .. versionchanged:: 0.13.0
        Added ``strict`` for the DML gate.
    """

    if isinstance(node, BinaryExpression):
        if node.operator in (Operator.EQ, Operator.NE):
            # a None literal under == or != is unpushable because Notion rejects it with HTTP 400
            return node.value.effective_value is not None
        return True

    if isinstance(node, UnaryExpression):
        # not - never pushable
        return False

    if isinstance(node, BooleanClauseList):
        if node.operator == "and":
            if strict:
                return all(_is_pushable(c, strict=strict) for c in node.clauses)

            return any(_is_pushable(c, strict=strict) for c in node.clauses)

        if node.operator == "or":
            return all(_is_pushable(c, strict=strict) for c in node.clauses)

    raise NotImplementedError(f"Unknown or unsupported expression node: '{type(node).__name__}'")

def _all_terms_pushable(node: ColumnElement) -> bool:
    """``True`` only if EVERY term in ``node`` is pushable.

    The gate condition for a ``DELETE``/``UPDATE`` WHERE clause. Where
    :func:`_is_pushable` in its default (lax) mode admits an **and** with a single
    pushable child -- because ``SELECT`` may drop the rest and let the recheck decide
    (ADR-0022) -- this admits it only if no child would be dropped at all. On the DML
    path a dropped conjunct is not a weakened hint, it is a deleted or overwritten row.

    .. warning::
        ``True`` does NOT mean the pushed filter is EXACTLY equivalent to the WHERE.
        It means no term is *discarded*. A term may still be pushed as a Notion filter
        that over-matches, because Notion's negative operators are the boolean
        complement over a domain that INCLUDES the empty cell, while SQL's evaluate to
        UNKNOWN against NULL and the WHERE policy drops them.

        The ``!=`` instance of that is **closed**: ``id != 1`` used to emit a bare
        ``number.does_not_equal``, which also matched a valueless ``id``, so
        ``DELETE WHERE id != 1`` passed this gate and still removed a row SQL keeps
        (#384, ADR-0022). :meth:`NotionCompiler.visit_binary_expression` now conjoins
        ``is_not_empty`` onto that leaf and the push is exact (#383).

        ``NOT IN`` is **exact**, measured against the live API (2026-10-04,
        ``src/tools/notion_probe.py``). It emits ``does_not_contain``, and Notion
        matches every ``[]`` cell, for both ``title`` and ``relation``. SQL and
        eval3 also say TRUE there: ``[]`` is a PRESENT value, not a valueless
        cell (CONTEXT.md, "Raw cell <-> decoded NULL"), and Notion stores no
        valueless text or relation cell. So no repair applies. Measure every new
        negative operator the same way: reasoning by analogy across Notion's
        types has been wrong here before (ADR-0019).

    Args:
        node: The WHERE expression of the DML statement.

    Returns:
        ``True`` if no term of ``node`` would be dropped when the filter is emitted.

    .. seealso::

        Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.

        Issue `DELETE/UPDATE bypass the query plan, so their WHERE has no recheck <https://github.com/giant0791/normlite/issues/397>`_,
        which retires this gate by giving DML a recheck -- at which point the refusal
        is REMOVED, not relaxed.

    .. versionadded:: 0.13.0
    """

    return _is_pushable(node, strict=True)

def _depth(node: Any) -> int:
    """Count nested compound objects, the root compound being level 1.

    Notion accepts at most 2 (confirmed), so a filter measuring 3 is an
    HTTP 400 (#383).

    Args:
        node: A filter object, or any part of one.

    Returns:
        The number of compound levels on the deepest path. A leaf, or a value
        that is not a ``dict``, measures 0.

    .. versionadded:: 0.13.0
    """
    if not isinstance(node, dict):
        return 0

    for op in ("and", "or"):
        if op in node:
            return 1 + max((_depth(child) for child in node[op]), default=0)

    return 0

def _prune(node: Any, parent_op: Optional[str] = None, level: int = 1) -> Any:
    """Weaken ``node`` until no compound sits below level 2.

    The Notion API accepts at most 2 levels of compound nesting. The only
    compound operators are ``and`` and ``or``; Notion has no ``not``. A
    compound at level 3 or deeper (the violator) has one of two parents:

    * **or**: the violator is replaced by its first child, pruned again at
      the violator's level minus 1.
    * **and**: the violator is dropped.

    Both rules make the filter WEAKER, so the pushed filter returns a
    superset of the answer. Only a path with a recheck may call this
    (ADR-0022). DML and the aggregate refuse instead.

    Preconditions (the caller must guarantee both; ``_prune`` does not check them):

    * **Operators alternate.** No ``or`` sits directly under an ``or``, and no
      ``and`` under an ``and``. The replace rule depends on it: under an ``or``
      parent, the violator is an ``and``, and an ``and``'s first child is WEAKER
      than the ``and`` -- a superset, which the recheck re-narrows. If the
      violator were an ``or``, its first child would be STRONGER, and the prune
      would lose rows that no recheck can restore. :func:`_normalize_filter`
      splices same-operator nesting at emission, which is what makes this hold.
      This is also why the join site folds BEFORE it prunes. The depth bound
      rests on this precondition too.
    * **Every compound has at least 2 children.** This makes ``children[0]``
      safe. :func:`_normalize_filter` unwraps a one-child compound, so a
      well-formed input never holds one.

    Args:
        node (Any): The node to prune.
        parent_op (Optional[str]): The operator of the node's parent.
            ``None`` for the root.
        level (int): The node's level. The root compound is level 1.

    Returns:
        Any: ``node`` with every compound below level 2 pruned. A leaf comes
            back unchanged. The result is ``{}`` (push nothing) if pruning
            leaves nothing to push.

            An ``or`` never absorbs a falsy child: if a survivor of an ``or``
            is falsy, the whole ``or`` becomes ``{}``. The empty ``or`` then
            propagates by the same rules: an ``and`` parent drops it, and an
            ``or`` parent becomes ``{}`` too.

    .. note::
        An emptied ``or`` costs retrieval, not correctness. Its rows are no
        longer narrowed by Notion, so the query fetches more pages, and the
        recheck drops the extra ones.

    .. seealso::

        :func:`_normalize_filter`, which establishes both preconditions, and
        :func:`_depth`, which the call sites use to decide whether to prune.

    .. versionadded:: 0.13.0
    """
    if not isinstance(node, dict):
        # a non dict node is a leaf, nothing to prune, return the node
        return node
    
    for op in ("or", "and"):
        if op in node:
            # extract children from the compound operator
            children = node[op]

            # NOTE: this compares a node's LEVEL, while the call sites compare a
            # measured DEPTH. The same constant serves both only because the root
            # compound is level 1 (a node at level N implies a depth of at least N).
            if level > _NOTION_MAX_FILTER_DEPTH:
                # prune subtrees below the cap
                if parent_op == "or":
                    # replace with its first child
                    return _prune(children[0], parent_op, level - 1)


                else:
                    # it's an "and", drop the whole compound 
                    return {}
                    
            # traverse each child node in children and apply the pruning rule
            survivors = []
            for child in children:
                survivor = _prune(child, op, level + 1)
                survivors.append(survivor)

            if op == "or" and any(not s for s in survivors):
                # An `or` may never absorb a falsy child: 
                # if any survivor of an `or` is falsy, the whole `or` is `{}`
                return {}
            
            return _normalize_filter(op, survivors)            
              

    # op not in ("or", "and"): it's a leaf, return as is
    return node

def _normalize_filter(op: str, clauses: list[dict]) -> dict:
    """Fold ``clauses`` into the smallest filter object meaning ``op`` over them.

    The Notion API caps compound nesting at **2 levels** (the root compound being
    level 1), so a filter measuring 3 is an HTTP 400 (#383). Every level this function
    removes without changing which rows match is a level left in the budget for one
    that carries meaning, which is why the folding here is load-bearing rather than
    cosmetic.

    Three rules, applied in order:

    **Drop** falsy clauses, at every arity. Not defensive coding: the fold's output type
    IS its input element type. :meth:`NotionCompiler.visit_boolean_clause_list` returns
    this result, and that result becomes an element of the parent's clause list one
    level up -- so the moment ``{}`` is a legal output it is automatically a legal
    input. The drop is what keeps the function closed under composition.

    **Splice** a clause that is itself a compound of ``op`` one level in, merging its
    children in place. ONLY when the operators match: ``{"or": [{"and": [P, Q]}, R]}``
    must be left alone, because ``P OR Q OR R`` matches rows that ``(P AND Q) OR R``
    does not. One level of merging is always enough -- every element arrives either as
    a leaf, as the leaf repair's flat two-child compound, or as this function's own
    output, which is already spliced.

    **Unwrap** a lone survivor: an ``and`` of one thing IS that thing, and wrapping it
    spends half the depth budget to say nothing.

    .. note::
        The splice has **no reachable input today**. :class:`BooleanClauseList`'s
        constructor already flattens the same-operator nesting a user *writes*, at AST
        construction time, so no compound reaches this function holding a same-operator
        child. The splice handles the nesting the compiler *manufactures* during
        emission -- which begins with the leaf repair, when ``id != 1`` becomes
        ``{"and": [does_not_equal, is_not_empty]}``. The repair is live (#383), so the
        splice is live too: without it, ``(id != 1 AND name='x') OR grade='A'`` would
        nest one level deeper and draw HTTP 400.

    .. warning::
        The empty result is ``{}`` -- a FALSY sentinel meaning *push nothing*, not an
        error. An empty clause list is neither a compilation failure nor a snapped
        invariant; it is a well-defined outcome, sound under ADR-0022 because the pushed
        filter never decides and ``recheck_where`` holds the full predicate. Raising
        here would turn a working query into a crash.

        Both call sites therefore guard on this return value's TRUTHINESS and decline to
        assign, because ``payload['filter']`` must be ABSENT rather than
        present-and-empty. ``{op: []}`` would defeat that guard -- a non-empty dict is
        truthy -- and an empty compound in the payload either draws an HTTP 400 or is
        read as *no filter at all* (:mod:`normlite.notion_sdk.client` resolves it with
        ``payload.get('filter', False)``), which on a ``DELETE`` means match-everything.

    Idempotent over non-degenerate inputs: folding an already-folded result under the
    same operator returns it unchanged.

    Args:
        op: The compound operator, ``"and"`` or ``"or"``.
        clauses: The already-emitted filter objects to fold. Elements are opaque -- this
            function never inspects a leaf's internals.

    Returns:
        The smallest filter object equivalent to ``op`` over ``clauses``: ``{}`` when
        nothing survives, the lone survivor unwrapped, otherwise ``{op: [...]}``.

    .. seealso::

        Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.

    .. versionadded:: 0.13.0
    """
    # remove empty clauses up-front
    clauses = [clause for clause in clauses if clause]


    # {'and', [{'and': [P, Q]}, R]} must be spliced into: {"and": [P, Q, R]} 
    # {"and": [{"and": [P, Q]}, {"and": [R, S]}]} -> {"and": [P, Q, R, S]}
    spliced: list[dict] = []
    for clause in clauses:
        if isinstance(clause, dict) and op in clause:
            # flatten same-op nested expressions
            spliced.extend(clause[op])
            
        else:
            # leave different op nested or leaf nodes as is
            spliced.append(clause)

    if not spliced:
        # {op: []} collapses into {}
        return {}

    if len(spliced) == 1:
        # {op: [X] collapses into X}
        return spliced[0]

    return {op: spliced}

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
                        if select._is_aggregate:
                            # this site raises unconditionally because it pushes
                            # a strict subset. NOT a depth refusal: whatever this
                            # WHERE measures, only the left-only conjuncts go over
                            # the wire and the rest is handed to a recheck the
                            # aggregate branch never runs (queryplan.py:786).
                            raise CompileError(
                                "An aggregate SELECT cannot compile a WHERE clause that "
                                "spans both sides of a join: only the left-only terms have "
                                "a Notion filter form, so the pushed filter is a SUPERSET "
                                "of the rows this WHERE selects. A plain SELECT re-applies "
                                "the whole WHERE client-side, but the aggregate reduces "
                                "whatever the push returns, so it would count rows this "
                                "WHERE excludes. Run a plain SELECT over the join and "
                                "reduce client-side."
                            )

                        # Compound AND spanning both join sides: split per-clause.
                        # Left-only conjuncts narrow phase-1 to a SUPERSET of the
                        # answer, so push them into payload['filter']; the WHOLE
                        # compound is then held as AST for client-side evaluation
                        # after the merge -- the pushed conjuncts INCLUDED, because a
                        # push never decides (ADR-0022). (See #311, #363.)
                        left_conjuncts = [
                            clause._compiler_dispatch(self)
                            for clause in expression.clauses
                            if _get_expression_parent_tables(clause) == {select._table} and _is_pushable(clause)
                        ]

                        filter_obj = _prune(_normalize_filter("and", left_conjuncts))
                        if filter_obj:
                            payload['filter'] = filter_obj

                        self.planning_context.recheck_where = expression

                    else:
                        if parent_tables == {select._table} and _is_pushable(expression):
                            # the WHERE expression involves columns belonging to the SELECT's table
                            # here the whole predicate is pushed or nothing is:
                            # expression depth can break this exactness
                            emitted_json = expression._compiler_dispatch(self)

                            if select._is_aggregate:
                                # Scope of this gate. Read it before you change the gate.
                                #
                                # The gate refuses one case only: the whole WHERE is pushable,
                                # and its filter nests deeper than Notion allows. A plain
                                # SELECT prunes such a filter to a legal superset, and the
                                # recheck removes the extra rows. The planner's aggregate
                                # branch builds no Filter and drops the recheck (#399). So
                                # nothing narrows the rows after the push. A pruned push would
                                # make the aggregate reduce rows that the WHERE excludes. The
                                # gate raises.
                                #
                                # An unpushable WHERE never reaches this gate:
                                # `_is_pushable(expression)` in the branch above is false, and
                                # the compiler pushes no filter. The aggregate then reduces
                                # every row. That is the same defect, #399, and the fix goes in
                                # the planner. Do NOT hoist this raise above the `_is_pushable`
                                # branch to catch that case. The refusal would block every
                                # aggregate with an unpushable WHERE, which #399 makes correct.
                                #
                                # When #399 is fixed, this gate can prune in place of raising.
                                _raise_if_filter_max_depth_exceeded(
                                    stmt="An aggregate SELECT",
                                    filter_obj=emitted_json,
                                    remedy="Simplify the WHERE, or run a plain SELECT and reduce client-side",
                                )
                                payload["filter"] = emitted_json
                            else:
                                # for the left table, add "filter" to the payload 
                                # only if the expression is pushable (see #383) 
                                # prune the emitted_json to ensure max depth <= 2
                                pruned = _prune(emitted_json)
                                if pruned:
                                    payload['filter'] = pruned

                        # Hold the whole expression as raw AST for client-side evaluation,
                        # UNCONDITIONALLY -- whichever table it reads, and whether or not it
                        # was just pushed above. A pushed conjunct is a HINT that never
                        # decides (ADR-0022), so holding it is what makes the client-side
                        # evaluation the answer; #384 is exactly the case where the push
                        # over-keeps and only the re-application drops the row.
                        #
                        # In the settled vocabulary this channel now carries both kinds: a
                        # LEFT conjunct is a RECHECK (it was pushed on the line above and is
                        # re-applied), while a RIGHT conjunct is a genuine RESIDUAL (nothing
                        # pushes it today). `Planner.plan` derives which table to evaluate it
                        # against from the predicate itself -- do not assume the right side.
                        #
                        # Do NOT dispatch it here: that would register an unconsumed bind
                        # (see #363). Holding an ALREADY-dispatched conjunct is safe and
                        # measured -- _add_bindparam ran once inside the dispatch, and the
                        # literal survives unprocessed.
                        self.planning_context.recheck_where = expression
        
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

        # create the snapshot:
        self.planning_context.pre_widening_fetch_columns = list(self._compiler_state.fetch_columns)

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
            where_expression = delete._whereclause.expression
            _raise_if_filter_term_unpushable(
                stmt="DELETE",
                where_expression=where_expression,
                remedy="Express the condition without NOT, replace \"== None\" / \"!= None\" with is_empty() / is_not_empty() (they also match empty cells), or SELECT the rows first and delete them by object_id",
            )

            with self._compiling(new_state=_CompileState.COMPILING_WHERE):
                # emit the JSON code for the filter object of the query
                # in the COMPILING_WHERE context
                filter_obj = where_expression._compiler_dispatch(self)
                _raise_if_filter_max_depth_exceeded(
                    stmt="DELETE",
                    filter_obj=filter_obj,
                    remedy="SELECT the rows first and delete them by object_id",
                )

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
            where_expression = update._whereclause.expression
            _raise_if_filter_term_unpushable(
                stmt="UPDATE",
                where_expression=where_expression,
                remedy="Express the condition without NOT, replace \"== None\" / \"!= None\" with is_empty() / is_not_empty() (they also match empty cells), or SELECT the rows first and update them by object_id",
            )
            with self._compiling(new_state=_CompileState.COMPILING_WHERE):
                filter_obj = where_expression._compiler_dispatch(self)
                _raise_if_filter_max_depth_exceeded(
                    stmt="UPDATE",
                    filter_obj=filter_obj,
                    remedy="SELECT the rows first and update them by object_id",
                )
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
        """Emit a Notion API conform JSON object for a binary expression.

        Args:
            expression (BinaryExpression): The AST node representing the binary expression.

        Returns:
            dict: The Notion API conform JSON object.

        .. note::
            A ``!=`` leaf is emitted as an ``{"and": [does_not_equal, is_not_empty]}``
            compound, so that the pushed filter means exactly what the SQL means.

            Notion's negative operators are the boolean complement over a domain that
            INCLUDES the valueless cell: ``does_not_equal`` matches "not x OR valueless",
            whereas SQL's ``<>`` against NULL is UNKNOWN and the WHERE policy drops it.
            Conjoining ``is_not_empty`` removes the valueless cell from the match set,
            which makes this push EXACT rather than merely a sound superset. That
            matters most on the DML path, where nothing rechecks the pushed filter
            (ADR-0022) and an over-matching filter is a destroyed row.

            The guard is a **classification, not a capability check**: it reads
            :attr:`~normlite.sql.type_api.TypeEngine.supports_valueless_cells`, which
            each type declares from measurement. A type that cannot hold a valueless
            cell needs no repair, because its ``does_not_equal`` is already exact, and
            repairing it would NARROW a correct filter:

            * ``String`` -- Notion stores every blank title or rich text as ``[]``, a
              PRESENT value that decodes to ``""``. SQL evaluates ``'' <> 'x'`` to TRUE,
              and Notion's bare ``does_not_equal`` matches ``[]``. A conjoined
              ``is_not_empty`` would drop those rows, and on the SELECT path the
              recheck cannot add back a row the push never returned (measured on the
              live API, 2026-10-04).
            * :class:`~normlite.sql.type_api.Boolean` -- a Notion checkbox is a total
              two-valued domain, always concretely ``true``/``false`` and defaulting to
              ``false`` (ADR-0005). Repairing it would emit ``checkbox.is_not_empty``,
              an operator Notion does not define, turning a correct filter into the
              very HTTP 400 this issue is about.

            An EMPTY state is not a VALUELESS one: ``String`` declares ``is_not_empty``
            and still needs no repair. An earlier guard keyed on
            ``Operator.IS_NOT_EMPTY in supported_ops`` confused the two and repaired text.

        .. versionchanged:: 0.13.0
            Added SQL-92 3VL semantics preservation.

        .. seealso::

            Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.
        """
        supported_ops = expression.column.type_.supported_ops
        if (
            expression.operator is Operator.NE and
            expression.column.type_.supports_valueless_cells
        ):
            # emit {"and": {"does_not_equal": ..., "is_not_empty": ...}} to preserve SQL-92 3VL semantics
            # The repair removes valueless cells that does_not_equal would leak.
            # That leak is possible only when the type can hold a valueless cell.
            lhs = {
                "property": expression.column.name,
                **self._compile_type_filter(
                    expression.column,
                    expression.operator,
                    expression.value
                )
            }

            # right hand side is always fixed:
            # {"property": ..., <notion_type>: {"is_not_empty": true}}
            rhs = {
                "property": expression.column.name,
                expression.column.type_.get_col_spec(): {
                    supported_ops[Operator.IS_NOT_EMPTY]: True
                }
            }

            return {"and": [lhs, rhs]}

        return {
            "property": expression.column.name,
            **self._compile_type_filter(
                expression.column,
                expression.operator,
                expression.value
            )
        }
    
    def visit_unary_expression(self, expression: UnaryExpression) -> NoReturn:
        """Refuse to emit a NOT expression: Notion filters have no ``not``.

        Every dispatch site gates on :func:`_is_pushable`, which rejects a NOT.
        A call here is therefore a compiler defect, not a user error.

        Raises:
            UnsupportedCompilationError: Always. No filter object can hold a
                ``not``.

        .. seealso::

            Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.
        
        .. versionchanged:: 0.13.0
            Raises instead of emitting a ``{"not": ...}`` object, which Notion rejects.
        """
        raise UnsupportedCompilationError(
            f"Notion API does not support unary operators in filters: '{expression.operator}'"
        )
    
    def visit_boolean_clause_list(self, expression: BooleanClauseList) -> dict:
        """Emit the JSON filter object for a compound expression, dropping unpushable conjuncts.

        Only reached for a node :func:`_is_pushable` has already admitted -- every
        dispatch site gates on it -- so this method emits, it never decides.

        The per-clause filter below is the **and** rule alone, and it is the second
        half of a rule whose first half lives in :func:`_is_pushable`. The two
        operators are asymmetric:

        * **and** is ``any()``, which is NOT downward-closed: an admitted ``and``
          needs only one pushable clause, so the others are still here and must be
          dropped. Dropping a conjunct WEAKENS the filter, which returns a superset
          of the answer -- sound, because the recheck decides (ADR-0022).
        * **or** is ``all()``, which IS downward-closed: an admitted ``or`` has every
          clause pushable, so the filter matches everything and drops nothing. It is
          vacuous here BY CONSTRUCTION, not by accident -- and that is what makes it
          safe to write one comprehension for both operators. Were it ever to drop a
          disjunct, the filter would NARROW, losing rows no recheck can recover.

        Because "pushable" means ``any()`` for **and** and ``all()`` for **or**, an
        empty clause list cannot be emitted in either case: a node with no surviving
        clause is never admitted in the first place.

        .. seealso::

            Issue `normlite pushes filter constructs the Notion API rejects with HTTP 400 <https://github.com/giant0791/normlite/issues/383>`_.

        .. versionchanged:: 0.13.0
            Unpushable clauses are pruned instead of being emitted.
        """

        emitted_clauses = [
            clause._compiler_dispatch(self)
            for clause in expression.clauses
            if _is_pushable(clause)
        ]

        return _normalize_filter(expression.operator, emitted_clauses)

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