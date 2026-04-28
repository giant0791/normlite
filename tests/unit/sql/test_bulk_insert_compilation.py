import pdb
from types import MappingProxyType

import pytest

from normlite.engine.base import Engine
from normlite.engine.context import ExecutionContext, ExecutionStyle
from normlite.engine.interfaces import _CoreMultiExecuteParams
from normlite.exceptions import ArgumentError, StatementError
from normlite.notion_sdk.getters import get_number_property_value, get_property, get_title_property_value, rich_text_to_plain_text
from normlite.sql.base import Compiled
from normlite.sql.compiler import NotionCompiler
from normlite.sql.dml import insert
from normlite.sql.elements import _BindRole, BindParameter, _NoArg
from normlite.sql.schema import Column, MetaData, Table
from normlite.sql.type_api import Integer, String
from tests.utils.db_helpers import attach_table_oid

@pytest.fixture
def single_dict_params() -> dict:
    return dict(
        name="Galileo Galilei", 
        id= 1, 
        is_active= False, 
        start_on="1600-01-01", 
        grade="A",
    )    

@pytest.fixture
def single_tuple_params() -> tuple:
    return ("Galileo Galilei", 1, False, "1600-01-01", "A")

@pytest.fixture
def single_list_params() -> list:
    return ["Galileo Galilei", 1, False, "1600-01-01", "A"]

@pytest.fixture
def multi_params() -> list[dict]:
    mp = []
    for i in range(5):
        mp.append(
            dict(
                name=f"name_{i}", 
                id=i, 
                is_active= True, 
                start_on=f"160{i}-01-01", 
                grade="A",
            )               
        )
    return mp

@pytest.fixture
def table():
    new_table = Table(
        "test_table",
        MetaData(),
        Column("name", String(is_title=True)),
        Column("id", Integer())
    )
    attach_table_oid(new_table, "XXXXX")
    return new_table

def compile_stmt(stmt) -> Compiled:
    nc = NotionCompiler()
    return stmt.compile(nc)

# -------------------------------------------------
# Process single values tests
# -------------------------------------------------
    
def test_process_single_dict_values(students: Table, single_dict_params: dict):
    stmt = insert(students).values(**single_dict_params)

    assert not stmt._has_multi_parameters
    assert stmt._multi_parameters is None
    assert isinstance(stmt._values, MappingProxyType)
    assert set(stmt._values.keys()) == set(single_dict_params.keys())
    assert all([b.value is _NoArg.NO_ARG for b in stmt._values.values()])
    assert set(stmt._single_parameters.values()) == set(single_dict_params.values())

def test_process_single_tuple_values(students: Table, single_tuple_params: dict):
    stmt = insert(students).values(single_tuple_params)

    assert not stmt._has_multi_parameters
    assert stmt._multi_parameters is None
    assert isinstance(stmt._values, MappingProxyType)
    assert set(stmt._values.keys()) == set([c.name for c in students.uc])
    assert all([b.value is _NoArg.NO_ARG for b in stmt._values.values()])
    assert set(stmt._single_parameters.values()) == set(single_tuple_params)

def test_process_single_list_values(students: Table, single_list_params: dict):
    stmt = insert(students).values(single_list_params)

    assert not stmt._has_multi_parameters
    assert stmt._multi_parameters is None
    assert isinstance(stmt._values, MappingProxyType)
    assert set(stmt._values.keys()) == set([c.name for c in students.uc])
    assert all([b.value is _NoArg.NO_ARG for b in stmt._values.values()])
    assert set(stmt._single_parameters.values()) == set(single_list_params)

def test_process_pos_and_kwarg_raises(students: Table):
    with pytest.raises(ArgumentError):
        stmt = insert(students).values(["Galilei"], id=1)

def test_process_more_than_one_pos_arg_raises(students: Table):
    with pytest.raises(ArgumentError):
        stmt = insert(students).values(["Galilei"], (1,))

# -------------------------------------------------
# Process multi values tests
# -------------------------------------------------

def test_process_multi_params(students: Table, multi_params: list[dict]):
    stmt = insert(students).values(multi_params)

    assert stmt._has_multi_parameters
    assert stmt._multi_parameters == multi_params

def test_process_empty_seq_as_multi_raises(students: Table):
    with pytest.raises(ArgumentError):
        _ = insert(students).values([])

def test_mixing_multi_and_single_raises(
    students: Table, 
    single_dict_params: dict,
    single_tuple_params: tuple,
    single_list_params: list,
    multi_params: list[dict]
):
    with pytest.raises(ArgumentError):
        _ = (
            insert(students)
            .values(multi_params)
            .values(**single_dict_params)
        )

    with pytest.raises(ArgumentError):
        _ = (
            insert(students)
            .values(multi_params)
            .values(single_tuple_params)
        )
    with pytest.raises(ArgumentError):
        _ = (
            insert(students)
            .values(multi_params)
            .values(single_list_params)
        )

def test_mixing_single_and_multi_raises(
    students: Table, 
    single_dict_params: dict,
    single_tuple_params: tuple,
    single_list_params: list,
    multi_params: list[dict]
):
    with pytest.raises(ArgumentError):
        _ = (
            insert(students)
            .values(**single_dict_params)
            .values(multi_params)
        )

    with pytest.raises(ArgumentError):
        _ = (
            insert(students)
            .values(single_tuple_params)
            .values(multi_params)
        )
    with pytest.raises(ArgumentError):
        _ = (
            insert(students)
            .values(single_list_params)
            .values(multi_params)
        )

def test_missing_values_in_bulk_insert_raises(students: Table):
    with pytest.raises(ArgumentError) as exc:
        stmt = insert(students).values([{"name": "Alice"}, {"id": 2}])

    assert "row 0" in str(exc.value)

# -------------------------------------------------
# Construct parameters tests
# -------------------------------------------------

def test_insert_sets_single_values_correctly(table: Table):
    """Test the key transformation of the late binding refactor:
    
    from:
        _values = {col → BindParameter(value=...)}

    to:
        _values = {col → BindParameter(NO_ARG)}
        _single_parameters = {col → value}

    _values holds the template, _single_parameters holds the values
    """
    stmt = insert(table).values(name="Alice", id=1)

    assert not stmt._has_multi_parameters
    assert stmt._single_parameters == dict(name="Alice", id=1)
    expected_template = MappingProxyType({
        "name": BindParameter("name", type_=None),      # remember: type_ is set at compilation time
        "id": BindParameter("id", type_=None)
    })

    # REMEMBER: BindParam does not have __eq__, so the mapping equality fails because bindparams have different identities
    assert set(stmt._values.keys()) == set(expected_template.keys())
    assert [bp.key for bp in stmt._values.values()] == [bp.key for bp in expected_template.values()]
    assert [bp.value for bp in stmt._values.values()] == [bp.value for bp in expected_template.values()]
    assert [bp.role for bp in stmt._values.values()] == [bp.role for bp in expected_template.values()]

def test_construct_params_returns_dict_for_single_values(table: table):
    stmt = insert(table).values(name="Alice", id=1)
    compiled = compile_stmt(stmt)

    assert compiled.params == dict(name="Alice", id=1)      # only user-facing parameters

def test_construct_params_single_values(table: Table):
    stmt = insert(table).values(name="Galileo", id=1)
    compiled = compile_stmt(stmt)

    params = compiled._compiler.construct_params()

    assert params["name"] == "Galileo"
    assert params["id"] == 1

def test_construct_params_with_execution_override(table):
    stmt = insert(table).values(name="Galileo", id=-1)
    compiled = compile_stmt(stmt)

    params = compiled._compiler.construct_params({"id": 42})

    assert params["name"] == "Galileo"
    assert params["id"] == 42

def test_construct_params_override_precedence(table):
    stmt = insert(table).values(name="Default", id=1)
    compiled = compile_stmt(stmt)

    params = compiled._compiler.construct_params({"name": "Override"})

    assert params["name"] == "Override"
    assert params["id"] == 1

def test_construct_params_includes_dbapi_param(table):
    stmt = insert(table).values(name="Galileo", id=1)
    compiled = compile_stmt(stmt)

    params = compiled._compiler.construct_params()

    # ensure DBAPI_PARAM exists
    dbapi_keys = [
        k for k, bp in compiled._compiler_state.execution_binds.items()
        if bp.role == _BindRole.DBAPI_PARAM
    ]

    for key in dbapi_keys:
        assert key in params
        assert params[key] is not None

def test_construct_params_unknown_parameter_raises(table):
    stmt = insert(table).values(name="Galileo", id=100)
    compiled = compile_stmt(stmt)

    with pytest.raises(StatementError) as exc:
        compiled._compiler.construct_params({"unknown": 123})

    assert "Unknown parameter" in str(exc.value)

def test_construct_params_missing_dbapi_param_raises(table):
    stmt = insert(table).values(name="Galileo", id=1)
    compiled = compile_stmt(stmt)

    # break DBAPI_PARAM
    for bp in compiled._compiler_state.execution_binds.values():
        if bp.role == _BindRole.DBAPI_PARAM:
            bp.value = None

    with pytest.raises(StatementError) as exc:
        compiled._compiler.construct_params()

    assert "Internal bind parameter" in str(exc.value)

# --------------------------------------------------------
# Compilation tests
# --------------------------------------------------------


def test_compile_multi_values_sets_template_and_multi_params(table):
    stmt = insert(table).values(
        [
            {"name": "Alice", "id":1},
            {"name": "Bob", "id":2},
        ]
    )
    nc = NotionCompiler()
    compiled = stmt.compile(nc)

# --------------------------------------------------------
# Context tests
# --------------------------------------------------------

class MockExecContext(ExecutionContext):
    def __init__(self, 
        engine: Engine,
        compiled: Compiled, 
        distilled_params: _CoreMultiExecuteParams
    ):
        connection = engine.connect()
        cursor = engine._dbapi_connection.cursor()
        super().__init__(
            engine,
            connection, 
            cursor,
            compiled, 
            distilled_params
        )

def test_exec_params_on_bulk_insert_raises(engine: Engine, table: table):
    stmt = insert(table).values([
        {"name": "Alice", "id": 1},
        {"name": "Bob", "id": 2}
    ])

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    distilled_params = [{"name": "Bob", "id": 3}]
    ctx = MockExecContext(engine, compiled, distilled_params)

    with pytest.raises(StatementError):
        ctx.pre_exec()

def test_exec_params_on_single_insert_overrides(engine: Engine, table: Table):
    stmt = insert(table).values({"name": "Alice", "id": 2})

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    distilled_params = [{"name": "Bob", "id": 3}]
    ctx = MockExecContext(engine, compiled, distilled_params)
    ctx.pre_exec()
    dbapi_params = ctx.parameters
    payload = dbapi_params["payload"]

    assert len(dbapi_params) == 1
    assert "Bob" ==  rich_text_to_plain_text(get_property(payload, "name").get("title"))
    assert 3 == get_property(payload, "id").get("number")

def test_context_can_prep_exec(engine: Engine, table: Table):
    stmt = insert(table).values([
        {"name": "Alice", "id": 1},
        {"name": "Bob", "id": 2}
    ])

    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    ctx = MockExecContext(engine, compiled, [])
    ctx.pre_exec()
    params = ctx.parameters
    payload_0 = params[0]["payload"]
    payload_1 = params[1]["payload"]

    assert ctx.execution_style == ExecutionStyle.INSERTMANYVALUES
    assert len(params) == 2
    assert "Alice" == rich_text_to_plain_text(get_property(payload_0, "name").get("title"))
    assert "Bob" ==  rich_text_to_plain_text(get_property(payload_1, "name").get("title"))
    assert 1 == get_property(payload_0, "id").get("number")
    assert 2 == get_property(payload_1, "id").get("number")

def test_missing_values_in_bulk_insert_params_raises(engine: Engine, table: Table):
    stmt = insert(table)
    distilled_params = [
        {"name": "Alice", "id": 1},
        {"id": 2}
    ]
    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    ctx = MockExecContext(engine, compiled, distilled_params)
    
    with pytest.raises(StatementError) as exc:
        ctx.pre_exec()

    assert "name" in str(exc.value)
    assert "parameter set 1"

def test_no_values_and_one_exec_param(engine: Engine, table: Table):
    stmt = insert(table)
    distilled_params = [
        {"name": "Alice", "id": 1},
    ]
    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    ctx = MockExecContext(engine, compiled, distilled_params)
    ctx.pre_exec()
    dbapi_params = ctx.parameters
    payload = dbapi_params["payload"]

    assert not ctx.invoked_stmt._single_parameters
    assert ctx.execution_style == ExecutionStyle.EXECUTE
    assert len(dbapi_params) == 1
    assert "Alice" ==  rich_text_to_plain_text(get_property(payload, "name").get("title"))
    assert 1 == get_property(payload, "id").get("number")

def test_no_values_and_multi_exec_params(engine: Engine, table: Table):
    stmt = insert(table)
    distilled_params = [
        {"name": "Alice", "id": 1},
        {"name": "Bob", "id": 2}
    ]
    nc = NotionCompiler()
    compiled = stmt.compile(nc)
    ctx = MockExecContext(engine, compiled, distilled_params)
    ctx.pre_exec()
    dbapi_params = ctx.parameters

    assert not ctx.invoked_stmt._single_parameters
    assert not ctx.invoked_stmt._has_multi_parameters
    assert ctx.execution_style == ExecutionStyle.INSERTMANYVALUES
    assert len(dbapi_params) == 2





