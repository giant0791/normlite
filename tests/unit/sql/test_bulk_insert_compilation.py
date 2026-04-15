import pdb
from types import MappingProxyType

import pytest

from normlite.engine.base import Engine
from normlite.exceptions import ArgumentError
from normlite.sql.dml import insert
from normlite.sql.schema import Table

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
    
def test_process_single_dict_values(students: Table, single_dict_params: dict):
    stmt = insert(students).values(**single_dict_params)

    assert not stmt._has_multi_parameters
    assert stmt._multi_parameters is None
    assert isinstance(stmt._values, MappingProxyType)
    assert set(stmt._values.keys()) == set(single_dict_params.keys())
    assert set([b.value for b in stmt._values.values()]) == set(single_dict_params.values())

def test_process_single_tuple_values(students: Table, single_dict_params: dict):
    stmt = insert(students).values(("Galileo Galilei", 1, False, "1600-01-01", "A"))

    assert not stmt._has_multi_parameters
    assert stmt._multi_parameters is None
    assert isinstance(stmt._values, MappingProxyType)
    assert set(stmt._values.keys()) == set(single_dict_params.keys())
    assert set([b.value for b in stmt._values.values()]) == set(single_dict_params.values())

def test_process_single_list_values(students: Table, single_dict_params: dict):
    stmt = insert(students).values(["Galileo Galilei", 1, False, "1600-01-01", "A"])

    assert not stmt._has_multi_parameters
    assert stmt._multi_parameters is None
    assert isinstance(stmt._values, MappingProxyType)
    assert set(stmt._values.keys()) == set(single_dict_params.keys())
    assert set([b.value for b in stmt._values.values()]) == set(single_dict_params.values())

def test_process_pos_and_kwarg_raises(students: Table):
    with pytest.raises(ArgumentError):
        stmt = insert(students).values(["Galilei"], id=1)

def test_process_more_than_one_pos_arg_raises(students: Table):
    with pytest.raises(ArgumentError):
        stmt = insert(students).values(["Galilei"], (1,))

def test_process_multi_params(students: Table, multi_params: list[dict]):
    stmt = insert(students).values(multi_params)

    assert stmt._has_multi_parameters
    assert stmt._multi_parameters == multi_params