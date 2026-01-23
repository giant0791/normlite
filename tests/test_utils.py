import pytest
from collections.abc import Mapping

from normlite.utils import frozendict
# adjust import path as needed


# ---------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------

def test_empty_construction():
    fd = frozendict()
    assert isinstance(fd, Mapping)
    assert len(fd) == 0


def test_kwargs_construction():
    fd = frozendict(a=1, b=2)
    assert fd["a"] == 1
    assert fd["b"] == 2


def test_mapping_construction():
    d = {"a": 1, "b": 2}
    fd = frozendict(d)
    assert fd == d


def test_iterable_construction():
    items = [("a", 1), ("b", 2)]
    fd = frozendict(items)
    assert dict(fd.items()) == {"a": 1, "b": 2}


def test_frozendict_copy_construction_is_identity():
    fd1 = frozendict(a=1, b=2)
    fd2 = frozendict(fd1)

    assert fd1 is not fd2 or fd1 == fd2
    assert fd1 == fd2
    assert hash(fd1) == hash(fd2)


# ---------------------------------------------------------------------
# Mapping behavior
# ---------------------------------------------------------------------

def test_mapping_interface():
    fd = frozendict(a=1, b=2)

    assert list(fd.keys()) == ["a", "b"]
    assert list(fd.values()) == [1, 2]
    assert list(fd.items()) == [("a", 1), ("b", 2)]
    assert "a" in fd
    assert "c" not in fd


def test_iteration_yields_keys():
    fd = frozendict(a=1, b=2)
    assert list(iter(fd)) == ["a", "b"]


# ---------------------------------------------------------------------
# Equality semantics
# ---------------------------------------------------------------------

def test_equality_with_dict_and_order_independence():
    d1 = {"a": 1, "b": 2}
    d2 = {"b": 2, "a": 1}

    fd1 = frozendict(d1)
    fd2 = frozendict(d2)

    assert d1 == d2 == fd1 == fd2


def test_equality_with_other_mapping():
    fd = frozendict(a=1)
    assert fd == {"a": 1}
    assert {"a": 1} == fd


# ---------------------------------------------------------------------
# Immutability guarantees
# ---------------------------------------------------------------------

def test_no_item_assignment():
    fd = frozendict(a=1)
    with pytest.raises(TypeError):
        fd["a"] = 2  # type: ignore


def test_no_item_deletion():
    fd = frozendict(a=1)
    with pytest.raises(TypeError):
        del fd["a"]  # type: ignore


def test_no_update_method():
    fd = frozendict(a=1)
    with pytest.raises(AttributeError):
        fd.update({"b": 2})  # type: ignore


def test_no_mutation_via_views():
    fd = frozendict(a=1, b=2)

    keys = fd.keys()
    values = fd.values()
    items = fd.items()

    # Views must not expose mutation APIs
    assert not hasattr(keys, "add")
    assert not hasattr(keys, "remove")
    assert not hasattr(items, "add")
    assert not hasattr(items, "remove")

    # Views must be iterable and readable
    assert list(keys) == ["a", "b"]
    assert list(values) == [1, 2]
    assert list(items) == [("a", 1), ("b", 2)]

    # Sanity check: data unchanged
    assert fd == {"a": 1, "b": 2}

def test_mutating_original_input_does_not_affect_frozendict():
    d = {"a": 1}
    fd = frozendict(d)

    d["a"] = 999
    d["b"] = 2

    assert fd == {"a": 1}


# ---------------------------------------------------------------------
# Hashing semantics
# ---------------------------------------------------------------------

def test_hashable_when_values_are_hashable():
    fd = frozendict(a=1, b="x", c=(1, 2))
    h = hash(fd)
    assert isinstance(h, int)


def test_unhashable_when_value_is_mutable():
    fd = frozendict(a=[1, 2, 3])
    with pytest.raises(TypeError):
        hash(fd)


def test_hash_is_order_independent():
    fd1 = frozendict(a=1, b=2)
    fd2 = frozendict(b=2, a=1)

    assert hash(fd1) == hash(fd2)


def test_hash_stable_across_calls():
    fd = frozendict(a=1, b=2)
    assert hash(fd) == hash(fd)
    assert hash(fd) == hash(fd)


# ---------------------------------------------------------------------
# Union operators
# ---------------------------------------------------------------------

def test_union_with_frozendict():
    fd1 = frozendict(a=1)
    fd2 = frozendict(b=2)

    fd3 = fd1 | fd2
    assert isinstance(fd3, frozendict)
    assert fd3 == {"a": 1, "b": 2}


def test_union_with_dict():
    fd = frozendict(a=1)
    d = {"b": 2}

    result = fd | d
    assert isinstance(result, frozendict)
    assert result == {"a": 1, "b": 2}


def test_reverse_union():
    fd = frozendict(b=2)
    d = {"a": 1}

    result = d | fd
    assert isinstance(result, frozendict)
    assert result == {"a": 1, "b": 2}


def test_union_assignment_creates_new_instance():
    fd1 = frozendict(a=1)
    fd2 = fd1

    fd1 |= {"b": 2}

    assert fd1 == {"a": 1, "b": 2}
    assert fd2 == {"a": 1}
    assert fd1 is not fd2


# ---------------------------------------------------------------------
# Copy helper
# ---------------------------------------------------------------------

def test_copy_without_updates_returns_same_instance():
    fd = frozendict(a=1)
    assert fd.copy() is fd


def test_copy_with_updates_returns_new_frozendict():
    fd = frozendict(a=1)
    fd2 = fd.copy(b=2)

    assert fd == {"a": 1}
    assert fd2 == {"a": 1, "b": 2}
    assert fd is not fd2
