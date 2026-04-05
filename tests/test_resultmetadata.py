import pytest
import itertools

from normlite._constants import SpecialColumns
from normlite.engine.resultmetadata import CursorResultMetaData
from normlite.notiondbapi.dbapi2_consts import DBAPITypeCode


# -------------------------------------------------------------
# Important Pytest Constraint: 
# Fixtures like row_description cannot be used inside 
# parametrize directly.
# -------------------------------------------------------------
ROW_DESCRIPTION = (
    (SpecialColumns.NO_ID, DBAPITypeCode.ID, None, None, None, None, None,),
    (SpecialColumns.NO_ARCHIVED, DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
    (SpecialColumns.NO_IN_TRASH, DBAPITypeCode.ARCHIVAL_FLAG, None, None, None, None, None,),
    (SpecialColumns.NO_CREATED_TIME, DBAPITypeCode.TIMESTAMP, None, None, None, None, None,),
    ("name", DBAPITypeCode.TITLE, None, None, None, None, None,),
    ("id", DBAPITypeCode.NUMBER, None, None, None, None, None,),
    ("is_active", DBAPITypeCode.CHECKBOX, None, None, None, None, None,),
    ("start_on", DBAPITypeCode.DATE, None, None, None, None, None,),
    ("grade", DBAPITypeCode.RICH_TEXT, None, None, None, None, None,),
)

SYSTEM_DESC = ROW_DESCRIPTION[:4]
USER_DESC = ROW_DESCRIPTION[4:]

# ------------------------------------------------------------
# Helper
# ------------------------------------------------------------

def all_desc_subsets(desc):
    return list(
        itertools.chain.from_iterable(
            itertools.combinations(desc, r)
            for r in range(len(desc) + 1)
        )
    )

# ------------------------------------------------------------
# Baseline behavior
# ------------------------------------------------------------

def test_no_projection_returns_all_columns():
    meta = CursorResultMetaData(ROW_DESCRIPTION, is_ddl=False)

    expected_keys = [col[0] for col in ROW_DESCRIPTION]

    assert meta.keys == expected_keys
    assert set(meta.key_to_index.keys()) == set(expected_keys)
    assert set(meta.index_for_key.values()) == set(expected_keys)

# ------------------------------------------------------------
# System Column Invariants
# ------------------------------------------------------------

@pytest.mark.parametrize(
    "subset",
    list(
        itertools.chain.from_iterable(
            itertools.combinations(SYSTEM_DESC, r)
            for r in range(len(SYSTEM_DESC) + 1)
        )
    ),
)
def test_system_columns_preserve_identity_and_index(subset):
    meta = CursorResultMetaData(subset, is_ddl=False)

    expected_keys = [col[0] for col in subset]

    # Keys must match exactly (no transformation)
    assert meta.keys == expected_keys

    # Index must match tuple position
    for idx, key in enumerate(expected_keys):
        assert meta.key_to_index[key] == idx
        assert meta.index_for_key[idx] == key

# ------------------------------------------------------------
# User Column Invariants
# ------------------------------------------------------------

@pytest.mark.parametrize(
    "subset",
    list(
        itertools.chain.from_iterable(
            itertools.combinations(USER_DESC, r)
            for r in range(len(USER_DESC) + 1)
        )
    ),
)
def test_user_columns_preserve_order_and_mapping(subset):
    meta = CursorResultMetaData(subset, is_ddl=False)

    expected_keys = [col[0] for col in subset]

    assert meta.keys == expected_keys

    for idx, key in enumerate(expected_keys):
        assert meta.key_to_index[key] == idx
        assert meta.index_for_key[idx] == key

# ------------------------------------------------------------
# Mixed Column Invariants
# ------------------------------------------------------------

@pytest.mark.parametrize(
    "sys_subset,user_subset",
    [
        (SYSTEM_DESC[:2], USER_DESC[:2]),
        (SYSTEM_DESC[2:], USER_DESC[2:]),
        (SYSTEM_DESC[::2], USER_DESC[::2]),
        (SYSTEM_DESC, USER_DESC),
        ((), USER_DESC),
        (SYSTEM_DESC, ()),
    ],
)
def test_mixed_columns_consistency(sys_subset, user_subset):
    combined = tuple(sys_subset) + tuple(user_subset)

    meta = CursorResultMetaData(combined, is_ddl=False)

    expected_keys = [col[0] for col in combined]

    # Order must be exactly preserved
    assert meta.keys == expected_keys

    # Bidirectional mapping must hold
    for idx, key in enumerate(expected_keys):
        assert meta.key_to_index[key] == idx
        assert meta.index_for_key[idx] == key

# ------------------------------------------------------------
# Strong Invariant: Roundtrip Mapping
# ------------------------------------------------------------

@pytest.mark.parametrize(
    "subset",
    [
        ROW_DESCRIPTION,
        ROW_DESCRIPTION[:3],
        ROW_DESCRIPTION[3:],
        ROW_DESCRIPTION[::2],
    ],
)
def test_roundtrip_key_index_mapping(subset):
    meta = CursorResultMetaData(subset, is_ddl=False)

    for key, idx in meta.key_to_index.items():
        assert meta.index_for_key[idx] == key

# ------------------------------------------------------------
# Empty Description Edge Case
# ------------------------------------------------------------

def test_empty_description():
    meta = CursorResultMetaData((), is_ddl=False)

    assert meta.keys == []
    assert meta.key_to_index == {}
    assert meta.index_for_key == {}

# ------------------------------------------------------------
# DDL flag passthrough
# ------------------------------------------------------------

def test_is_ddl_flag():
    meta = CursorResultMetaData(ROW_DESCRIPTION, is_ddl=True)

    assert meta.is_ddl is True