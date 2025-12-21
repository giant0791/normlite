import pdb
from typing import Any, Mapping, Sequence, Union
from normlite._constants import SpecialColumns
from normlite.engine.resultmetadata import CursorResultMetaData
from normlite.sql.type_api import type_mapper

class Row:
    """Provide pythonic high level interface to a single SQL database row.

    This class automatically process JSON objects into Python values for a real pythonic high level interface.

    .. versionchanged:: 0.8.0
        This version fully supports rows with a subset of DBAPI columns.

    .. versionchanged:: 0.7.0
        :meth:`__getattr__()` has been added to provide access to row values with Python dot notation.
    
    .. versionchanged:: 0.5.0
        :class:`Row` has been significantly extended to provide iteratable capabilities
        and a mapping-sytle object to access the values of the columns returned in the row.
        
    """
    def __init__(self, metadata: CursorResultMetaData, row_data: tuple):
        self._metadata = metadata
        """The metadata object to process raw rows."""

        self._values = [None] * len(row_data)
        """The column values."""

        if self._metadata.is_ddl:
            self._process_ddl_row(row_data)
        else:
            self._process_dml_row(row_data)

    
    def _process_dml_row(self, row_data: tuple) -> None:
        for col_name, rec in self._metadata._colmap.items():
            _, col_index, col_type = rec

            type_factory = type_mapper[col_type]
            result_proc = type_factory.result_processor()
            value = row_data[col_index]
            self._values[col_index] = result_proc(value)
    
    def _process_ddl_row(self, row_data: tuple) -> None:
        col_name, col_type, col_id, col_value = row_data
        is_special_col = col_name in SpecialColumns.values()
        type_factory = type_mapper[col_type]
        result_proc = type_factory.result_processor()
        self._values[0] = col_name
        self._values[1] = type_factory
        self._values[2] = col_id
        self._values[3] = result_proc(col_value) if is_special_col else None

    def keys(self) -> Sequence[str]:
        """Column names that can be accessed by this row."""
        return self._metadata.keys

    def __getattr__(self, key: str) -> Any:
        """Provide access with dot notation to row values."""
        try:
            col_idx = self._metadata.key_to_index[key]
            return self._values[col_idx]
        except KeyError as err:
            raise AttributeError(key) from err

    def __getitem__(self, key_or_index: Union[str, int]) -> Any:
        if isinstance(key_or_index, int):
            try:
                return self._values[key_or_index]
            except IndexError:
                raise IndexError(f"{type(self).__name__} index out of range: {key_or_index}")
            
        elif isinstance(key_or_index, str):
            try:
                col_name = self._metadata.key_to_index[key_or_index]
                return self._values[col_name]
            except KeyError:
                raise KeyError(f"{type(self).__name__} has no column named {key_or_index!r}")
            
        else:
            raise TypeError(
                f"{type(self).__name__} indices must be str (column name) or int (column index), "
                f"not {type(key_or_index).__name__}"
            )

    def as_tuple(self) -> tuple:
        return tuple(self._values)

    def mapping(self) -> dict:
        """Provide the mapping object for this row.
        
        .. versionadded:: 0.5.0
        
        """
        return RowMapping(self)

    def __repr__(self):
        return f"Row({{ {', '.join(f'{k!r}: {self[k]!r}' for k in self._metadata.key_to_index)} }})"

    def __str__(self):
        values =  ", ".join([f'{self[k]!r}' for k in self._metadata.key_to_index])
        return f'({values})'

class RowMapping(Mapping[str, Any]):
    """Helper to construct mapping objects for rows.
    
    :class:`RowMapping` provides a dedicated mapping implementation for column name, column value pairs.
        
    .. versionadded:: 0.5.0
    
    """
    def __init__(self, row: Row):
        self._row = row
        """The underlying row object."""

        self._mapping = {
            key: value 
            for key, value in zip(self._row._metadata.keys, self._row)
        }
        """The mapping object created from the row object."""

    def __getitem__(self, key):
        return self._row[key]

    def __iter__(self):
        return iter(self._row._metadata.keys)

    def __len__(self):
        return len(self._row._metadata.keys)
    
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Mapping):
            return dict(self.items()) == dict(other.items())
        return NotImplemented

    def keys(self):
        return self._mapping.keys()

    def values(self):
        return self._mapping.values()

    def items(self):
        return self._mapping.items()