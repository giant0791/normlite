# normlite/engine/interfaces.py
# Copyright (C) 2026 Gianmarco Antonini
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
from typing import TYPE_CHECKING, Any, Literal, Mapping, Optional, Sequence, TypedDict, Union

if TYPE_CHECKING:
    from normlite.sql.base import Compiled

    CompiledCacheType = Mapping[Any, Compiled]

IsolationLevel = Literal[
    "SERIALIZABLE",
    "REPEATABLE READ",
    "READ COMMITTED",
    "READ UNCOMMITTED",
    "AUTOCOMMIT", 
]
""""AUTOCOMMIT" is currently the only supported isolation level."""

ReturningStrategy = Literal["echo", "refetch"]

class ExecutionOptions(TypedDict, total=False):
    """Options for configuring the execution pipeline.
    
    .. versionadded:: 0.8.0
    
    """

    compiled_cache: Optional[CompiledCacheType]
    """Cache to re-use statement compilation object.
    
    .. warning:: 
        
        Not supported yet. This option will be used in a later version.
    """
    
    logging_token: str
    """Token for finer granular logging.
    
    .. warning:: 
        Not supported yet. This option will be used in a later version.
    """
    
    isolation_level: IsolationLevel
    """Isolation level for transactions.
    
    .. Hint::

        Currently, "AUTOCOMMIT" supported only.
    """

    preserve_rowcount: bool
    """When True, the ``cursor.rowcount`` attribute will be unconditionally memoized within the result and made available via the 
    :attr:`normlite.engine.cursor.CursorResult.rowcount` attribute. 
    
    Normally, this attribute is only preserved for UPDATE and DELETE statements. 
    Using this option, the DBAPIs rowcount value can be accessed for other kinds of statements such as INSERT and SELECT.

    .. seealso::
        
        See :attr:`normlite.engine.CursorResult.rowcount` for notes regarding the behavior of this attribute.

    .. warning:: 
        
        Not supported yet. This option will be used in a later version.
    
    """

_CoreSingleExecuteParams = Mapping[str, Any]
_CoreMultiExecuteParams = Sequence[_CoreSingleExecuteParams]
_CoreAnyExecuteParams = Union[
    _CoreSingleExecuteParams,
    _CoreMultiExecuteParams
]

_DBAPISingleExecuteParams = Union[Sequence[Any], _CoreSingleExecuteParams]

def _distill_params(
    parameters: Optional[_CoreAnyExecuteParams] = None,
) -> _CoreMultiExecuteParams:
    """Normalize execution parameters into a sequence of parameter mappings.

    Parameter distillation is a _normalization_  _step_ that ensures that the
    execution pipeline sees exactly **one shape**.
    
    - None -> [{}]
    
    - Mapping -> [mapping]
    
    - Sequence[Mapping] -> sequence (executemany)

    .. versionadded:: 0.8.0

    Args:
        parameters (Optional[_CoreAnyExecuteParams]): The execution parameters to be normalized

    Raises:
        TypeError: if parameters are not of an expected shape.
    
    Returns:
        _CoreMultiExecuteParams: Either a mapping or a sequence of mappings
    """

    if parameters is None:
        return [{}]

    # Mapping = single execution
    if isinstance(parameters, Mapping):
        return [parameters]

    # Sequence = potentially executemany
    if isinstance(parameters, Sequence):
        if not parameters:
            # Empty sequence is just returned as is
            return parameters

        # Validate that all elements are mappings
        for param in parameters:
            if not isinstance(param, Mapping):
                raise TypeError(
                    "Each element of a multi-execute parameter sequence "
                    "must be a mapping"
                )
        return parameters

    raise TypeError(
        "Execution parameters must be a mapping or a sequence of mappings"
    )
